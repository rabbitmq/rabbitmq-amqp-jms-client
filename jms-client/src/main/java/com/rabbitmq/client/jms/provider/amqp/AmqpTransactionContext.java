/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.rabbitmq.client.jms.provider.amqp;

import java.util.HashMap;
import java.util.Map;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import com.rabbitmq.client.jms.meta.JmsConsumerId;
import com.rabbitmq.client.jms.meta.JmsProducerId;
import com.rabbitmq.client.jms.meta.JmsSessionInfo;
import com.rabbitmq.client.jms.meta.JmsTransactionId;
import com.rabbitmq.client.jms.meta.JmsTransactionInfo;
import com.rabbitmq.client.jms.meta.JmsXaRequest;
import com.rabbitmq.client.jms.provider.exceptions.ProviderXaException;
import com.rabbitmq.client.jms.provider.AsyncResult;
import com.rabbitmq.client.jms.provider.ProviderException;
import com.rabbitmq.client.jms.provider.amqp.builders.AmqpTransactionCoordinatorBuilder;
import com.rabbitmq.client.jms.provider.exceptions.ProviderIllegalStateException;
import com.rabbitmq.client.jms.provider.exceptions.ProviderTransactionRolledBackException;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.transaction.TransactionalState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the operations surrounding AMQP transaction control.
 *
 * The Transaction will carry a JmsTransactionId while the Transaction is open, once a
 * transaction has been committed or rolled back the Transaction Id is cleared.
 */
public class AmqpTransactionContext implements AmqpResourceParent {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpTransactionContext.class);

    private final AmqpSession session;
    private final JmsSessionInfo resourceInfo;
    private final Map<JmsConsumerId, AmqpConsumer> txConsumers = new HashMap<>();
    private final Map<JmsProducerId, AmqpProducer> txProducers = new HashMap<>();

    private JmsTransactionId current;
    private TransactionalState cachedAcceptedState;
    private TransactionalState cachedTransactedState;
    private AmqpTransactionCoordinator coordinator;

    /**
     * Creates a new AmqpTransactionContext instance.
     *
     * @param session
     *        The session that owns this transaction context.
     * @param resourceInfo
     *        The resourceInfo that defines this transaction context.
     */
    public AmqpTransactionContext(AmqpSession session, JmsSessionInfo resourceInfo) {
        this.session = session;
        this.resourceInfo = resourceInfo;
    }

    public void begin(final JmsTransactionId txId, final AsyncResult request) throws ProviderException {
        if (current != null) {
            throw new ProviderIllegalStateException("Begin called while a TX is still Active.");
        }

        final AsyncResult declareCompletion = new AsyncResult() {

            @Override
            public void onSuccess() {
                current = txId;
                cachedAcceptedState = new TransactionalState();
                cachedAcceptedState.setOutcome(Accepted.getInstance());
                cachedAcceptedState.setTxnId(getAmqpTransactionId());
                cachedTransactedState = new TransactionalState();
                cachedTransactedState.setTxnId(getAmqpTransactionId());
                request.onSuccess();
            }

            @Override
            public void onFailure(ProviderException result) {
                current = null;
                cachedAcceptedState = null;
                cachedTransactedState = null;
                request.onFailure(result);
            }

            @Override
            public boolean isComplete() {
                return current != null;
            }
        };

        if (coordinator == null || coordinator.isClosed()) {
            AmqpTransactionCoordinatorBuilder builder =
                new AmqpTransactionCoordinatorBuilder(this, session.getResourceInfo());
            builder.buildResource(new AsyncResult() {

                @Override
                public void onSuccess() {
                    try {
                        coordinator.declare(txId, declareCompletion);
                    } catch (ProviderException e) {
                        request.onFailure(e);
                    }
                }

                @Override
                public void onFailure(ProviderException result) {
                    request.onFailure(result);
                }

                @Override
                public boolean isComplete() {
                    return request.isComplete();
                }
            });
        } else {
            coordinator.declare(txId, declareCompletion);
        }
    }

    public void commit(final JmsTransactionInfo transactionInfo, JmsTransactionInfo nextTransactionInfo, final AsyncResult request) throws ProviderException {
        if (!transactionInfo.getId().equals(current)) {
            if (!transactionInfo.isInDoubt() && current == null) {
                throw new ProviderIllegalStateException("Commit called with no active Transaction.");
            } else if (!transactionInfo.isInDoubt() && current != null) {
                throw new ProviderIllegalStateException("Attempt to Commit a transaction other than the current one");
            } else {
                throw new ProviderTransactionRolledBackException("Transaction in doubt and cannot be committed.");
            }
        }

        preCommit();

        LOG.trace("TX Context[{}] committing current TX[[]]", this, current);

        DischargeCompletion completion = new DischargeCompletion(request, nextTransactionInfo, true);

        coordinator.discharge(current, completion);
        current = null;

        if (completion.isPipelined()) {
            // If the discharge completed abnormally then we don't bother creating a new TX as the
            // caller will determine how to recover.
            if (!completion.isComplete()) {
                begin(nextTransactionInfo.getId(), completion.getDeclareCompletion());
            } else {
                completion.getDeclareCompletion().onFailure(completion.getFailureCause());
            }
        }
    }

    public void rollback(JmsTransactionInfo transactionInfo, JmsTransactionInfo nextTransactionInfo, final AsyncResult request) throws ProviderException {
        if (!transactionInfo.getId().equals(current)) {
            if (!transactionInfo.isInDoubt() && current == null) {
                throw new ProviderIllegalStateException("Rollback called with no active Transaction.");
            } else if (!transactionInfo.isInDoubt() && current != null) {
                throw new ProviderIllegalStateException("Attempt to rollback a transaction other than the current one");
            } else {
                request.onSuccess();
                return;
            }
        }

        preRollback();

        LOG.trace("TX Context[{}] rolling back current TX[[]]", this, current);

        DischargeCompletion completion = new DischargeCompletion(request, nextTransactionInfo, false);

        coordinator.discharge(current, completion);
        current = null;

        if (completion.isPipelined()) {
            // If the discharge completed abnormally then we don't bother creating a new TX as the
            // caller will determine how to recover.
            if (!completion.isComplete()) {
                begin(nextTransactionInfo.getId(), completion.getDeclareCompletion());
            } else {
                completion.getDeclareCompletion().onFailure(completion.getFailureCause());
            }
        }
    }

    //----- XA ---------------------------------------------------------------//

    private final Map<String, XaBranch> branches = new HashMap<>();
    private boolean inUnknownBranch;

    /**
     * A branch that was started on this session, and has not been committed or rolled back.
     */
    private static final class XaBranch {

        private final JmsTransactionId txId;
        private boolean prepared;

        private XaBranch(JmsTransactionId txId) {
            this.txId = txId;
        }

        private Binary amqpTransactionId() {
            return (Binary) txId.getProviderHint();
        }
    }

    private interface XaAction {
        void run() throws ProviderException;
    }

    /**
     * Runs an operation on an XA branch. The session is not in a transaction unless a branch
     * has been started and not ended, which means that the work of the session is not
     * transactional in between.
     *
     * @param xaRequest
     *        describes the operation.
     * @param result
     *        the request to signal when the operation completes.
     *
     * @throws ProviderException if the operation cannot be started.
     */
    public void xa(final JmsXaRequest xaRequest, final AsyncResult result) throws ProviderException {
        final Xid xid = xaRequest.getXid();
        final String key = xid == null ? null : branchKey(xid);
        final XaBranch branch = key == null ? null : branches.get(key);

        switch (xaRequest.getType()) {
            case START:
                xaStart(xaRequest, key, result);
                break;
            case RESUME:
                // After a reconnect the branch is unknown, so the work of the session is dropped until it ends.
                setCurrent(branch == null ? null : branch.txId);
                inUnknownBranch = branch == null;
                result.onSuccess();
                break;
            case END:
                setCurrent(null);
                result.onSuccess();
                break;
            case PREPARE:
                if (branch == null) {
                    result.onFailure(new ProviderXaException(XAException.XAER_NOTA, "Unknown branch " + xaRequest));
                    break;
                }
                setCurrent(null);
                preCommit();
                withCoordinator(result, () -> coordinator.xaPrepare(branch.amqpTransactionId(), xaRequest,
                    new XaCompletion(result) {
                        @Override
                        public void onSuccess() {
                            branch.prepared = true;
                            // The deliveries are settled now, because another session can complete the branch.
                            postCommit();
                            result.onSuccess();
                        }

                        @Override
                        public void onFailure(ProviderException failure) {
                            branches.remove(key);
                            postCommit();
                            result.onFailure(failure);
                        }
                    }));
                break;
            case COMMIT_ONE_PHASE:
                if (branch == null || branch.prepared) {
                    result.onFailure(new ProviderXaException(XAException.XAER_NOTA, "Unknown branch " + xaRequest));
                    break;
                }
                setCurrent(null);
                preCommit();
                dischargeBranch(branch, key, false, xaRequest, result);
                break;
            case ROLLBACK:
                if (branch != null && !branch.prepared) {
                    setCurrent(null);
                    preRollback();
                    dischargeBranch(branch, key, true, xaRequest, result);
                } else {
                    withCoordinator(result, () -> coordinator.xaRollback(xid, xaRequest,
                        new XaCompletion(result) {
                            @Override
                            public void onSuccess() {
                                branches.remove(key);
                                result.onSuccess();
                            }
                        }));
                }
                break;
            case COMMIT:
                withCoordinator(result, () -> coordinator.xaCommit(xid, xaRequest,
                    new XaCompletion(result) {
                        @Override
                        public void onSuccess() {
                            branches.remove(key);
                            result.onSuccess();
                        }
                    }));
                break;
            case FORGET:
                withCoordinator(result, () -> coordinator.xaForget(xid, xaRequest, new XaCompletion(result)));
                break;
            case RECOVER:
                withCoordinator(result, () -> coordinator.xaRecover(xaRequest.getStartAfter(), xaRequest,
                    new XaCompletion(result)));
                break;
            default:
                throw new ProviderIllegalStateException("Unknown XA request " + xaRequest);
        }
    }

    private void xaStart(final JmsXaRequest xaRequest, final String key, final AsyncResult result) throws ProviderException {
        if (current != null || branches.containsKey(key)) {
            result.onFailure(new ProviderXaException(
                current != null ? XAException.XAER_PROTO : XAException.XAER_DUPID,
                "Cannot start a branch: " + xaRequest));
            return;
        }

        final JmsTransactionId txId = xaRequest.getTransactionId();
        withCoordinator(result, () -> coordinator.xaDeclare(txId, xaRequest.getXid(), xaRequest, new XaCompletion(result) {
            @Override
            public void onSuccess() {
                // A reply after the request has timed out does not associate the session.
                if (result.isComplete()) {
                    return;
                }
                branches.put(key, new XaBranch(txId));
                setCurrent(txId);
                result.onSuccess();
            }

            @Override
            public boolean isComplete() {
                return current != null;
            }
        }));
    }

    /**
     * Commits or rolls back a branch of this session that has not been prepared.
     */
    private void dischargeBranch(final XaBranch branch, final String key, boolean fail, JmsXaRequest xaRequest, final AsyncResult result) {
        withCoordinator(result, () -> coordinator.xaDischarge(branch.amqpTransactionId(), fail, xaRequest,
            new XaCompletion(result) {
                @Override
                public void onSuccess() {
                    branches.remove(key);
                    // The broker returns the messages of a branch that rolled back to the queue.
                    postCommit();
                    result.onSuccess();
                }

                @Override
                public void onFailure(ProviderException failure) {
                    branches.remove(key);
                    postCommit();
                    result.onFailure(failure);
                }
            }));
    }

    private void setCurrent(JmsTransactionId txId) {
        current = txId;
        inUnknownBranch = false;
        if (txId == null) {
            cachedAcceptedState = null;
            cachedTransactedState = null;
        } else {
            cachedAcceptedState = new TransactionalState();
            cachedAcceptedState.setOutcome(Accepted.getInstance());
            cachedAcceptedState.setTxnId(getAmqpTransactionId());
            cachedTransactedState = new TransactionalState();
            cachedTransactedState.setTxnId(getAmqpTransactionId());
        }
    }

    private void withCoordinator(final AsyncResult request, final XaAction action) {
        if (coordinator == null || coordinator.isClosed()) {
            AmqpTransactionCoordinatorBuilder builder =
                new AmqpTransactionCoordinatorBuilder(this, resourceInfo);
            builder.buildResource(new AsyncResult() {

                @Override
                public void onSuccess() {
                    run();
                }

                @Override
                public void onFailure(ProviderException result) {
                    request.onFailure(new ProviderXaException(XAException.XAER_RMERR,
                        "The broker does not support XA transactions: " + result.getMessage(), result));
                }

                @Override
                public boolean isComplete() {
                    return request.isComplete();
                }

                private void run() {
                    try {
                        action.run();
                    } catch (ProviderException e) {
                        request.onFailure(e);
                    }
                }
            });
        } else {
            try {
                action.run();
            } catch (ProviderException e) {
                request.onFailure(e);
            }
        }
    }

    /**
     * Passes the outcome of a request to the coordinator on to the original request.
     */
    private static class XaCompletion implements AsyncResult {

        private final AsyncResult request;

        private XaCompletion(AsyncResult request) {
            this.request = request;
        }

        @Override
        public void onSuccess() {
            request.onSuccess();
        }

        @Override
        public void onFailure(ProviderException result) {
            request.onFailure(result);
        }

        @Override
        public boolean isComplete() {
            return request.isComplete();
        }
    }

    private static String branchKey(Xid xid) {
        return xid.getFormatId() + ":" + java.util.Base64.getEncoder().encodeToString(xid.getGlobalTransactionId())
            + ":" + java.util.Base64.getEncoder().encodeToString(xid.getBranchQualifier());
    }

    //----- Context utility methods ------------------------------------------//

    public void registerTxConsumer(AmqpConsumer consumer) {
        txConsumers.put(consumer.getConsumerId(), consumer);
    }

    public boolean isInTransaction(JmsConsumerId consumerId) {
        return txConsumers.containsKey(consumerId);
    }

    public void registerTxProducer(AmqpProducer producer) {
        txProducers.put(producer.getProducerId(), producer);
    }

    public boolean isInTransaction(JmsProducerId producerId) {
        return txProducers.containsKey(producerId);
    }

    public AmqpSession getSession() {
        return session;
    }

    public TransactionalState getTxnAcceptState() {
        return cachedAcceptedState;
    }

    public TransactionalState getTxnEnrolledState() {
        return cachedTransactedState;
    }

    public JmsTransactionId getTransactionId() {
        return current;
    }

    public boolean isTransactionInDoubt() {
        if (session.getResourceInfo().isXa()) {
            // Outside of a branch, the work is not transactional.
            return inUnknownBranch || current != null && (coordinator == null || coordinator.isClosed());
        }
        // A context either has an active transaction or the transaction state of all
        // operations is in-doubt and cannot proceed as normal.
        return coordinator == null ? true : coordinator.isClosed();
    }

    public Binary getAmqpTransactionId() {
        Binary result = null;
        if (current != null) {
            result = (Binary) current.getProviderHint();
        }
        return result;
    }

    @Override
    public String toString() {
        return this.session.getSessionId() + ": txContext";
    }

    //----- Transaction pre / post completion --------------------------------//

    private void preCommit() {
        for (AmqpConsumer consumer : txConsumers.values()) {
            consumer.preCommit();
        }
    }

    private void preRollback() {
        for (AmqpConsumer consumer : txConsumers.values()) {
            consumer.preRollback();
        }
    }

    private void postCommit() {
        for (AmqpConsumer consumer : txConsumers.values()) {
            consumer.postCommit();
        }

        txConsumers.clear();
        txProducers.clear();
    }

    private void postRollback() {
        for (AmqpConsumer consumer : txConsumers.values()) {
            consumer.postRollback();
        }

        txConsumers.clear();
        txProducers.clear();
    }

    //----- Resource Parent implementation -----------------------------------//

    @Override
    public void addChildResource(AmqpResource resource) {
        if (resource instanceof AmqpTransactionCoordinator) {
            coordinator = (AmqpTransactionCoordinator) resource;
        }
    }

    @Override
    public void removeChildResource(AmqpResource resource) {
        // We don't clear the coordinator link so that we can refer to it
        // to check if the current TX has failed due to link closed during
        // normal operations.
    }

    @Override
    public AmqpProvider getProvider() {
        return session.getProvider();
    }

    //----- Completion for Commit or Rollback operation ----------------------//

    private abstract class Completion implements AsyncResult {

        protected boolean complete;
        protected ProviderException failure;

        @Override
        public boolean isComplete() {
            return complete;
        }

        public ProviderException getFailureCause() {
            return failure;
        }
    }

    private class DeclareCompletion extends Completion {

        protected final DischargeCompletion parent;

        public DeclareCompletion(DischargeCompletion parent) {
            this.parent = parent;
        }

        @Override
        public void onFailure(ProviderException result) {
            complete = true;
            failure = result;
            parent.onDeclareFailure(result);
        }

        @Override
        public void onSuccess() {
            complete = true;
            parent.onDeclareSuccess();
        }
    }

    public class DischargeCompletion extends Completion {

        private final DeclareCompletion declare;

        private final AsyncResult request;
        private final boolean commit;

        public DischargeCompletion(AsyncResult request, JmsTransactionInfo nextTx, boolean commit) {
            this.request = request;
            this.commit = commit;

            if (nextTx != null) {
                this.declare = new DeclareCompletion(this);
            } else {
                this.declare = null;
            }
        }

        public DeclareCompletion getDeclareCompletion() {
            return declare;
        }

        public boolean isCommit() {
            return commit;
        }

        public boolean isPipelined() {
            return declare != null;
        }

        @Override
        public void onFailure(ProviderException result) {
            complete = true;
            failure = result;
            onDischargeFailure(result);
        }

        @Override
        public void onSuccess() {
            complete = true;
            onDischargeSuccess();
        }

        public void onDeclareSuccess() {
            // If the discharge has not completed yet we wait until it does
            // so we end up with the correct result.
            if (isComplete()) {
                if (getFailureCause() == null) {
                    request.onSuccess();
                } else {
                    request.onFailure(getFailureCause());
                }
            }
        }

        public void onDischargeSuccess() {
            cleanup();

            // If the declare already returned a result we can proceed otherwise
            // we need to wait for it finish in order to get the correct outcome.
            if (declare == null) {
                request.onSuccess();
            } else if (declare.isComplete()) {
                if (declare.getFailureCause() == null) {
                    request.onSuccess();
                } else {
                    request.onFailure(declare.getFailureCause());
                }
            }
        }

        public void onDeclareFailure(ProviderException failure) {
            // If the discharge has not completed yet we wait until it does
            // so we end up with the correct result.
            if (isComplete()) {
                if (getFailureCause() == null) {
                    request.onFailure(failure);
                } else {
                    request.onFailure(getFailureCause());
                }
            }
        }

        public void onDischargeFailure(ProviderException failure) {
            cleanup();

            // If the declare already returned a result we can proceed otherwise
            // we need to wait for it finish in order to get the correct outcome.
            if (declare == null) {
                request.onFailure(failure);
            } else if (declare.isComplete()) {
                request.onFailure(failure);
            }
        }

        private void cleanup() {
            if (commit) {
                postCommit();
            } else {
                postRollback();
            }
        }
    }
}
