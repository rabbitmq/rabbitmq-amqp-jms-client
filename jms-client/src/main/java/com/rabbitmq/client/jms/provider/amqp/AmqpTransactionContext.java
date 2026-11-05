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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import javax.transaction.xa.XAException;

import com.rabbitmq.client.jms.meta.JmsConsumerId;
import com.rabbitmq.client.jms.meta.JmsProducerId;
import com.rabbitmq.client.jms.meta.JmsSessionInfo;
import com.rabbitmq.client.jms.meta.JmsTransactionId;
import com.rabbitmq.client.jms.meta.JmsTransactionInfo;
import com.rabbitmq.client.jms.meta.JmsXaRequest;
import com.rabbitmq.client.jms.provider.exceptions.ProviderXaException;
import com.rabbitmq.client.jms.provider.AsyncResult;
import com.rabbitmq.client.jms.provider.NoOpAsyncResult;
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

    private final List<Runnable> awaitingDeclare = new ArrayList<>();
    private boolean declaring;
    private ProviderException declareFailure;

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

    /**
     * Starts a local transaction. The transaction is declared when the first work is
     * done in it, so that a transaction without work sends nothing to the remote peer.
     *
     * @param txId
     *        the id of the new transaction.
     * @param request
     *        the request to signal once the coordinator link is attached.
     *
     * @throws ProviderException if the current transaction has been declared.
     */
    public void begin(final JmsTransactionId txId, final AsyncResult request) throws ProviderException {
        if (declaring || current != null && current.getProviderHint() != null) {
            throw new ProviderIllegalStateException("Begin called while a TX is still Active.");
        }

        withCoordinator(request, () -> {
            startTransaction(txId);
            request.onSuccess();
        }, Function.identity());
    }

    /**
     * Whether work of the current local transaction has to wait until the transaction
     * has been declared.
     *
     * @return true if the current local transaction has not been declared yet.
     */
    public boolean isDeclareNeeded() {
        return !resourceInfo.isXa() && current != null &&
               current.getProviderHint() == null && declareFailure == null;
    }

    /**
     * Runs the action once the current local transaction has been declared, and sends the
     * declare if it has not been sent yet. The action also runs if the declare fails, and
     * then finds the transaction in doubt.
     *
     * @param request
     *        the request to fail if the action throws.
     * @param action
     *        the work to run.
     */
    public void afterDeclare(final AsyncResult request, final Action action) {
        awaitingDeclare.add(() -> run(request, action));
        if (declaring) {
            return;
        }

        declaring = true;
        final DeclareCompletion completion = new DeclareCompletion(current);
        withCoordinator(completion, () -> coordinator.declare(completion.txId, completion), Function.identity());
        // A send does not pump the transport unless it writes a transfer.
        getProvider().pumpToProtonTransport(completion);
    }

    /**
     * Whether the declare of the current local transaction has been sent and not answered.
     *
     * @return true if a declare is outstanding.
     */
    public boolean isDeclarePending() {
        return declaring;
    }

    /**
     * Whether the declare of the current local transaction failed, which means that the
     * remote peer has not seen any work of the transaction.
     *
     * @return true if the current local transaction can only roll back.
     */
    public boolean isDeclareFailed() {
        return declareFailure != null;
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

        if (declaring) {
            afterDeclare(request, () -> commit(transactionInfo, nextTransactionInfo, request));
            return;
        }

        preCommit();

        LOG.trace("TX Context[{}] committing current TX[[]]", this, current);

        if (declareFailure != null || current.getProviderHint() == null) {
            ProviderException failure = declareFailure;
            startTransaction(nextTransactionInfo);
            if (failure == null) {
                postCommit();
                request.onSuccess();
            } else {
                postFailedCommit(true);
                request.onFailure(new ProviderTransactionRolledBackException(
                    "Transaction could not be declared: " + failure.getMessage(), failure));
            }
            return;
        }

        coordinator.discharge(current, true, new DischargeCompletion(request, true));
        startTransaction(nextTransactionInfo);
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

        if (declaring) {
            afterDeclare(request, () -> rollback(transactionInfo, nextTransactionInfo, request));
            return;
        }

        preRollback();

        LOG.trace("TX Context[{}] rolling back current TX[[]]", this, current);

        if (declareFailure != null || current.getProviderHint() == null) {
            startTransaction(nextTransactionInfo);
            postRollback();
            request.onSuccess();
            return;
        }

        coordinator.discharge(current, false, new DischargeCompletion(request, false));
        startTransaction(nextTransactionInfo);
    }

    private void startTransaction(JmsTransactionInfo transactionInfo) {
        startTransaction(transactionInfo == null ? null : transactionInfo.getId());
    }

    private void startTransaction(JmsTransactionId txId) {
        current = txId;
        declareFailure = null;
        cachedAcceptedState = null;
        cachedTransactedState = null;
    }

    private void runAwaitingDeclare() {
        declaring = false;
        List<Runnable> actions = new ArrayList<>(awaitingDeclare);
        awaitingDeclare.clear();
        for (Runnable action : actions) {
            action.run();
        }
    }

    private static void run(AsyncResult request, Action action) {
        try {
            action.run();
        } catch (ProviderException e) {
            request.onFailure(e);
        }
    }

    //----- XA ---------------------------------------------------------------//

    private final Map<String, JmsTransactionId> xaBranches = new HashMap<>();
    // Branches that the remote peer rolled back when it detached the control link.
    private final Set<String> rolledBackBranches = new HashSet<>();
    private boolean inUnknownBranch;

    /**
     * Work that runs on the provider thread and can fail.
     */
    public interface Action {
        void run() throws ProviderException;
    }

    /**
     * Runs an operation on an XA branch. The session is not in a transaction unless a branch
     * has been started and not ended, which means that the work of the session is not
     * transactional in between.
     *
     * The JMS XA resource checks the state of the branches. This context only keeps the
     * transactions of the branches of the session that have not been prepared.
     *
     * @param xaRequest
     *        describes the operation.
     * @param result
     *        the request to signal when the operation completes.
     *
     * @throws ProviderException if the operation cannot be started.
     */
    public void xa(final JmsXaRequest xaRequest, final AsyncResult result) throws ProviderException {
        if (coordinator != null && coordinator.isClosed() && !xaBranches.isEmpty()) {
            // "If the control link is closed while there exist non-discharged transactions
            // it created, then all such transactions are immediately rolled back" [4.1]
            rolledBackBranches.addAll(xaBranches.keySet());
            xaBranches.clear();
            for (AmqpConsumer consumer : txConsumers.values()) {
                consumer.giveBackTransactedDeliveries();
            }
            txConsumers.clear();
            txProducers.clear();
        }
        final String key = xaRequest.getKey();
        final JmsTransactionId txId = key == null ? null : xaBranches.get(key);

        switch (xaRequest.getType()) {
            case START:
                xaStart(xaRequest, result);
                break;
            case RESUME:
                // After a reconnect the branch is unknown, so the work of the session is dropped until it ends.
                setCurrent(txId);
                inUnknownBranch = txId == null;
                result.onSuccess();
                break;
            case END:
                boolean lost = isTransactionInDoubt();
                setCurrent(null);
                if (lost) {
                    postLostBranch();
                }
                result.onSuccess();
                break;
            case PREPARE:
                if (txId == null) {
                    result.onFailure(unknownBranch(key, xaRequest));
                    break;
                }
                preCommit();
                withXaCoordinator(result, () -> coordinator.xaPrepare(amqpTransactionId(txId), xaRequest,
                    new BranchCompletion(key, result)));
                break;
            case COMMIT_ONE_PHASE:
                if (txId == null) {
                    result.onFailure(unknownBranch(key, xaRequest));
                    break;
                }
                preCommit();
                withXaCoordinator(result, () -> coordinator.xaDischarge(amqpTransactionId(txId), false, xaRequest,
                    new BranchCompletion(key, result)));
                break;
            case ROLLBACK:
                if (txId == null && rolledBackBranches.remove(key)) {
                    result.onSuccess();
                    break;
                }
                if (txId == null) {
                    withXaCoordinator(result, () -> coordinator.xaRollback(xaRequest.getXid(), xaRequest,
                        new XaCompletion(result)));
                    break;
                }
                if (txId.equals(current)) {
                    setCurrent(null);
                }
                preRollback();
                withXaCoordinator(result, () -> coordinator.xaDischarge(amqpTransactionId(txId), true, xaRequest,
                    new BranchCompletion(key, result)));
                break;
            case COMMIT:
                withXaCoordinator(result, () -> coordinator.xaCommit(xaRequest.getXid(), xaRequest,
                    new XaCompletion(result)));
                break;
            case FORGET:
                withXaCoordinator(result, () -> coordinator.xaForget(xaRequest.getXid(), xaRequest,
                    new XaCompletion(result)));
                break;
            case RECOVER:
                withXaCoordinator(result, () -> coordinator.xaRecover(xaRequest.getStartAfter(), xaRequest,
                    new XaCompletion(result)));
                break;
            case GET_TIMEOUT:
                withXaCoordinator(result, () -> {
                    xaRequest.setTimeout(coordinator.getTransactionTimeout());
                    result.onSuccess();
                });
                break;
            default:
                throw new ProviderIllegalStateException("Unknown XA request " + xaRequest);
        }
    }

    private void xaStart(final JmsXaRequest xaRequest, final AsyncResult result) {
        final JmsTransactionId txId = xaRequest.getTransactionId();
        withXaCoordinator(result, () -> coordinator.xaDeclare(txId, xaRequest.getXid(), xaRequest, new XaCompletion(result) {
            @Override
            public void onSuccess() {
                // A reply after the request has timed out does not associate the session.
                if (!result.isComplete()) {
                    xaBranches.put(xaRequest.getKey(), txId);
                    setCurrent(txId);
                    super.onSuccess();
                } else {
                    // The broker holds the transaction as the one of the session, and
                    // would refuse the next start.
                    rollbackLateBranch(xaRequest, txId);
                }
            }
        }));
    }

    private ProviderXaException unknownBranch(String key, JmsXaRequest xaRequest) {
        if (rolledBackBranches.remove(key)) {
            return new ProviderXaException(XAException.XA_RBROLLBACK,
                "Branch rolled back when the control link was detached " + xaRequest);
        }
        return new ProviderXaException(XAException.XAER_NOTA, "Unknown branch " + xaRequest);
    }

    private void rollbackLateBranch(JmsXaRequest start, JmsTransactionId txId) {
        JmsXaRequest rollback = new JmsXaRequest(JmsXaRequest.Type.ROLLBACK, start.getSessionId(),
                                                 start.getXid(), start.getKey());
        try {
            coordinator.xaDischarge(amqpTransactionId(txId), true, rollback, NoOpAsyncResult.INSTANCE);
        } catch (ProviderException e) {
            LOG.debug("{} Failed to roll back the branch of a start that timed out: {}", this, e.getMessage());
        }
    }

    private static Binary amqpTransactionId(JmsTransactionId txId) {
        return (Binary) txId.getProviderHint();
    }

    private void setCurrent(JmsTransactionId txId) {
        startTransaction(txId);
        inUnknownBranch = false;
        if (txId != null) {
            cacheTransactionalStates();
        }
    }

    private void cacheTransactionalStates() {
        cachedAcceptedState = new TransactionalState();
        cachedAcceptedState.setOutcome(Accepted.getInstance());
        cachedAcceptedState.setTxnId(getAmqpTransactionId());
        cachedTransactedState = new TransactionalState();
        cachedTransactedState.setTxnId(getAmqpTransactionId());
    }

    private void withXaCoordinator(final AsyncResult request, final Action action) {
        withCoordinator(request, action, cause -> new ProviderXaException(XAException.XAER_RMERR,
            "The broker does not support XA transactions: " + cause.getMessage(), cause));
    }

    private void withCoordinator(final AsyncResult request, final Action action,
                                 final Function<ProviderException, ProviderException> attachFailure) {
        if (coordinator == null || coordinator.isClosed()) {
            AmqpTransactionCoordinatorBuilder builder =
                new AmqpTransactionCoordinatorBuilder(this, resourceInfo);
            builder.buildResource(new AsyncResult() {

                @Override
                public void onSuccess() {
                    run(request, action);
                }

                @Override
                public void onFailure(ProviderException result) {
                    request.onFailure(attachFailure.apply(result));
                }

                @Override
                public boolean isComplete() {
                    return request.isComplete();
                }
            });
        } else {
            run(request, action);
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

    /**
     * Ends the transaction of a branch of this session that has not been prepared.
     */
    private final class BranchCompletion extends XaCompletion {

        private final String key;

        private BranchCompletion(String key, AsyncResult request) {
            super(request);
            this.key = key;
        }

        @Override
        public void onSuccess() {
            endBranch();
            super.onSuccess();
        }

        @Override
        public void onFailure(ProviderException result) {
            endBranch();
            super.onFailure(result);
        }

        private void endBranch() {
            xaBranches.remove(key);
            // Another session can complete a prepared branch, and the broker returns the
            // messages of a branch that rolled back to the queue, so the deliveries are settled.
            postCommit();
        }
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
        if (declareFailure != null) {
            return true;
        }
        if (isDeclareNeeded()) {
            return false;
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

    // The consumers stay registered for another branch of the session that has not
    // been prepared yet.
    private void postLostBranch() {
        for (AmqpConsumer consumer : txConsumers.values()) {
            consumer.postLostBranch();
        }
    }

    private void postFailedCommit(boolean rolledBack) {
        for (AmqpConsumer consumer : txConsumers.values()) {
            consumer.postFailedCommit(rolledBack);
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

    //----- Completion for Declare, Commit or Rollback operation -------------//

    private final class DeclareCompletion implements AsyncResult {

        private final JmsTransactionId txId;
        private boolean complete;

        private DeclareCompletion(JmsTransactionId txId) {
            this.txId = txId;
        }

        @Override
        public void onSuccess() {
            if (complete) {
                rollbackLateDeclare();
                return;
            }
            complete = true;
            cacheTransactionalStates();
            runAwaitingDeclare();
        }

        // The remote peer holds a transaction without work after a declare that timed
        // out, and would refuse the next declare of the session.
        private void rollbackLateDeclare() {
            try {
                coordinator.discharge(txId, false, NoOpAsyncResult.INSTANCE);
            } catch (ProviderException e) {
                LOG.debug("{} Failed to roll back TX[{}] declared after its timeout: {}", AmqpTransactionContext.this, txId, e.getMessage());
            }
            txId.setProviderHint(null);
        }

        @Override
        public void onFailure(ProviderException result) {
            if (complete) {
                return;
            }
            complete = true;
            LOG.debug("{} Failed to declare TX[{}]: {}", AmqpTransactionContext.this, txId, result);
            declareFailure = result != null ? result :
                new ProviderIllegalStateException("The coordinator link was closed before the TX was declared");
            runAwaitingDeclare();
        }

        @Override
        public boolean isComplete() {
            return complete;
        }
    }

    private final class DischargeCompletion implements AsyncResult {

        private final AsyncResult request;
        private final boolean commit;
        private boolean complete;

        private DischargeCompletion(AsyncResult request, boolean commit) {
            this.request = request;
            this.commit = commit;
        }

        @Override
        public void onFailure(ProviderException result) {
            complete = true;
            cleanup(result);
            request.onFailure(result);
        }

        @Override
        public void onSuccess() {
            complete = true;
            cleanup(null);
            request.onSuccess();
        }

        @Override
        public boolean isComplete() {
            return complete;
        }

        private void cleanup(ProviderException failure) {
            if (!commit) {
                postRollback();
            } else if (failure == null) {
                postCommit();
            } else {
                postFailedCommit(failure instanceof ProviderTransactionRolledBackException);
            }
        }
    }
}
