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

import java.nio.BufferOverflowException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import com.rabbitmq.client.jms.meta.JmsConnectionInfo;
import com.rabbitmq.client.jms.meta.JmsSessionInfo;
import com.rabbitmq.client.jms.meta.JmsTransactionId;
import com.rabbitmq.client.jms.provider.AsyncResult;
import com.rabbitmq.client.jms.provider.ProviderException;
import com.rabbitmq.client.jms.provider.amqp.AmqpTransactionContext.DischargeCompletion;
import com.rabbitmq.client.jms.provider.exceptions.ProviderExceptionSupport;
import com.rabbitmq.client.jms.provider.exceptions.ProviderIllegalStateException;
import com.rabbitmq.client.jms.provider.exceptions.ProviderOperationTimedOutException;
import com.rabbitmq.client.jms.provider.exceptions.ProviderTransactionInDoubtException;
import com.rabbitmq.client.jms.provider.exceptions.ProviderTransactionRolledBackException;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.transaction.GlobalTxId;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import com.rabbitmq.client.jms.meta.JmsXaRequest;
import com.rabbitmq.client.jms.provider.exceptions.ProviderXaException;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.transaction.Declare;
import org.apache.qpid.proton.amqp.transaction.Declared;
import org.apache.qpid.proton.amqp.transaction.Discharge;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the AMQP Transaction coordinator link used by the transaction context
 * of a session to control the lifetime of a given transaction.
 */
public class AmqpTransactionCoordinator extends AmqpAbstractResource<JmsSessionInfo, Sender> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpTransactionCoordinator.class);

    private static final Boolean ROLLBACK_MARKER = Boolean.FALSE;
    private static final Boolean COMMIT_MARKER = Boolean.TRUE;

    private final byte[] OUTBOUND_BUFFER = new byte[64];

    private final AmqpTransferTagGenerator tagGenerator = new AmqpTransferTagGenerator();

    public AmqpTransactionCoordinator(JmsSessionInfo resourceInfo, Sender endpoint, AmqpResourceParent parent) {
        super(resourceInfo, endpoint, parent);
    }

    @Override
    public void processDeliveryUpdates(AmqpProvider provider, Delivery delivery) throws ProviderException {

        try {
            if (delivery != null && delivery.remotelySettled()) {
                DeliveryState state = delivery.getRemoteState();

                if (delivery.getContext() == null || !(delivery.getContext() instanceof OperationContext)) {
                    return;
                }

                OperationContext context = (OperationContext) delivery.getContext();

                AsyncResult pendingRequest = context.getRequest();
                JmsTransactionId txId = context.getTransactionId();

                if (context.getXaRequest() != null) {
                    completeXa(context.getXaRequest(), state, txId, pendingRequest);
                } else if (state instanceof Declared) {
                    LOG.debug("New TX started: {}", txId);
                    Declared declared = (Declared) state;
                    txId.setProviderHint(declared.getTxnId());
                    pendingRequest.onSuccess();
                } else if (state instanceof Rejected) {
                    LOG.debug("Last TX request failed: {}", txId);
                    Rejected rejected = (Rejected) state;
                    ProviderException cause = AmqpSupport.convertToNonFatalException(getParent().getProvider(), getEndpoint(), rejected.getError());
                    if (COMMIT_MARKER.equals(txId.getProviderContext())) {
                        // A rejected commit has rolled back, for example with amqp:transaction:rollback.
                        if (!(cause instanceof ProviderTransactionRolledBackException)) {
                            cause = new ProviderTransactionRolledBackException(cause.getMessage(), cause);
                        }
                    } else {
                        cause = new ProviderTransactionInDoubtException(cause.getMessage(), cause);
                    }

                    txId.setProviderHint(null);
                    pendingRequest.onFailure(cause);
                } else {
                    LOG.debug("Last TX request succeeded: {}", txId);
                    pendingRequest.onSuccess();
                }

                // Reset state for next TX action.
                delivery.settle();
                pendingRequest = null;

                if (context.getTimeout() != null) {
                    context.getTimeout().cancel(false);
                }
            }

            super.processDeliveryUpdates(provider, delivery);
        } catch (Throwable e) {
            throw ProviderExceptionSupport.createNonFatalOrPassthrough(e);
        }
    }

    public void declare(JmsTransactionId txId, AsyncResult request) throws ProviderException {

        if (isClosed()) {
            request.onFailure(new ProviderIllegalStateException("Cannot start new transaction: Coordinator remotely closed"));
            return;
        }

        if (txId.getProviderHint() != null) {
            throw new ProviderIllegalStateException("Declar called while a TX is still Active.");
        }

        Message message = Message.Factory.create();
        Declare declare = new Declare();
        message.setBody(new AmqpValue(declare));

        ScheduledFuture<?> timeout = scheduleTimeoutIfNeeded("Timed out waiting for declare of TX.", request);
        OperationContext context = new OperationContext(txId, request, timeout);

        Delivery delivery = getEndpoint().delivery(tagGenerator.getNextTag());
        delivery.setContext(context);

        sendTxCommand(message);
    }

    public void discharge(JmsTransactionId txId, DischargeCompletion request) throws ProviderException {

        if (isClosed()) {
            ProviderException failureCause = null;

            if (request.isCommit()) {
                failureCause = new ProviderTransactionRolledBackException("Transaction inbout: Coordinator remotely closed");
            } else {
                failureCause = new ProviderIllegalStateException("Rollback cannot complete: Coordinator remotely closed");
            }

            request.onFailure(failureCause);
            return;
        }

        if (txId.getProviderHint() == null) {
            throw new ProviderIllegalStateException("Discharge called with no active Transaction.");
        }

        // Store the context of this action in the transaction ID for later completion.
        txId.setProviderContext(request.isCommit() ? COMMIT_MARKER : ROLLBACK_MARKER);

        Message message = Message.Factory.create();
        Discharge discharge = new Discharge();
        discharge.setFail(!request.isCommit());
        discharge.setTxnId((Binary) txId.getProviderHint());
        message.setBody(new AmqpValue(discharge));

        ScheduledFuture<?> timeout = scheduleTimeoutIfNeeded("Timed out waiting for discharge of TX.", request);
        OperationContext context = new OperationContext(txId, request, timeout);

        Delivery delivery = getEndpoint().delivery(tagGenerator.getNextTag());
        delivery.setContext(context);

        sendTxCommand(message);
    }

    //----- XA ---------------------------------------------------------------//

    private static final Symbol XA_XID = Symbol.valueOf("rabbitmq:xid");
    private static final Symbol XA_ANN_XIDS = Symbol.valueOf("x-opt-rabbitmq-xids");
    private static final Symbol XA_ANN_MORE = Symbol.valueOf("x-opt-rabbitmq-more");
    private static final Symbol TXN_ROLLBACK = Symbol.valueOf("amqp:transaction:rollback");
    private static final Symbol TXN_TIMEOUT = Symbol.valueOf("amqp:transaction:timeout");
    private static final Symbol TXN_UNKNOWN_ID = Symbol.valueOf("amqp:transaction:unknown-id");
    private static final Symbol XA_DUPLICATE_ID = Symbol.valueOf("rabbitmq:xa:duplicate-id");
    private static final Symbol XA_PROTOCOL_ERROR = Symbol.valueOf("rabbitmq:xa:protocol-error");
    private static final Symbol XA_HEURISTIC_ROLLBACK = Symbol.valueOf("rabbitmq:xa:heuristic-rollback");
    private static final Symbol XA_HEURISTIC_COMMIT = Symbol.valueOf("rabbitmq:xa:heuristic-commit");
    private static final Symbol INVALID_FIELD = Symbol.valueOf("amqp:invalid-field");

    /**
     * Declares a transaction that is the branch of a global transaction.
     */
    public void xaDeclare(JmsTransactionId txId, Xid xid, JmsXaRequest xaRequest, AsyncResult request) throws ProviderException {
        if (isClosed()) {
            request.onFailure(new ProviderIllegalStateException("Cannot start new transaction: Coordinator remotely closed"));
            return;
        }

        Declare declare = new Declare();
        declare.setGlobalId(describedXid(xid));
        sendXa(declare, txId, xaRequest, request, "Timed out waiting for declare of XA branch.");
    }

    /**
     * Discharges a branch that has not been prepared, which is a one-phase commit or a rollback.
     */
    public void xaDischarge(Binary txnId, boolean fail, JmsXaRequest xaRequest, AsyncResult request) throws ProviderException {
        if (isClosed()) {
            request.onFailure(new ProviderXaException(XAException.XAER_RMFAIL, "Coordinator remotely closed"));
            return;
        }

        Discharge discharge = new Discharge();
        discharge.setFail(fail);
        discharge.setTxnId(txnId);
        sendXa(discharge, null, xaRequest, request, "Timed out waiting for discharge of XA branch.");
    }

    public void xaPrepare(Binary txnId, JmsXaRequest xaRequest, AsyncResult request) throws ProviderException {
        xaControl("rabbitmq:xa-prepare", Arrays.asList(txnId), xaRequest, request);
    }

    public void xaCommit(Xid xid, JmsXaRequest xaRequest, AsyncResult request) throws ProviderException {
        xaControl("rabbitmq:xa-commit", Arrays.asList(describedXid(xid)), xaRequest, request);
    }

    public void xaRollback(Xid xid, JmsXaRequest xaRequest, AsyncResult request) throws ProviderException {
        xaControl("rabbitmq:xa-rollback", Arrays.asList(describedXid(xid)), xaRequest, request);
    }

    public void xaForget(Xid xid, JmsXaRequest xaRequest, AsyncResult request) throws ProviderException {
        xaControl("rabbitmq:xa-forget", Arrays.asList(describedXid(xid)), xaRequest, request);
    }

    public void xaRecover(Xid startAfter, JmsXaRequest xaRequest, AsyncResult request) throws ProviderException {
        List<Object> fields = new ArrayList<>();
        if (startAfter != null) {
            fields.add(describedXid(startAfter));
        }
        xaControl("rabbitmq:xa-recover", fields, xaRequest, request);
    }

    private void xaControl(String name, List<Object> fields, JmsXaRequest xaRequest, AsyncResult request) throws ProviderException {
        if (isClosed()) {
            request.onFailure(new ProviderXaException(XAException.XAER_RMFAIL, "Coordinator remotely closed"));
            return;
        }

        sendXa(new DescribedValue(Symbol.valueOf(name), fields), null, xaRequest, request,
            "Timed out waiting for " + name + ".");
    }

    private void sendXa(Object body, JmsTransactionId txId, JmsXaRequest xaRequest, AsyncResult request, String timeoutMessage) throws ProviderException {
        Message message = Message.Factory.create();
        message.setBody(new AmqpValue(body));

        ScheduledFuture<?> timeout = scheduleTimeoutIfNeeded(timeoutMessage, request);
        OperationContext context = new OperationContext(txId, request, timeout);
        context.setXaRequest(xaRequest);

        Delivery delivery = getEndpoint().delivery(tagGenerator.getNextTag());
        delivery.setContext(context);

        sendTxCommand(message);
    }

    private static DescribedXid describedXid(Xid xid) {
        return new DescribedXid(Arrays.asList(xid.getFormatId(),
            new Binary(xid.getGlobalTransactionId()), new Binary(xid.getBranchQualifier())));
    }

    private static void completeXa(JmsXaRequest xaRequest, DeliveryState state, JmsTransactionId txId, AsyncResult request) {
        JmsXaRequest.Type type = xaRequest.getType();
        if (state instanceof Rejected) {
            LOG.debug("Last XA request failed: {}", xaRequest);
            request.onFailure(toXaException(((Rejected) state).getError()));
        } else if (type == JmsXaRequest.Type.START && state instanceof Declared) {
            txId.setProviderHint(((Declared) state).getTxnId());
            request.onSuccess();
        } else if (type == JmsXaRequest.Type.RECOVER && state instanceof Modified) {
            try {
                readRecovered((Modified) state, xaRequest);
                request.onSuccess();
            } catch (RuntimeException e) {
                request.onFailure(new ProviderXaException(XAException.XAER_RMERR,
                    "Unexpected reply to recover: " + e.getMessage(), e));
            }
        } else if (type != JmsXaRequest.Type.START && type != JmsXaRequest.Type.RECOVER && state instanceof Accepted) {
            request.onSuccess();
        } else {
            request.onFailure(new ProviderXaException(XAException.XAER_RMERR,
                "Unexpected reply " + state + " to " + xaRequest));
        }
    }

    @SuppressWarnings("unchecked")
    private static void readRecovered(Modified modified, JmsXaRequest xaRequest) {
        Map<Symbol, Object> annotations = modified.getMessageAnnotations();
        if (annotations == null || !(annotations.get(XA_ANN_XIDS) instanceof List)) {
            throw new IllegalArgumentException("no " + XA_ANN_XIDS + " annotation");
        }
        List<Xid> xids = new ArrayList<>();
        for (Object entry : (List<Object>) annotations.get(XA_ANN_XIDS)) {
            List<Object> fields = (List<Object>) entry;
            xids.add(new BranchId((Integer) fields.get(0), ((Binary) fields.get(1)).getArray(), ((Binary) fields.get(2)).getArray()));
        }
        Object more = annotations.get(XA_ANN_MORE);
        xaRequest.setRecovered(xids, Boolean.TRUE.equals(more));
    }

    static ProviderXaException toXaException(ErrorCondition error) {
        Symbol condition = error == null ? null : error.getCondition();
        String description = error == null ? "" : String.valueOf(error.getDescription());
        int code;
        if (TXN_ROLLBACK.equals(condition)) {
            code = XAException.XA_RBROLLBACK;
        } else if (TXN_TIMEOUT.equals(condition)) {
            code = XAException.XA_RBTIMEOUT;
        } else if (TXN_UNKNOWN_ID.equals(condition)) {
            code = XAException.XAER_NOTA;
        } else if (XA_DUPLICATE_ID.equals(condition)) {
            code = XAException.XAER_DUPID;
        } else if (XA_PROTOCOL_ERROR.equals(condition)) {
            code = XAException.XAER_PROTO;
        } else if (XA_HEURISTIC_ROLLBACK.equals(condition)) {
            code = XAException.XA_HEURRB;
        } else if (XA_HEURISTIC_COMMIT.equals(condition)) {
            code = XAException.XA_HEURCOM;
        } else if (INVALID_FIELD.equals(condition)) {
            code = XAException.XAER_INVAL;
        } else {
            code = XAException.XAER_RMERR;
        }
        return new ProviderXaException(code, condition + ": " + description);
    }

    /** The branch identifier, which is the global id of a declare. */
    private static final class DescribedXid implements DescribedType, GlobalTxId {

        private final List<Object> fields;

        private DescribedXid(List<Object> fields) {
            this.fields = fields;
        }

        @Override
        public Object getDescriptor() {
            return XA_XID;
        }

        @Override
        public Object getDescribed() {
            return fields;
        }
    }

    /** A described value that the AMQP codec encodes as it is. */
    private static final class DescribedValue implements DescribedType {

        private final Symbol descriptor;
        private final Object described;

        private DescribedValue(Symbol descriptor, Object described) {
            this.descriptor = descriptor;
            this.described = described;
        }

        @Override
        public Object getDescriptor() {
            return descriptor;
        }

        @Override
        public Object getDescribed() {
            return described;
        }
    }

    /** An XA branch identifier that was received from the broker. */
    static final class BranchId implements Xid {

        private final int formatId;
        private final byte[] globalTransactionId;
        private final byte[] branchQualifier;

        BranchId(int formatId, byte[] globalTransactionId, byte[] branchQualifier) {
            this.formatId = formatId;
            this.globalTransactionId = globalTransactionId;
            this.branchQualifier = branchQualifier;
        }

        @Override
        public int getFormatId() {
            return formatId;
        }

        @Override
        public byte[] getGlobalTransactionId() {
            return globalTransactionId;
        }

        @Override
        public byte[] getBranchQualifier() {
            return branchQualifier;
        }
    }

    //----- Base class overrides ---------------------------------------------//

    @Override
    public void closeResource(AmqpProvider provider, ProviderException cause, boolean localClose) {

        // Alert any pending operation that the link failed to complete the pending
        // begin / commit / rollback operation.
        Delivery pending = getEndpoint().head();
        while (pending != null) {
            Delivery nextPending = pending.next();
            if (pending.getContext() != null && pending.getContext() instanceof OperationContext) {
                OperationContext context = (OperationContext) pending.getContext();
                context.request.onFailure(cause);
            }

            pending = nextPending;
        }

        // Override the base class version because we do not want to propagate
        // an error up to the client if remote close happens as that is an
        // acceptable way for the remote to indicate the discharge could not
        // be applied.

        if (getParent() != null) {
            getParent().removeChildResource(this);
        }

        if (getEndpoint() != null) {
            getEndpoint().close();
            getEndpoint().free();
        }

        LOG.debug("Transaction Coordinator link {} was remotely closed", getResourceInfo());
    }

    //----- Internal implementation ------------------------------------------//

    private class OperationContext {

        private final AsyncResult request;
        private final ScheduledFuture<?> timeout;
        private final JmsTransactionId transactionId;
        private JmsXaRequest xaRequest;

        public OperationContext(JmsTransactionId transactionId, AsyncResult request, ScheduledFuture<?> timeout) {
            this.transactionId = transactionId;
            this.request = request;
            this.timeout = timeout;
        }

        public JmsTransactionId getTransactionId() {
            return transactionId;
        }

        public JmsXaRequest getXaRequest() {
            return xaRequest;
        }

        public void setXaRequest(JmsXaRequest xaRequest) {
            this.xaRequest = xaRequest;
        }

        public AsyncResult getRequest() {
            return request;
        }

        public ScheduledFuture<?> getTimeout() {
            return timeout;
        }
    }

    private ScheduledFuture<?> scheduleTimeoutIfNeeded(String cause, AsyncResult pendingRequest) {
        AmqpProvider provider = getParent().getProvider();
        if (provider.getRequestTimeout() != JmsConnectionInfo.INFINITE) {
            return provider.scheduleRequestTimeout(pendingRequest, provider.getRequestTimeout(), new ProviderOperationTimedOutException(cause));
        } else {
            return null;
        }
    }

    private void sendTxCommand(Message message) throws ProviderException {
        int encodedSize = 0;
        byte[] buffer = OUTBOUND_BUFFER;
        while (true) {
            try {
                encodedSize = message.encode(buffer, 0, buffer.length);
                break;
            } catch (BufferOverflowException e) {
                buffer = new byte[buffer.length * 2];
            }
        }

        Sender sender = getEndpoint();
        sender.send(buffer, 0, encodedSize);
        sender.advance();
    }
}
