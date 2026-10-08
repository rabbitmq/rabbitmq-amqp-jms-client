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
package com.rabbitmq.client.jms;

import jakarta.jms.JMSException;
import jakarta.jms.TransactionInProgressException;

import com.rabbitmq.client.jms.message.JmsInboundMessageDispatch;
import com.rabbitmq.client.jms.message.JmsOutboundMessageDispatch;
import com.rabbitmq.client.jms.meta.JmsResourceId;
import com.rabbitmq.client.jms.meta.JmsTransactionId;
import com.rabbitmq.client.jms.provider.Provider;
import com.rabbitmq.client.jms.provider.ProviderConstants.ACK_TYPE;
import com.rabbitmq.client.jms.provider.ProviderSynchronization;

/**
 * The transaction context of a session that takes part in XA transactions.
 *
 * The work of the session is transactional while it is associated with a branch, and the
 * transaction manager controls the branch through the {@link JmsXAResource}. Outside of
 * a branch the work is not transactional.
 */
public class JmsXATransactionContext implements JmsTransactionContext {

    private final JmsSession session;
    private final JmsXAResource xaResource;
    private volatile boolean inBranch;

    public JmsXATransactionContext(JmsXASession session) {
        this.session = session;
        this.xaResource = new JmsXAResource(session);
    }

    @Override
    public void send(JmsConnection connection, JmsOutboundMessageDispatch envelope, ProviderSynchronization outcome) throws JMSException {
        if (xaResource.isAssociatedWithLostBranch()) {
            if (outcome != null) {
                outcome.onPendingSuccess();
            }
            if (envelope.isCompletionRequired()) {
                connection.onCompletedMessageSend(envelope);
            }
            return;
        }
        connection.send(envelope, outcome);
    }

    @Override
    public void acknowledge(JmsConnection connection, JmsInboundMessageDispatch envelope, ACK_TYPE ackType) throws JMSException {
        connection.acknowledge(envelope, ackType);
    }

    @Override
    public boolean isInDoubt() {
        return xaResource.isAssociatedWithLostBranch();
    }

    @Override
    public void begin() throws JMSException {
        // The transaction manager starts a branch.
    }

    @Override
    public void rollback() throws JMSException {
        throw new TransactionInProgressException("A session of an XA connection cannot roll back, the transaction manager does");
    }

    @Override
    public void commit() throws JMSException {
        throw new TransactionInProgressException("A session of an XA connection cannot commit, the transaction manager does");
    }

    @Override
    public void shutdown() throws JMSException {
        // Closing the session ends its control link, which rolls back an open branch.
        inBranch = false;
    }

    @Override
    public JmsTransactionId getTransactionId() {
        return null;
    }

    @Override
    public JmsTransactionListener getListener() {
        return null;
    }

    @Override
    public void setListener(JmsTransactionListener listener) {
    }

    @Override
    public boolean isInTransaction() {
        return inBranch;
    }

    @Override
    public boolean isActiveInThisContext(JmsResourceId resouceId) {
        return inBranch;
    }

    @Override
    public void onConnectionInterrupted() {
        xaResource.onConnectionInterrupted();
    }

    @Override
    public void onConnectionRecovery(Provider provider) throws Exception {
        xaResource.onConnectionRecovery(provider);
    }

    void setInBranch(boolean inBranch) {
        this.inBranch = inBranch;
    }

    JmsSession getSession() {
        return session;
    }

    JmsXAResource getXAResource() {
        return xaResource;
    }
}
