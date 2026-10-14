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

import javax.transaction.xa.XAResource;

import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.TransactionInProgressRuntimeException;
import jakarta.jms.XAJMSContext;

/**
 * A context whose work is part of the XA transactions that a transaction manager controls.
 */
public class JmsXAContext extends JmsContext implements XAJMSContext {

    public JmsXAContext(JmsXAConnection connection) {
        super(connection, JMSContext.SESSION_TRANSACTED);
    }

    @Override
    protected JmsSession createSession(JmsConnection connection, int sessionMode) throws JMSException {
        return (JmsSession) ((JmsXAConnection) connection).createXASession();
    }

    @Override
    public JMSContext getContext() {
        return this;
    }

    @Override
    public XAResource getXAResource() {
        return ((JmsXASession) getSession()).getXAResource();
    }

    @Override
    public void commit() {
        throw new TransactionInProgressRuntimeException("An XA context cannot commit, the transaction manager does");
    }

    @Override
    public void rollback() {
        throw new TransactionInProgressRuntimeException("An XA context cannot roll back, the transaction manager does");
    }
}
