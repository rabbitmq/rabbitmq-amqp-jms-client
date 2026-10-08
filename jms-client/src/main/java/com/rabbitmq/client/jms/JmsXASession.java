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

import jakarta.jms.JMSException;
import jakarta.jms.Session;
import jakarta.jms.XASession;

import com.rabbitmq.client.jms.meta.JmsSessionId;

/**
 * A session whose work is part of the XA transactions that a transaction manager controls.
 */
public class JmsXASession extends JmsSession implements XASession {

    private final JmsXAResource xaResource;

    protected JmsXASession(JmsConnection connection, JmsSessionId sessionId) throws JMSException {
        super(connection, sessionId, Session.SESSION_TRANSACTED);
        this.xaResource = new JmsXAResource(this);
    }

    @Override
    protected JmsTransactionContext createTransactionContext() {
        return new JmsXATransactionContext(this);
    }

    @Override
    protected boolean isXa() {
        return true;
    }

    @Override
    public Session getSession() throws JMSException {
        checkClosed();
        return this;
    }

    @Override
    public XAResource getXAResource() {
        return xaResource;
    }

    @Override
    public boolean getTransacted() throws JMSException {
        checkClosed();
        return true;
    }
}
