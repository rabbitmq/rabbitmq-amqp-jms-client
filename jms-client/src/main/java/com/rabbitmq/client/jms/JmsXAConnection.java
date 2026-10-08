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
import jakarta.jms.XAConnection;
import jakarta.jms.XASession;

import com.rabbitmq.client.jms.meta.JmsConnectionInfo;
import com.rabbitmq.client.jms.provider.Provider;

/**
 * A connection whose sessions take part in XA transactions.
 */
public class JmsXAConnection extends JmsConnection implements XAConnection {

    protected JmsXAConnection(JmsConnectionInfo connectionInfo, Provider provider) throws JMSException {
        super(connectionInfo, provider);
    }

    @Override
    public XASession createXASession() throws JMSException {
        return createXASessionInternal();
    }
}
