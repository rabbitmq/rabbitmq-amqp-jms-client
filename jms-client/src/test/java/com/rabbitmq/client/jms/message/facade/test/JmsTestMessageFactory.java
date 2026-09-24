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
package com.rabbitmq.client.jms.message.facade.test;

import java.io.Serializable;

import jakarta.jms.JMSException;

import com.rabbitmq.client.jms.message.JmsBytesMessage;
import com.rabbitmq.client.jms.message.JmsMapMessage;
import com.rabbitmq.client.jms.message.JmsMessage;
import com.rabbitmq.client.jms.message.JmsMessageFactory;
import com.rabbitmq.client.jms.message.JmsObjectMessage;
import com.rabbitmq.client.jms.message.JmsStreamMessage;
import com.rabbitmq.client.jms.message.JmsTextMessage;

/**
 * Implementation of the JmsMessageFactory that create simple
 * jakarta.jms.Message instances with test facades.
 */
public class JmsTestMessageFactory implements JmsMessageFactory {

    @Override
    public JmsMessage createMessage() throws UnsupportedOperationException {
        return new JmsMessage(new JmsTestMessageFacade());
    }

    @Override
    public JmsTextMessage createTextMessage() throws UnsupportedOperationException {
        return createTextMessage(null);
    }

    @Override
    public JmsTextMessage createTextMessage(String payload) throws UnsupportedOperationException {
        JmsTextMessage result = new JmsTextMessage(new JmsTestTextMessageFacade());
        if (payload != null) {
            try {
                result.setText(payload);
            } catch (JMSException e) {
            }
        }
        return result;
    }

    @Override
    public JmsBytesMessage createBytesMessage() throws UnsupportedOperationException {
        return new JmsBytesMessage(new JmsTestBytesMessageFacade());
    }

    @Override
    public JmsMapMessage createMapMessage() throws UnsupportedOperationException {
        return new JmsMapMessage(new JmsTestMapMessageFacade());
    }

    @Override
    public JmsStreamMessage createStreamMessage() throws UnsupportedOperationException {
        return new JmsStreamMessage(new JmsTestStreamMessageFacade());
    }

    @Override
    public JmsObjectMessage createObjectMessage() throws UnsupportedOperationException {
        return createObjectMessage(null);
    }

    @Override
    public JmsObjectMessage createObjectMessage(Serializable payload) throws UnsupportedOperationException {
        JmsObjectMessage result = new JmsObjectMessage(new JmsTestObjectMessageFacade());
        if (payload != null) {
            try {
                result.setObject(payload);
            } catch (Exception e) {
            }
        }
        return result;
    }
}
