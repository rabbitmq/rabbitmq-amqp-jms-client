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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.ExceptionListener;
import jakarta.jms.IllegalStateException;
import jakarta.jms.QueueSession;
import jakarta.jms.Session;
import jakarta.jms.TopicSession;
import jakarta.jms.TransactionInProgressException;
import jakarta.jms.XAQueueConnection;
import jakarta.jms.XAQueueConnectionFactory;
import jakarta.jms.XAQueueSession;
import jakarta.jms.XASession;
import jakarta.jms.XATopicConnection;
import jakarta.jms.XATopicConnectionFactory;
import jakarta.jms.XATopicSession;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Timeout(20)
public class JmsXADomainTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testQueueFactoryAndSession(boolean explicitCredentials) throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory("default-user", "default-password", "mock://localhost");
        ExceptionListener listener = exception -> {};
        factory.setExceptionListener(listener);
        XAQueueConnectionFactory queueFactory = factory;
        try (XAQueueConnection connection = explicitCredentials
                ? queueFactory.createXAQueueConnection("user", "password") : queueFactory.createXAQueueConnection()) {
            assertEquals(explicitCredentials ? "user" : "default-user", ((JmsConnection) connection).getUsername());
            assertEquals(explicitCredentials ? "password" : "default-password", ((JmsConnection) connection).getPassword());
            assertSame(listener, connection.getExceptionListener());
            assertThrows(IllegalStateException.class, () -> ((JmsConnection) connection).createTopicSession(false, Session.AUTO_ACKNOWLEDGE));
            connection.start();

            XAQueueSession xaSession = connection.createXAQueueSession();
            QueueSession session = xaSession.getQueueSession();
            assertSame(session, xaSession.getSession());
            assertTrue(((JmsSession) session).isStarted());
            assertTrue(session.getTransacted());
            assertEquals(Session.SESSION_TRANSACTED, session.getAcknowledgeMode());
            assertNotNull(xaSession.getXAResource());
            assertNotNull(session.createSender(session.createQueue("queue")));
            assertNotNull(session.createReceiver(session.createQueue("queue")));
            assertNotNull(session.createBrowser(session.createQueue("queue")));
            assertThrows(TransactionInProgressException.class, session::commit);
            assertThrows(TransactionInProgressException.class, session::rollback);

            JmsTopic topic = new JmsTopic("topic");
            assertThrows(IllegalStateException.class, () -> session.createTopic("topic"));
            assertThrows(IllegalStateException.class, session::createTemporaryTopic);
            assertThrows(IllegalStateException.class, () -> session.createProducer(topic));
            assertThrows(IllegalStateException.class, () -> session.createConsumer(topic));
            assertThrows(IllegalStateException.class, () -> session.createConsumer(topic, null));
            assertThrows(IllegalStateException.class, () -> session.createConsumer(topic, null, false));
            assertThrows(IllegalStateException.class, () -> session.createDurableSubscriber(topic, "subscription"));
            assertThrows(IllegalStateException.class, () -> session.createDurableConsumer(topic, "subscription"));
            assertThrows(IllegalStateException.class, () -> session.createSharedConsumer(topic, "subscription"));
            assertThrows(IllegalStateException.class, () -> session.createSharedDurableConsumer(topic, "subscription"));
            assertThrows(IllegalStateException.class, () -> session.unsubscribe("subscription"));

            try (XASession generic = connection.createXASession()) {
                assertThrows(IllegalStateException.class, () -> generic.getSession().createTopic("topic"));
            }
            xaSession.close();
            assertThrows(IllegalStateException.class, xaSession::getQueueSession);
            assertThrows(IllegalStateException.class, xaSession::getSession);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testTopicFactoryAndSession(boolean explicitCredentials) throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory("default-user", "default-password", "mock://localhost");
        ExceptionListener listener = exception -> {};
        factory.setExceptionListener(listener);
        XATopicConnectionFactory topicFactory = factory;
        try (XATopicConnection connection = explicitCredentials
                ? topicFactory.createXATopicConnection("user", "password") : topicFactory.createXATopicConnection()) {
            assertEquals(explicitCredentials ? "user" : "default-user", ((JmsConnection) connection).getUsername());
            assertEquals(explicitCredentials ? "password" : "default-password", ((JmsConnection) connection).getPassword());
            assertSame(listener, connection.getExceptionListener());
            assertThrows(IllegalStateException.class, () -> ((JmsConnection) connection).createQueueSession(false, Session.AUTO_ACKNOWLEDGE));
            connection.start();

            XATopicSession xaSession = connection.createXATopicSession();
            TopicSession session = xaSession.getTopicSession();
            assertSame(session, xaSession.getSession());
            assertTrue(((JmsSession) session).isStarted());
            assertTrue(session.getTransacted());
            assertEquals(Session.SESSION_TRANSACTED, session.getAcknowledgeMode());
            assertNotNull(xaSession.getXAResource());
            assertNotNull(session.createPublisher(session.createTopic("topic")));
            assertNotNull(session.createSubscriber(session.createTopic("topic")));
            assertThrows(TransactionInProgressException.class, session::commit);
            assertThrows(TransactionInProgressException.class, session::rollback);

            JmsQueue queue = new JmsQueue("queue");
            assertThrows(IllegalStateException.class, () -> session.createQueue("queue"));
            assertThrows(IllegalStateException.class, session::createTemporaryQueue);
            assertThrows(IllegalStateException.class, () -> session.createProducer(queue));
            assertThrows(IllegalStateException.class, () -> session.createConsumer(queue));
            assertThrows(IllegalStateException.class, () -> session.createConsumer(queue, null));
            assertThrows(IllegalStateException.class, () -> session.createConsumer(queue, null, false));
            assertThrows(IllegalStateException.class, () -> session.createBrowser(queue));
            assertThrows(IllegalStateException.class, () -> session.createBrowser(queue, null));

            try (XASession generic = connection.createXASession()) {
                assertThrows(IllegalStateException.class, () -> generic.getSession().createQueue("queue"));
            }
            xaSession.close();
            assertThrows(IllegalStateException.class, xaSession::getTopicSession);
            assertThrows(IllegalStateException.class, xaSession::getSession);
        }
    }
}
