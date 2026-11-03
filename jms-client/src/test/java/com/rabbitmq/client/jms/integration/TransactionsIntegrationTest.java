/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.rabbitmq.client.jms.integration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.TransactionRolledBackException;

import com.rabbitmq.client.jms.JmsConnection;
import com.rabbitmq.client.jms.JmsDefaultConnectionListener;
import com.rabbitmq.client.jms.JmsOperationTimedOutException;
import com.rabbitmq.client.jms.JmsSession;
import com.rabbitmq.client.jms.JmsTransactionListener;
import com.rabbitmq.client.jms.exceptions.JmsConnectionFailedException;
import com.rabbitmq.client.jms.message.JmsInboundMessageDispatch;
import com.rabbitmq.client.jms.policy.JmsDefaultPrefetchPolicy;
import com.rabbitmq.client.jms.test.QpidJmsTestCase;
import com.rabbitmq.client.jms.test.testpeer.TestAmqpPeer;
import com.rabbitmq.client.jms.test.testpeer.basictypes.AmqpError;
import com.rabbitmq.client.jms.test.testpeer.describedtypes.Accepted;
import com.rabbitmq.client.jms.test.testpeer.basictypes.TransactionError;
import com.rabbitmq.client.jms.test.testpeer.describedtypes.Error;
import com.rabbitmq.client.jms.test.testpeer.describedtypes.Modified;
import com.rabbitmq.client.jms.test.testpeer.describedtypes.Rejected;
import com.rabbitmq.client.jms.test.testpeer.describedtypes.Released;
import com.rabbitmq.client.jms.test.testpeer.describedtypes.TransactionalState;
import com.rabbitmq.client.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import com.rabbitmq.client.jms.test.testpeer.matchers.AcceptedMatcher;
import com.rabbitmq.client.jms.test.testpeer.matchers.ErrorMatcher;
import com.rabbitmq.client.jms.test.testpeer.matchers.ModifiedMatcher;
import com.rabbitmq.client.jms.test.testpeer.matchers.ReleasedMatcher;
import com.rabbitmq.client.jms.test.testpeer.matchers.SourceMatcher;
import com.rabbitmq.client.jms.test.testpeer.matchers.TransactionalStateMatcher;
import com.rabbitmq.client.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import com.rabbitmq.client.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import com.rabbitmq.client.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for behavior of Transacted Session operations.
 */
public class TransactionsIntegrationTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionsIntegrationTest.class);

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test
    @Timeout(20)
    public void testTransactionRolledBackOnSessionClose() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);
            expectTransactionalTransfer(testPeer, txnId);

            producer.send(session.createMessage());

            // Closed session should roll-back the TX with a failed discharge
            testPeer.expectDischarge(txnId, true);
            testPeer.expectEnd();

            session.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testTransactionCommitFailWithEmptyRejectedDisposition() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            // Create a producer to use in provoking creation of the AMQP transaction
            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            // Expect the message which was sent under the current transaction. Check it carries
            // TransactionalState with the above txnId but has no outcome. Respond with a
            // TransactionalState with Accepted outcome.
            Binary txnId1 = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId1);
            expectTransactionalTransfer(testPeer, txnId1);

            producer.send(session.createMessage());

            // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
            // and reply with rejected and settled disposition to indicate the commit failed
            testPeer.expectDischarge(txnId1, false, new Rejected());

            try {
                session.commit();
                fail("Commit operation should have failed.");
            } catch (TransactionRolledBackException jmsTxRb) {
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testTransactionCommitFailWithRollbackError() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            JmsTransactionListener listener = Mockito.mock(JmsTransactionListener.class);
            ((JmsSession) session).getTransactionContext().setListener(listener);

            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            Binary txnId1 = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId1);
            expectTransactionalTransfer(testPeer, txnId1);

            producer.send(session.createMessage());

            // Reply to the 'discharge' with a rejected outcome carrying the rollback error.
            Error rejectError = new Error();
            rejectError.setCondition(TransactionError.TRANSACTION_ROLLBACK);
            rejectError.setDescription("queue 'myQueue' does not support transactions");
            testPeer.expectDischarge(txnId1, false, new Rejected().setError(rejectError));

            try {
                session.commit();
                fail("Commit operation should have failed.");
            } catch (TransactionRolledBackException jmsTxRb) {
                assertTrue(jmsTxRb.getMessage().contains("does not support transactions"));
            }
            Mockito.verify(listener).onTransactionRolledBack();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testProducedMessagesAfterCommitOfSentMessagesFails() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            // Create a producer to use in provoking creation of the AMQP transaction
            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            // Expect the message which was sent under the current transaction. Check it carries
            // TransactionalState with the above txnId but has no outcome. Respond with a
            // TransactionalState with Accepted outcome.
            Binary txnId1 = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId1);
            expectTransactionalTransfer(testPeer, txnId1);

            producer.send(session.createMessage());

            // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
            // and reply with rejected and settled disposition to indicate the commit failed
            Rejected commitFailure = new Rejected(new Error(Symbol.valueOf("failed"), "Unknown error"));
            testPeer.expectDischarge(txnId1, false, commitFailure);

            try {
                session.commit();
                fail("Commit operation should have failed.");
            } catch (TransactionRolledBackException jmsTxRb) {
            }

            // The next send declares the next transaction.
            Binary txnId2 = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            testPeer.expectDeclare(txnId2);
            expectTransactionalTransfer(testPeer, txnId2);
            testPeer.expectDischarge(txnId2, true);

            producer.send(session.createMessage());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testProducedMessagesAfterRollbackSentMessagesFails() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Binary txnId1 = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            Binary txnId2 = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            // Create a producer to use in provoking creation of the AMQP transaction
            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            // Expect the message which was sent under the current transaction. Check it carries
            // TransactionalState with the above txnId but has no outcome. Respond with a
            // TransactionalState with Accepted outcome.
            testPeer.expectDeclare(txnId1);
            expectTransactionalTransfer(testPeer, txnId1);

            producer.send(session.createMessage());

            // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
            // and reply with rejected and settled disposition to indicate the rollback failed
            Rejected commitFailure = new Rejected(new Error(Symbol.valueOf("failed"), "Unknown error"));
            testPeer.expectDischarge(txnId1, true, commitFailure);

            try {
                session.rollback();
                fail("Rollback operation should have failed.");
            } catch (JMSException jmsex) {
            }

            // The next send declares the next transaction.
            testPeer.expectDeclare(txnId2);
            expectTransactionalTransfer(testPeer, txnId2);
            testPeer.expectDischarge(txnId2, true);

            producer.send(session.createMessage());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCommitTransactedSessionWithConsumerReceivingAllMessages() throws Exception {
        doCommitTransactedSessionWithConsumerTestImpl(1, 1, false, false);
    }

    @Test
    @Timeout(20)
    public void testCommitTransactedSessionWithConsumerReceivingAllMessagesAndCloseBefore() throws Exception {
        doCommitTransactedSessionWithConsumerTestImpl(1, 1, true, true);
    }

    @Test
    @Timeout(20)
    public void testCommitTransactedSessionWithConsumerReceivingAllMessagesAndCloseAfter() throws Exception {
        doCommitTransactedSessionWithConsumerTestImpl(1, 1, true, false);
    }

    @Test
    @Timeout(20)
    public void testCommitTransactedSessionWithConsumerReceivingSomeMessages() throws Exception {
        doCommitTransactedSessionWithConsumerTestImpl(5, 2, false, false);
    }

    @Test
    @Timeout(20)
    public void testCommitTransactedSessionWithConsumerReceivingSomeMessagesAndClosesBefore() throws Exception {
        doCommitTransactedSessionWithConsumerTestImpl(5, 2, true, true);
    }

    @Test
    @Timeout(20)
    public void testCommitTransactedSessionWithConsumerReceivingSomeMessagesAndClosesAfter() throws Exception {
        doCommitTransactedSessionWithConsumerTestImpl(5, 2, true, false);
    }

    private void doCommitTransactedSessionWithConsumerTestImpl(int transferCount, int consumeCount, boolean closeConsumer, boolean closeBeforeCommit) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final int DEFAULT_PREFETCH = 100;

            // Set to fixed known value to reduce breakage if defaults are changed.
            Connection connection = testFixture.establishConnecton(testPeer, "jms.prefetchPolicy.all=" + DEFAULT_PREFETCH);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), transferCount);

            // The first acknowledgement declares the transaction.
            Binary txnId = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            testPeer.expectDeclare(txnId);

            // Then expect an *unsettled* TransactionalState disposition for each message once received by
            // the consumer, because the message remains acquired by the consumer if the transaction rolls back.
            TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
            stateMatcher.withTxnId(equalTo(txnId));
            stateMatcher.withOutcome(new AcceptedMatcher());
            for (int i = 1; i <= consumeCount; i++) {
                testPeer.expectDisposition(false, stateMatcher);
            }

            final CountDownLatch expected = new CountDownLatch(transferCount);
            ((JmsConnection) connection).addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onInboundMessage(JmsInboundMessageDispatch envelope) {
                    expected.countDown();
                }
            });

            MessageConsumer messageConsumer = session.createConsumer(queue);

            for (int i = 1; i <= consumeCount; i++) {
                Message receivedMessage = messageConsumer.receive(3000);

                assertNotNull(receivedMessage);
                assertTrue(receivedMessage instanceof TextMessage);
            }

            // Ensure all the messages arrived so that the matching below is deterministic
            assertTrue(expected.await(10, TimeUnit.SECONDS), "Expected transfers didnt occur: " + expected.getCount());

            // Expect the consumer to close now
            if (closeConsumer && closeBeforeCommit) {

                // Expect the client to then drain off all credit from the link.
                testPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.valueOf(DEFAULT_PREFETCH - transferCount)));

                // Expect the messages that were not consumed to be released
                int unconsumed = transferCount - consumeCount;
                for (int i = 1; i <= unconsumed; i++) {
                    testPeer.expectDispositionThatIsReleasedAndSettled();
                }

                // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
                // and reply with accepted and settled disposition to indicate the commit succeeded
                testPeer.expectDischarge(txnId, false);

                // Once committed, expect the consumed messages to be settled.
                for (int i = 1; i <= consumeCount; i++) {
                    testPeer.expectDisposition(true, stateMatcher);
                }

                // Now the deferred close should be performed.
                testPeer.expectDetach(true, true, true);

                messageConsumer.close();
            } else {
                // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
                // and reply with accepted and settled disposition to indicate the commit succeeded
                testPeer.expectDischarge(txnId, false);

                // Once committed, expect the consumed messages to be settled.
                for (int i = 1; i <= consumeCount; i++) {
                    testPeer.expectDisposition(true, stateMatcher);
                }
            }

            session.commit();

            if (closeConsumer && !closeBeforeCommit) {
                testPeer.expectDetach(true, true, true);

                // Expect the messages that were not consumed to be released
                int unconsumed = transferCount - consumeCount;
                for (int i = 1; i <= unconsumed; i++) {
                    testPeer.expectDispositionThatIsReleasedAndSettled();
                }

                messageConsumer.close();
            }

            testPeer.expectClose();

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testConsumerWithNoMessageCanCloseBeforeCommit() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();
            testPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH)));
            testPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH)));
            testPeer.expectDetach(true, true, true);

            MessageConsumer messageConsumer = session.createConsumer(queue);
            assertNull(messageConsumer.receiveNoWait());

            messageConsumer.close();

            // The transaction has no work, so the commit sends nothing.
            session.commit();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testConsumerWithNoMessageCanCloseBeforeRollback() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();
            testPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH)));
            testPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH)));
            testPeer.expectDetach(true, true, true);

            MessageConsumer messageConsumer = session.createConsumer(queue);
            assertNull(messageConsumer.receiveNoWait());

            messageConsumer.close();

            // The transaction has no work, so the rollback sends nothing.
            session.rollback();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testProducedMessagesOnTransactedSessionCarryTxnId() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            // Create a producer to use in provoking creation of the AMQP transaction
            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            // The send declares the transaction.
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);

            // Expect the message which was sent under the current transaction. Check it carries
            // TransactionalState with the above txnId but has no outcome. Respond with a
            // TransactionalState with Accepted outcome.
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));

            TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
            stateMatcher.withTxnId(equalTo(txnId));
            stateMatcher.withOutcome(nullValue());

            TransactionalState txState = new TransactionalState();
            txState.setTxnId(txnId);
            txState.setOutcome(new Accepted());

            testPeer.expectTransfer(messageMatcher, stateMatcher, txState, true);
            testPeer.expectDischarge(txnId, true);

            producer.send(session.createMessage());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testProducedMessagesOnTransactedSessionCanBeReused() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            // Create a producer to use in provoking creation of the AMQP transaction
            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            // The first send declares the transaction.
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);

            // Expect the message which was sent under the current transaction. Check it carries
            // TransactionalState with the above txnId but has no outcome. Respond with a
            // TransactionalState with Accepted outcome.

            Message message = session.createMessage();

            for(int i = 0; i < 3; ++i) {
                TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
                messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
                messageMatcher.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));

                TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
                stateMatcher.withTxnId(equalTo(txnId));
                stateMatcher.withOutcome(nullValue());

                TransactionalState txState = new TransactionalState();
                txState.setTxnId(txnId);
                txState.setOutcome(new Accepted());

                testPeer.expectTransfer(messageMatcher, stateMatcher, txState, true);

                message.setIntProperty("sequence", i);

                producer.send(message);
            }

            // Expect rollback on close without a commit call.
            testPeer.expectDischarge(txnId, true);
            testPeer.expectClose();

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testRollbackTransactedSessionWithConsumerReceivingAllMessages() throws Exception {
        doRollbackTransactedSessionWithConsumerTestImpl(1, 1, false);
    }

    @Test
    @Timeout(20)
    public void testRollbackTransactedSessionWithConsumerReceivingAllMessagesThenCloses() throws Exception {
        doRollbackTransactedSessionWithConsumerTestImpl(1, 1, true);
    }

    @Test
    @Timeout(20)
    public void testRollbackTransactedSessionWithConsumerReceivingSomeMessages() throws Exception {
        doRollbackTransactedSessionWithConsumerTestImpl(5, 2, false);
    }

    @Test
    @Timeout(20)
    public void testRollbackTransactedSessionWithConsumerReceivingSomeMessagesThenCloses() throws Exception {
        doRollbackTransactedSessionWithConsumerTestImpl(5, 2, true);
    }

    private void doRollbackTransactedSessionWithConsumerTestImpl(int transferCount, int consumeCount, boolean closeConsumer) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Set to fixed known value to reduce breakage if defaults are changed.
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), transferCount);

            // The first acknowledgement declares the transaction.
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);

            for (int i = 1; i <= consumeCount; i++) {
                // Then expect an *unsettled* TransactionalState disposition for each message once received by
                // the consumer, because the message remains acquired by the consumer if the transaction rolls back.
                TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
                stateMatcher.withTxnId(equalTo(txnId));
                stateMatcher.withOutcome(new AcceptedMatcher());

                testPeer.expectDisposition(false, stateMatcher);
            }

            MessageConsumer messageConsumer = session.createConsumer(queue);

            for (int i = 1; i <= consumeCount; i++) {
                Message receivedMessage = messageConsumer.receive(3000);

                assertNotNull(receivedMessage);
                assertTrue(receivedMessage instanceof TextMessage);
            }

            // Expect the consumer to be 'stopped' prior to rollback by issuing a 'drain'
            testPeer.expectLinkFlow(true, true, greaterThan(UnsignedInteger.ZERO));

            if (closeConsumer) {

                // Expect the messages that were not consumed to be released
                int unconsumed = transferCount - consumeCount;
                for (int i = 1; i <= unconsumed; i++) {
                    testPeer.expectDispositionThatIsReleasedAndSettled();
                }

                // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
                // and reply with accepted and settled disposition to indicate the rollback succeeded
                testPeer.expectDischarge(txnId, true);

                // The consumed messages are still acquired by the closing consumer,
                // which gives them back.
                for (int i = 1; i <= consumeCount; i++) {
                    testPeer.expectDisposition(true, new ModifiedMatcher().withDeliveryFailed(equalTo(true)));
                }

                // Now the deferred close should be performed.
                testPeer.expectDetach(true, true, true);

                messageConsumer.close();
            } else {
                // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
                // and reply with accepted and settled disposition to indicate the rollback succeeded
                testPeer.expectDischarge(txnId, true);

                // Expect the messages that were not consumed to be released
                int unconsumed = transferCount - consumeCount;
                for (int i = 1; i <= unconsumed; i++) {
                    testPeer.expectDisposition(true, new ReleasedMatcher());
                }

                // Expect the consumer to be 'started' again as rollback completes
                testPeer.expectLinkFlow(false, false, greaterThan(UnsignedInteger.ZERO));
            }

            session.rollback();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    // AMQP 1.0 section 4.4.2: after a rollback "the deliveries will still be "live"
    // and will remain acquired by the controller".
    @Test
    @Timeout(20)
    public void testRollbackDeliversAcknowledgedMessagesAgain() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final int messageCount = 2;
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"),
                                                       messageCount, false, false, greaterThan(UnsignedInteger.ZERO), 1, false, true);

            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);
            TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
            stateMatcher.withTxnId(equalTo(txnId));
            stateMatcher.withOutcome(new AcceptedMatcher());
            for (int i = 1; i <= messageCount; i++) {
                testPeer.expectDisposition(false, stateMatcher);
            }

            MessageConsumer messageConsumer = session.createConsumer(queue);
            for (int i = 0; i < messageCount; i++) {
                Message message = messageConsumer.receive(3000);
                assertNotNull(message);
                assertEquals(i, message.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER));
                assertFalse(message.getJMSRedelivered());
            }

            // Nothing is released: the messages remain acquired by the consumer.
            testPeer.expectLinkFlow(true, true, greaterThan(UnsignedInteger.ZERO));
            testPeer.expectDischarge(txnId, true);
            testPeer.expectLinkFlow(false, false, greaterThan(UnsignedInteger.ZERO));

            session.rollback();

            // The messages are delivered again in the same order and acknowledged in the next transaction.
            Binary txnId2 = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            testPeer.expectDeclare(txnId2);
            TransactionalStateMatcher stateMatcher2 = new TransactionalStateMatcher();
            stateMatcher2.withTxnId(equalTo(txnId2));
            stateMatcher2.withOutcome(new AcceptedMatcher());
            for (int i = 1; i <= messageCount; i++) {
                testPeer.expectDisposition(false, stateMatcher2);
            }
            for (int i = 0; i < messageCount; i++) {
                Message message = messageConsumer.receive(3000);
                assertNotNull(message);
                assertEquals(i, message.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER));
                assertTrue(message.getJMSRedelivered());
            }

            testPeer.expectDischarge(txnId2, false);
            for (int i = 1; i <= messageCount; i++) {
                testPeer.expectDisposition(true, stateMatcher2);
            }

            session.commit();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    // A message that a rollback delivered again was delivered before, so a later
    // rollback gives it back as a failed delivery, even if the application did not
    // receive it again.
    @Test
    @Timeout(20)
    public void testSecondRollbackGivesBackRecoveredMessageAsFailed() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final int messageCount = 2;
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"),
                                                       messageCount, false, false, greaterThan(UnsignedInteger.ZERO), 1, false, true);

            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);
            TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
            stateMatcher.withTxnId(equalTo(txnId));
            stateMatcher.withOutcome(new AcceptedMatcher());
            for (int i = 1; i <= messageCount; i++) {
                testPeer.expectDisposition(false, stateMatcher);
            }

            MessageConsumer messageConsumer = session.createConsumer(queue);
            for (int i = 0; i < messageCount; i++) {
                assertNotNull(messageConsumer.receive(3000));
            }

            testPeer.expectLinkFlow(true, true, greaterThan(UnsignedInteger.ZERO));
            testPeer.expectDischarge(txnId, true);
            testPeer.expectLinkFlow(false, false, greaterThan(UnsignedInteger.ZERO));

            session.rollback();

            Binary txnId2 = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            testPeer.expectDeclare(txnId2);
            TransactionalStateMatcher stateMatcher2 = new TransactionalStateMatcher();
            stateMatcher2.withTxnId(equalTo(txnId2));
            stateMatcher2.withOutcome(new AcceptedMatcher());
            testPeer.expectDisposition(false, stateMatcher2);
            Message message = messageConsumer.receive(3000);
            assertNotNull(message);
            assertEquals(0, message.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER));

            testPeer.expectLinkFlow(true, true, greaterThan(UnsignedInteger.ZERO));
            testPeer.expectDischarge(txnId2, true);
            testPeer.expectDisposition(true, new ModifiedMatcher().withDeliveryFailed(equalTo(true)));
            testPeer.expectLinkFlow(false, false, greaterThan(UnsignedInteger.ZERO));

            session.rollback();

            testPeer.waitForAllHandlersToComplete(1000);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    // A commit that the remote peer rolled back leaves the deliveries acquired, as a rollback does.
    @Test
    @Timeout(20)
    public void testFailedCommitDeliversAcknowledgedMessagesAgain() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final int messageCount = 2;
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"),
                                                       messageCount, false, false, greaterThan(UnsignedInteger.ZERO), 1, false, true);

            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);
            TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
            stateMatcher.withTxnId(equalTo(txnId));
            stateMatcher.withOutcome(new AcceptedMatcher());
            for (int i = 1; i <= messageCount; i++) {
                testPeer.expectDisposition(false, stateMatcher);
            }

            MessageConsumer messageConsumer = session.createConsumer(queue);
            for (int i = 0; i < messageCount; i++) {
                Message message = messageConsumer.receive(3000);
                assertNotNull(message);
                assertEquals(i, message.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER));
            }

            Error rejectError = new Error();
            rejectError.setCondition(TransactionError.TRANSACTION_ROLLBACK);
            testPeer.expectDischarge(txnId, false, new Rejected().setError(rejectError));

            try {
                session.commit();
                fail("Commit operation should have failed.");
            } catch (TransactionRolledBackException jmsTxRb) {
            }

            Binary txnId2 = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            testPeer.expectDeclare(txnId2);
            TransactionalStateMatcher stateMatcher2 = new TransactionalStateMatcher();
            stateMatcher2.withTxnId(equalTo(txnId2));
            stateMatcher2.withOutcome(new AcceptedMatcher());
            for (int i = 1; i <= messageCount; i++) {
                testPeer.expectDisposition(false, stateMatcher2);
            }
            for (int i = 0; i < messageCount; i++) {
                Message message = messageConsumer.receive(3000);
                assertNotNull(message);
                assertEquals(i, message.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER));
                assertTrue(message.getJMSRedelivered());
            }

            testPeer.expectDischarge(txnId2, false);
            for (int i = 1; i <= messageCount; i++) {
                testPeer.expectDisposition(true, stateMatcher2);
            }

            session.commit();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testRollbackTransactedSessionWithPrefetchFullBeforeStoppingConsumer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final int messageCount = 5;
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.prefetchPolicy.all=" + messageCount);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            // Create a consumer and fill the prefetch with messages, which we wont consume any of
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), messageCount);

            session.createConsumer(queue);

            // Create a producer
            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            // Expect the message, which declares the transaction
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher.setMessageAnnotationsMatcher( new MessageAnnotationsSectionMatcher(true));
            TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
            stateMatcher.withTxnId(equalTo(txnId));
            stateMatcher.withOutcome(nullValue());

            TransactionalState txState = new TransactionalState();
            txState.setTxnId(txnId);
            txState.setOutcome(new Accepted());

            testPeer.expectTransfer(messageMatcher, stateMatcher, txState, true);

            producer.send(session.createMessage());

            // The consumer will be 'stopped' prior to rollback, however we will NOT send a 'drain' Flow
            // frame as we have manipulated that all the credit was already used, i.e. it already stopped.

            // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
            // and reply with accepted and settled disposition to indicate the rollback succeeded
            testPeer.expectDischarge(txnId, true);

            session.rollback();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testRollbackTransactedSessionWithPrefetchFullyUtilisedByDrainWhenStoppingConsumer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final int messageCount = 5;
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.prefetchPolicy.all=" + messageCount);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            // Create a consumer, expect it to flow credit, but don't send it any messages
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(messageCount)));

            session.createConsumer(queue);

            // Create a producer to use in provoking creation of the AMQP transaction
            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            // Expect the message which provoked creating the transaction
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher.setMessageAnnotationsMatcher( new MessageAnnotationsSectionMatcher(true));

            TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
            stateMatcher.withTxnId(equalTo(txnId));
            stateMatcher.withOutcome(nullValue());

            TransactionalState txState = new TransactionalState();
            txState.setTxnId(txnId);
            txState.setOutcome(new Accepted());

            testPeer.expectTransfer(messageMatcher, stateMatcher, txState, true);

            producer.send(session.createMessage());

            // Expect the consumer to be 'stopped' prior to rollback by issuing a 'drain' Flow.
            // Action the drain by filling the prefetch (which is equivalent to this having happened while
            // the Flow was in flight to the peer), and then DONT send a flow frame back to the client
            // as it can tell from the messages that all the credit has been used.
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"),
                                                       messageCount, true, false, equalTo(UnsignedInteger.valueOf(messageCount)), 1, false);

            // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
            // and reply with accepted and settled disposition to indicate the rollback succeeded
            testPeer.expectDischarge(txnId, true);

            session.rollback();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testDefaultOutcomeIsModifiedForConsumerSourceOnTransactedSession() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            SourceMatcher sourceMatcher = new SourceMatcher();
            sourceMatcher.withAddress(equalTo(queueName));
            sourceMatcher.withDynamic(equalTo(false));
            sourceMatcher.withOutcomes(arrayContaining(Accepted.DESCRIPTOR_SYMBOL, Rejected.DESCRIPTOR_SYMBOL, Released.DESCRIPTOR_SYMBOL, Modified.DESCRIPTOR_SYMBOL));
            ModifiedMatcher outcomeMatcher = new ModifiedMatcher().withDeliveryFailed(equalTo(true)).withUndeliverableHere(nullValue());
            sourceMatcher.withDefaultOutcome(outcomeMatcher);

            testPeer.expectReceiverAttach(notNullValue(), sourceMatcher);
            testPeer.expectLinkFlow();
            session.createConsumer(queue);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCoordinatorLinkSupportedOutcomes() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            // Expect session, then coordinator link
            testPeer.expectBegin();

            SourceMatcher sourceMatcher = new SourceMatcher();
            sourceMatcher.withOutcomes(arrayContaining(Accepted.DESCRIPTOR_SYMBOL, Rejected.DESCRIPTOR_SYMBOL, Released.DESCRIPTOR_SYMBOL, Modified.DESCRIPTOR_SYMBOL));

            testPeer.expectCoordinatorAttach(sourceMatcher);

            connection.createSession(true, Session.SESSION_TRANSACTED);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testRollbackErrorCoordinatorClosedOnCommit() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);
            expectTransactionalTransfer(testPeer, txnId);

            producer.send(session.createMessage());

            testPeer.remotelyCloseLastCoordinatorLinkOnDischarge(txnId, false);
            // The next transaction gets a new coordinator link.
            testPeer.expectCoordinatorAttach();

            try {
                session.commit();
                fail("Transaction should have rolled back");
            } catch (TransactionRolledBackException ex) {
                LOG.info("Caught expected TransactionRolledBackException");
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testRollbackErrorWhenCoordinatorRemotelyClosed() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);
            expectTransactionalTransfer(testPeer, txnId);
            testPeer.remotelyCloseLastCoordinatorLink();

            producer.send(session.createMessage());

            testPeer.waitForAllHandlersToComplete(2000);

            // The next transaction gets a new coordinator link.
            testPeer.expectCoordinatorAttach();

            try {
                session.commit();
                fail("Transaction should have rolled back");
            } catch (TransactionRolledBackException ex) {
                LOG.info("Caught expected TransactionRolledBackException");
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testJMSErrorCoordinatorClosedOnRollback() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);
            expectTransactionalTransfer(testPeer, txnId);

            producer.send(session.createMessage());

            testPeer.remotelyCloseLastCoordinatorLinkOnDischarge(txnId, true);
            // The next transaction gets a new coordinator link.
            testPeer.expectCoordinatorAttach();

            try {
                session.rollback();
                fail("Transaction should have rolled back");
            } catch (JMSException ex) {
                LOG.info("Caught expected JMSException");
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testJMSExceptionOnRollbackWhenCoordinatorRemotelyClosed() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);
            expectTransactionalTransfer(testPeer, txnId);
            testPeer.remotelyCloseLastCoordinatorLink();

            producer.send(session.createMessage());

            testPeer.waitForAllHandlersToComplete(2000);

            // The next transaction gets a new coordinator link.
            testPeer.expectCoordinatorAttach();

            try {
                session.rollback();
                fail("Rollback should have thrown a JMSException");
            } catch (JMSException ex) {
                LOG.info("Caught expected JMSException");
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testSendAfterCoordinatorLinkClosedDuringTX() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);
            expectTransactionalTransfer(testPeer, txnId);

            // Close the link, the messages should now just get dropped on the floor.
            testPeer.remotelyCloseLastCoordinatorLink();

            producer.send(session.createMessage());

            testPeer.waitForAllHandlersToComplete(2000);

            producer.send(session.createMessage());

            // Expect that a new link will be created in order to start the next TX.
            testPeer.expectCoordinatorAttach();

            try {
                session.commit();
                fail("Commit operation should have failed.");
            } catch (TransactionRolledBackException jmsTxRb) {
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testReceiveAfterCoordinatorLinkClosedDuringTX() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            // Create a consumer and send it an initial message for receive to process.
            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);

            MessageConsumer consumer = session.createConsumer(queue);

            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);
            expectTransactionalTransfer(testPeer, txnId);

            // Close the link, the messages should now just get dropped on the floor.
            testPeer.remotelyCloseLastCoordinatorLink();

            producer.send(session.createMessage());

            testPeer.waitForAllHandlersToComplete(2000);

            // receiving the message would normally ack it, since the TX is failed this
            // should not result in a disposition going out.
            Message received = consumer.receive();
            assertNotNull(received);

            // Expect that a new link will be created in order to start the next TX.
            testPeer.expectCoordinatorAttach();

            try {
                session.commit();
                fail("Commit operation should have failed.");
            } catch (TransactionRolledBackException jmsTxRb) {
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCommitFailsWhenDeclareTimesOut() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setRequestTimeout(500);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            // The send waits for the declare, and is dropped once the declare times out.
            testPeer.expectDeclareButDoNotRespond();

            producer.send(session.createMessage());

            try {
                session.commit();
                fail("Commit operation should have failed.");
            } catch (TransactionRolledBackException jmsTxRb) {
                assertTrue(jmsTxRb.getMessage().contains("Timed out waiting for declare"), jmsTxRb.getMessage());
            }

            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);
            expectTransactionalTransfer(testPeer, txnId);
            testPeer.expectDischarge(txnId, false);

            producer.send(session.createMessage());
            session.commit();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testDeclaredOutcomeAfterTimeoutRollsBackAndCommitFails() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setRequestTimeout(500);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            Binary txnId1 = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            testPeer.expectDeclare(txnId1, 1000);
            testPeer.expectDischarge(txnId1, true);

            producer.send(session.createMessage());

            testPeer.waitForAllHandlersToComplete(3000);

            try {
                session.commit();
                fail("Commit operation should have failed.");
            } catch (TransactionRolledBackException jmsTxRb) {
            }

            Binary txnId2 = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId2);
            expectTransactionalTransfer(testPeer, txnId2);
            testPeer.expectDischarge(txnId2, false);

            producer.send(session.createMessage());
            session.commit();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCommitFailsWhenDeclareIsRejected() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), 1);
            MessageConsumer consumer = session.createConsumer(queue);

            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            // Neither the send nor the acknowledgement reaches the peer.
            testPeer.expectDeclareAndReject();

            producer.send(session.createMessage());

            Message message = consumer.receive(3000);
            assertNotNull(message);
            assertFalse(message.getJMSRedelivered());

            try {
                session.commit();
                fail("Commit operation should have failed.");
            } catch (TransactionRolledBackException jmsTxRb) {
            }

            // The acknowledged message is delivered again, and the next transaction is declared.
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);
            testPeer.expectDisposition(false, acceptedIn(txnId));

            message = consumer.receive(3000);
            assertNotNull(message);
            assertTrue(message.getJMSRedelivered());

            testPeer.expectDischarge(txnId, false);
            testPeer.expectDisposition(true, acceptedIn(txnId));

            session.commit();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testSessionCreateFailsOnCoordinatorLinkRefusal() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            // Expect coordinator link, refuse it, expect detach reply
            String errorMessage = "CoordinatorLinkRefusal-breadcrumb";
            testPeer.expectCoordinatorAttach(true, false, AmqpError.NOT_IMPLEMENTED, errorMessage);
            testPeer.expectDetach(true, false, false);

            // Expect the AMQP session to be closed due to the JMS session creation failure.
            testPeer.expectEnd();

            try {
                connection.createSession(true, Session.SESSION_TRANSACTED);
                fail("should have thrown");
            } catch (JMSException jmse) {
                assertNotNull(jmse.getMessage());
                assertTrue(jmse.getMessage().contains(errorMessage), "Expected exception message to contain breadcrumb");
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testTransactionRolledBackOnSessionCloseTimesOut() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setRequestTimeout(500);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);
            expectTransactionalTransfer(testPeer, txnId);

            producer.send(session.createMessage());

            // Closed session should roll-back the TX with a failed discharge
            testPeer.expectDischargeButDoNotRespond(txnId, true);

            try {
                session.close();
                fail("Should have timed out waiting for discharge.");
            } catch (JmsOperationTimedOutException jmsEx) {
            } catch (Throwable error) {
                fail("Should have caught an timed out exception:");
                LOG.error("Caught -> ", error);
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testTransactionRolledBackTimesOut() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setRequestTimeout(500);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);
            expectTransactionalTransfer(testPeer, txnId);

            producer.send(session.createMessage());

            // Expect discharge but don't respond so that the request timeout kicks in and fails
            // the discharge.
            testPeer.expectDischargeButDoNotRespond(txnId, true);

            try {
                session.rollback();
                fail("Should have timed out waiting for discharge.");
            } catch (JmsOperationTimedOutException jmsEx) {
            } catch (Throwable error) {
                fail("Should have caught an timed out exception:");
                LOG.error("Caught -> ", error);
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testTransactionCommitTimesOut() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setRequestTimeout(500);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);
            expectTransactionalTransfer(testPeer, txnId);

            producer.send(session.createMessage());

            // Expect discharge but don't respond so that the request timeout kicks in and fails
            // the discharge.
            testPeer.expectDischargeButDoNotRespond(txnId, false);

            try {
                session.commit();
                fail("Should have timed out waiting for discharge.");
            } catch (JmsOperationTimedOutException jmsEx) {
            } catch (Throwable error) {
                fail("Should have caught an timed out exception:");
                LOG.error("Caught -> ", error);
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testTransactionCommitTimesOutAndNextDeclareTimesOut() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setRequestTimeout(500);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            Binary txnId1 = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            Binary txnId2 = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            testPeer.expectDeclare(txnId1);
            expectTransactionalTransfer(testPeer, txnId1);

            producer.send(session.createMessage());

            testPeer.expectDischargeButDoNotRespond(txnId1, false);

            try {
                session.commit();
                fail("Should have timed out waiting for discharge.");
            } catch (JmsOperationTimedOutException jmsEx) {
            }

            // The declare of the next transaction times out too, which drops the send.
            testPeer.expectDeclareButDoNotRespond();

            producer.send(session.createMessage());

            try {
                session.commit();
                fail("Commit operation should have failed.");
            } catch (TransactionRolledBackException jmsTxRb) {
            }

            testPeer.expectDeclare(txnId2);
            expectTransactionalTransfer(testPeer, txnId2);
            testPeer.expectDischarge(txnId2, false);

            producer.send(session.createMessage());
            session.commit();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testRollbackWithNoResponseForSuspendConsumer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?amqp.drainTimeout=1000");
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), 1);

            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);

            // Then expect an *unsettled* TransactionalState disposition for the message once received by the consumer
            TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
            stateMatcher.withTxnId(equalTo(txnId));
            stateMatcher.withOutcome(new AcceptedMatcher());

            testPeer.expectDisposition(false, stateMatcher);

            // Read one so we try to suspend on rollback
            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);

            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof TextMessage);

            // Expect the consumer to be 'stopped' prior to rollback by issuing a 'drain'
            testPeer.expectLinkFlow(true, false, greaterThan(UnsignedInteger.ZERO));

            // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
            // and reply with accepted and settled disposition to indicate the rollback succeeded
            testPeer.expectDischarge(txnId, true);

            try {
                session.rollback();
                fail("Should throw a timed out exception");
            } catch (JmsOperationTimedOutException jmsEx) {}

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(30)
    public void testConsumerMessageOrderOnTransactedSession() throws IOException, Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            final int messageCount = 10;

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            // Expect the browser enumeration to create a underlying consumer
            testPeer.expectReceiverAttach();
            // Expect initial credit to be sent, respond with some messages that are tagged with
            // a sequence number we can use to determine if order is maintained.
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent,
                messageCount, false, false, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH)), 1, false, true);

            Binary txnId = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            testPeer.expectDeclare(txnId);

            for (int i = 1; i <= messageCount; i++) {
                // Then expect an *unsettled* TransactionalState disposition for each message once received by the consumer
                TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
                stateMatcher.withTxnId(equalTo(txnId));
                stateMatcher.withOutcome(new AcceptedMatcher());

                testPeer.expectDisposition(false, stateMatcher);
            }

            MessageConsumer consumer = session.createConsumer(queue);
            for (int i = 0; i < messageCount; ++i) {
                Message message = consumer.receive(500);
                assertNotNull(message);
                assertEquals(i, message.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER));
            }

            // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
            // and reply with accepted and settled disposition to indicate the rollback succeeded
            testPeer.expectDischarge(txnId, true);
            testPeer.expectEnd();

            session.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test
    @Timeout(60)
    public void testConsumeManyWithSingleTXPerMessage() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            final int messageCount = 10;

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Deque<Binary> txnIdDeque = new ArrayDeque<>(3);
            txnIdDeque.offer(new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4}));
            txnIdDeque.offer(new Binary(new byte[]{ (byte) 2, (byte) 4, (byte) 6, (byte) 8}));
            txnIdDeque.offer(new Binary(new byte[]{ (byte) 5, (byte) 4, (byte) 3, (byte) 2}));

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            // Expect the browser enumeration to create a underlying consumer
            testPeer.expectReceiverAttach();
            // Expect initial credit to be sent, respond with some messages that are tagged with
            // a sequence number we can use to determine if order is maintained.
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent,
                messageCount, false, false, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH)), 1, false, true);

            MessageConsumer consumer = session.createConsumer(queue);

            for (int i = 0; i < messageCount; i++) {
                // Each acknowledgement declares a new transaction.
                Binary txnId = txnIdDeque.removeFirst();
                txnIdDeque.addLast(txnId);
                testPeer.expectDeclare(txnId);

                // Then expect an *unsettled* TransactionalState disposition for each message once received by the consumer
                TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
                stateMatcher.withTxnId(equalTo(txnId));
                stateMatcher.withOutcome(new AcceptedMatcher());

                testPeer.expectDisposition(false, stateMatcher);

                Message message = consumer.receive(500);
                assertNotNull(message);
                assertEquals(i, message.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER));

                // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
                // and reply with accepted and settled disposition to indicate the commit succeeded
                testPeer.expectDischarge(txnId, false);

                // Once committed, expect the message to be settled.
                testPeer.expectDisposition(true, stateMatcher);

                session.commit();
            }

            testPeer.expectEnd();

            session.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test
    @Timeout(20)
    public void testTransactionDeclaredDispositionWithoutTxnId() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            AtomicReference<JMSException> failure = new AtomicReference<>();
            CountDownLatch exceptionListenerFired = new CountDownLatch(1);
            connection.setExceptionListener(jmse -> {
                failure.compareAndSet(null, jmse);
                exceptionListenerFired.countDown();
            });

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            // Expect declare, reply declared but without a txn-id, which is illegal.
            testPeer.expectDeclare(null);

            ErrorMatcher errorMatcher = new ErrorMatcher()
                    .withCondition(equalTo(AmqpError.DECODE_ERROR))
                    .withDescription(equalTo("The txn-id field cannot be omitted"));
            testPeer.expectClose(errorMatcher, false);
            testPeer.setSuppressReadExceptionOnClose(true);

            try {
                producer.send(session.createMessage());
                fail("expected exception to be thrown");
            } catch (JMSException e) {
                // Expected
            }

            assertTrue(exceptionListenerFired.await(3, TimeUnit.SECONDS), "The ExceptionListener should have been alerted");
            JMSException ex = failure.get();
            assertTrue(ex instanceof JmsConnectionFailedException, "Unexpected exception type: " + ex);

            assertThat("Unexpected exception type: ", ex.getMessage(),
                    equalTo("The JMS connection has failed: Error in proton Transport: The txn-id field cannot be omitted [condition = amqp:decode-error]"));

            testPeer.waitForAllHandlersToComplete(1000);

            connection.close(); // Already nuked under the covers due to txn-id being missing
        }
    }

    @Test
    @Timeout(30)
    public void testAsyncConsumerAcksAfterCommitAndBeginWhenCommitCalledInOnMessage() throws Exception {
        final Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});

        final TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
        messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
        messageMatcher.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));

        final TransactionalStateMatcher transferStateMatcher = new TransactionalStateMatcher();
        transferStateMatcher.withTxnId(equalTo(txnId));
        transferStateMatcher.withOutcome(nullValue());

        final TransactionalStateMatcher dispositionStateMatcher = new TransactionalStateMatcher();
        dispositionStateMatcher.withTxnId(equalTo(txnId));
        dispositionStateMatcher.withOutcome(new AcceptedMatcher());

        final TransactionalState transferTxnOutcome = new TransactionalState();
        transferTxnOutcome.setTxnId(txnId);
        transferTxnOutcome.setOutcome(new Accepted());

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?amqp.drainTimeout=1000");
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), 1);
            testPeer.expectDeclare(txnId);
            testPeer.expectDisposition(false, dispositionStateMatcher);
            testPeer.expectSenderAttach();
            testPeer.expectTransfer(messageMatcher, transferStateMatcher, transferTxnOutcome, true);
            testPeer.expectDischarge(txnId, false);
            testPeer.expectDisposition(true, dispositionStateMatcher);

            // Test that consumer onMessage delivery and a send within the listener are
            // both included into the same transaction prior to the commit in the listener.
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");
            MessageConsumer messageConsumer = session.createConsumer(queue);
            messageConsumer.setMessageListener((message) -> {
                try {
                    session.createProducer(queue).send(session.createTextMessage("sample"));
                    session.commit();
                } catch (JMSException jmsEx) {
                    throw new RuntimeException("Behaving badly since commit already did", jmsEx);
                }
            });

            testPeer.waitForAllHandlersToComplete(1000);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCommitAndRollbackWithoutWorkSendNothing() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            session.commit();
            session.rollback();
            session.commit();

            testPeer.waitForAllHandlersToComplete(1000);

            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);
            expectTransactionalTransfer(testPeer, txnId);
            testPeer.expectDischarge(txnId, false);

            producer.send(session.createMessage());
            session.commit();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testFirstAcknowledgementDeclaresTransaction() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), 1);

            MessageConsumer consumer = session.createConsumer(queue);

            testPeer.waitForAllHandlersToComplete(1000);

            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);
            testPeer.expectDisposition(false, acceptedIn(txnId));

            assertNotNull(consumer.receive(3000));

            testPeer.expectDischarge(txnId, false);
            testPeer.expectDisposition(true, acceptedIn(txnId));

            session.commit();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testFirstAcknowledgementWaitsForDeclare() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), 1);

            MessageConsumer consumer = session.createConsumer(queue);

            // The disposition goes out before the sender attach that follows the receive.
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId, 500);
            testPeer.expectDisposition(false, acceptedIn(txnId));
            testPeer.expectSenderAttach();
            expectTransactionalTransfer(testPeer, txnId);
            testPeer.expectDischarge(txnId, false);
            testPeer.expectDisposition(true, acceptedIn(txnId));

            assertNotNull(consumer.receive(3000));
            MessageProducer producer = session.createProducer(queue);
            producer.send(session.createMessage());
            session.commit();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testConsumerClosedBeforeDeclareReplyDefersDetachUntilCommit() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final int prefetch = 100;
            Connection connection = testFixture.establishConnecton(testPeer, "jms.prefetchPolicy.all=" + prefetch);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), 1);

            MessageConsumer consumer = session.createConsumer(queue);

            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId, 1000);
            testPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.valueOf(prefetch - 1)));
            testPeer.expectDisposition(false, acceptedIn(txnId));

            // The acknowledgement in receive() waits for the declare reply, while the
            // consumer is closed from another thread.
            AtomicReference<Message> received = new AtomicReference<>();
            Thread receiver = new Thread(() -> {
                try {
                    received.set(consumer.receive(3000));
                } catch (JMSException e) {
                    LOG.warn("Receive failed", e);
                }
            });
            receiver.start();
            Thread.sleep(300);

            consumer.close();
            receiver.join(5000);
            assertNotNull(received.get());

            testPeer.waitForAllHandlersToComplete(1000);

            testPeer.expectDischarge(txnId, false);
            testPeer.expectDisposition(true, acceptedIn(txnId));
            testPeer.expectDetach(true, true, true);

            session.commit();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testSendAfterCoordinatorLinkClosedWithoutWorkDeclaresOnNewLink() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();
            testPeer.remotelyCloseLastCoordinatorLink();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.waitForAllHandlersToComplete(2000);

            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            // The transaction has no work, so it is not in doubt.
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectCoordinatorAttach();
            testPeer.expectDeclare(txnId);
            expectTransactionalTransfer(testPeer, txnId);
            testPeer.expectDischarge(txnId, false);

            producer.send(session.createMessage());
            session.commit();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    private static void expectTransactionalTransfer(TestAmqpPeer testPeer, Binary txnId) {
        TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
        messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
        messageMatcher.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));

        TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
        stateMatcher.withTxnId(equalTo(txnId));
        stateMatcher.withOutcome(nullValue());

        TransactionalState txState = new TransactionalState();
        txState.setTxnId(txnId);
        txState.setOutcome(new Accepted());

        testPeer.expectTransfer(messageMatcher, stateMatcher, txState, true);
    }

    private static TransactionalStateMatcher acceptedIn(Binary txnId) {
        TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
        stateMatcher.withTxnId(equalTo(txnId));
        stateMatcher.withOutcome(new AcceptedMatcher());
        return stateMatcher;
    }
}
