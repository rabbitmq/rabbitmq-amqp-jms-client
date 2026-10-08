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
package com.rabbitmq.client.jms.integration;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.TransactionInProgressException;
import jakarta.jms.XASession;

import com.rabbitmq.client.jms.JmsConnection;
import com.rabbitmq.client.jms.JmsConnectionFactory;
import com.rabbitmq.client.jms.JmsDefaultConnectionListener;
import com.rabbitmq.client.jms.JmsXAConnection;
import com.rabbitmq.client.jms.message.JmsInboundMessageDispatch;
import com.rabbitmq.client.jms.test.QpidJmsTestCase;
import com.rabbitmq.client.jms.test.testpeer.ListDescribedType;
import com.rabbitmq.client.jms.test.testpeer.TestAmqpPeer;
import com.rabbitmq.client.jms.test.testpeer.describedtypes.Accepted;
import com.rabbitmq.client.jms.test.testpeer.describedtypes.Declared;
import com.rabbitmq.client.jms.test.testpeer.describedtypes.Released;
import com.rabbitmq.client.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import com.rabbitmq.client.jms.test.testpeer.matchers.AcceptedMatcher;
import com.rabbitmq.client.jms.test.testpeer.matchers.CoordinatorMatcher;
import com.rabbitmq.client.jms.test.testpeer.matchers.TransactionalStateMatcher;
import com.rabbitmq.client.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import com.rabbitmq.client.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import com.rabbitmq.client.jms.test.testpeer.matchers.sections.MessagePropertiesSectionMatcher;
import com.rabbitmq.client.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import com.rabbitmq.client.jms.test.testpeer.matchers.types.EncodedAmqpValueMatcher;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.transaction.TxnCapability;
import org.apache.qpid.proton.codec.Data;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests for the XA transactions of the RabbitMQ AMQP 1.0 extension.
 */
public class XaTransactionsIntegrationTest extends QpidJmsTestCase {

    private static final Symbol XA_CAPABILITY = Symbol.valueOf("rabbitmq:xa");
    private static final UnsignedLong DECLARE = UnsignedLong.valueOf(0x31L);
    private static final Symbol XID = Symbol.valueOf("rabbitmq:xid");
    private static final Symbol XA_PREPARE = Symbol.valueOf("rabbitmq:xa-prepare");
    private static final Symbol XA_COMMIT = Symbol.valueOf("rabbitmq:xa-commit");
    private static final Symbol XA_RECOVER = Symbol.valueOf("rabbitmq:xa-recover");

    private static final Binary TXN_ID_1 = new Binary(new byte[] { 1, 2, 3, 4 });
    private static final Binary TXN_ID_2 = new Binary(new byte[] { 5, 6, 7, 8 });

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test
    @Timeout(20)
    public void testSendsInBranchLostOnFailoverAreDropped() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer()) {

            Xid xid = new TestXid(1);

            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            expectXaCoordinatorAttach(originalPeer);
            expectXaDeclare(originalPeer, xid, TXN_ID_1);
            originalPeer.expectSenderAttach();
            TransactionalStateMatcher inBranch = new TransactionalStateMatcher().withTxnId(equalTo(TXN_ID_1));
            originalPeer.expectTransfer(textMessage("lost"), inBranch, false, false, null, false);
            originalPeer.dropAfterLastHandler();

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectSenderAttach();

            JmsXAConnection connection = createFailoverXAConnection("jms.forceSyncSend=true", originalPeer, finalPeer);
            XASession session = connection.createXASession();
            XAResource resource = session.getXAResource();

            resource.start(xid, XAResource.TMNOFLAGS);
            MessageProducer producer = session.createProducer(session.createQueue("myQueue"));

            // Returns once the send has been replayed on the final peer.
            producer.send(session.createTextMessage("lost"));
            producer.send(session.createTextMessage("dropped"));

            resource.end(xid, XAResource.TMSUCCESS);
            assertXaError(XAException.XA_RBROLLBACK, () -> resource.prepare(xid));

            finalPeer.expectTransfer(textMessage("outside"));
            producer.send(session.createTextMessage("outside"));

            finalPeer.expectClose();
            connection.close();

            originalPeer.waitForAllHandlersToComplete(2000);
            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testAcknowledgementsInBranchLostOnFailoverAreDropped() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer()) {

            Xid xid = new TestXid(1);

            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            expectXaCoordinatorAttach(originalPeer);
            expectXaDeclare(originalPeer, xid, TXN_ID_1);
            originalPeer.dropAfterLastHandler();

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();

            JmsXAConnection connection = createFailoverXAConnection(null, originalPeer, finalPeer);
            CountDownLatch restored = awaitRestored(connection);
            connection.start();
            XASession session = connection.createXASession();
            XAResource resource = session.getXAResource();

            resource.start(xid, XAResource.TMNOFLAGS);
            assertTrue(restored.await(5, TimeUnit.SECONDS), "Should reconnect to the final peer");

            finalPeer.expectReceiverAttach();
            finalPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), 1);
            MessageConsumer consumer = session.createConsumer(session.createQueue("myQueue"));
            assertNotNull(consumer.receive(3000));

            resource.rollback(xid);

            finalPeer.expectSenderAttach();
            finalPeer.expectTransfer(textMessage("outside"));
            MessageProducer producer = session.createProducer(session.createQueue("myQueue"));
            producer.send(session.createTextMessage("outside"));

            finalPeer.expectClose();
            connection.close();

            originalPeer.waitForAllHandlersToComplete(2000);
            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testOnlyPreparedBranchesSurviveFailover() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer()) {

            Xid prepared = new TestXid(1);
            Xid lost = new TestXid(2);

            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            expectXaCoordinatorAttach(originalPeer);
            expectXaDeclare(originalPeer, prepared, TXN_ID_1);
            expectXaControl(originalPeer, XA_PREPARE, TXN_ID_1, new Accepted());
            expectXaDeclare(originalPeer, lost, TXN_ID_2);
            originalPeer.dropAfterLastHandler();

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();

            JmsXAConnection connection = createFailoverXAConnection(null, originalPeer, finalPeer);
            CountDownLatch restored = awaitRestored(connection);
            XASession session = connection.createXASession();
            XAResource resource = session.getXAResource();

            resource.start(prepared, XAResource.TMNOFLAGS);
            resource.end(prepared, XAResource.TMSUCCESS);
            assertEquals(XAResource.XA_OK, resource.prepare(prepared));
            resource.start(lost, XAResource.TMNOFLAGS);
            assertTrue(restored.await(5, TimeUnit.SECONDS), "Should reconnect to the final peer");

            resource.end(lost, XAResource.TMSUCCESS);
            resource.rollback(lost);
            assertXaError(XAException.XAER_NOTA, () -> resource.prepare(lost));

            expectXaCoordinatorAttach(finalPeer);
            expectXaControl(finalPeer, XA_COMMIT, xid(prepared), new Accepted());
            resource.commit(prepared, false);

            finalPeer.expectClose();
            connection.close();

            originalPeer.waitForAllHandlersToComplete(2000);
            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testSessionRollbackAndCommitFailWithoutSideEffects() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer()) {
            Xid xid = new TestXid(1);

            JmsXAConnection connection = createXAConnection(testPeer);
            CountDownLatch prefetched = new CountDownLatch(1);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onInboundMessage(JmsInboundMessageDispatch envelope) {
                    prefetched.countDown();
                }
            });
            connection.start();

            testPeer.expectBegin();
            XASession session = connection.createXASession();
            XAResource resource = session.getXAResource();

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), 1);
            MessageConsumer consumer = session.createConsumer(session.createQueue("myQueue"));
            assertTrue(prefetched.await(5, TimeUnit.SECONDS), "Should have prefetched the message");

            expectXaCoordinatorAttach(testPeer);
            expectXaDeclare(testPeer, xid, TXN_ID_1);
            resource.start(xid, XAResource.TMNOFLAGS);

            assertThrows(TransactionInProgressException.class, session::rollback);
            assertThrows(TransactionInProgressException.class, session::commit);

            TransactionalStateMatcher accepted = new TransactionalStateMatcher()
                .withTxnId(equalTo(TXN_ID_1)).withOutcome(new AcceptedMatcher());
            testPeer.expectDisposition(false, accepted);
            assertNotNull(consumer.receiveNoWait());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testBranchesCanBeCompletedAfterSessionClose() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer()) {
            Xid prepared = new TestXid(1);
            Xid toPrepare = new TestXid(2);
            Xid toCommit = new TestXid(3);
            Xid toRollback = new TestXid(4);

            JmsXAConnection connection = createXAConnection(testPeer);

            testPeer.expectBegin();
            XASession session = connection.createXASession();
            XAResource resource = session.getXAResource();

            expectXaCoordinatorAttach(testPeer);
            expectXaDeclare(testPeer, prepared, TXN_ID_1);
            expectXaControl(testPeer, XA_PREPARE, TXN_ID_1, new Accepted());
            resource.start(prepared, XAResource.TMNOFLAGS);
            resource.end(prepared, XAResource.TMSUCCESS);
            assertEquals(XAResource.XA_OK, resource.prepare(prepared));

            for (Xid xid : new Xid[] { toPrepare, toCommit, toRollback }) {
                expectXaDeclare(testPeer, xid, TXN_ID_2);
                resource.start(xid, XAResource.TMNOFLAGS);
                resource.end(xid, XAResource.TMSUCCESS);
            }

            testPeer.expectEnd();
            session.close();

            assertXaError(XAException.XA_RBROLLBACK, () -> resource.prepare(toPrepare));
            assertXaError(XAException.XA_RBROLLBACK, () -> resource.commit(toCommit, true));
            resource.rollback(toRollback);

            expectXaCoordinatorAttach(testPeer);
            expectXaControl(testPeer, XA_COMMIT, xid(prepared), new Accepted());
            resource.commit(prepared, false);

            testPeer.expectClose();
            connection.close();

            assertXaError(XAException.XAER_RMFAIL, () -> resource.recover(XAResource.TMSTARTRSCAN));

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testEndOfSuspendedBranch() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer()) {
            Xid succeeded = new TestXid(1);
            Xid failed = new TestXid(2);

            JmsXAConnection connection = createXAConnection(testPeer);

            testPeer.expectBegin();
            XASession session = connection.createXASession();
            XAResource resource = session.getXAResource();

            expectXaCoordinatorAttach(testPeer);
            expectXaDeclare(testPeer, succeeded, TXN_ID_1);
            resource.start(succeeded, XAResource.TMNOFLAGS);
            resource.end(succeeded, XAResource.TMSUSPEND);
            assertXaError(XAException.XAER_PROTO, () -> resource.end(succeeded, XAResource.TMSUSPEND));
            resource.end(succeeded, XAResource.TMSUCCESS);

            expectXaControl(testPeer, XA_PREPARE, TXN_ID_1, new Accepted());
            assertEquals(XAResource.XA_OK, resource.prepare(succeeded));

            expectXaDeclare(testPeer, failed, TXN_ID_2);
            resource.start(failed, XAResource.TMNOFLAGS);
            resource.end(failed, XAResource.TMSUSPEND);
            resource.end(failed, XAResource.TMFAIL);

            testPeer.expectDischarge(TXN_ID_2, true);
            assertXaError(XAException.XA_RBROLLBACK, () -> resource.prepare(failed));

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testUnexpectedRepliesFailXaRequests() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer()) {
            Xid notPrepared = new TestXid(1);
            Xid prepared = new TestXid(2);

            JmsXAConnection connection = createXAConnection(testPeer);

            testPeer.expectBegin();
            XASession session = connection.createXASession();
            XAResource resource = session.getXAResource();

            expectXaCoordinatorAttach(testPeer);
            expectXaDeclare(testPeer, notPrepared, TXN_ID_1);
            expectXaControl(testPeer, XA_PREPARE, TXN_ID_1, new Released());
            resource.start(notPrepared, XAResource.TMNOFLAGS);
            resource.end(notPrepared, XAResource.TMSUCCESS);
            assertXaError(XAException.XAER_RMERR, () -> resource.prepare(notPrepared));

            expectXaDeclare(testPeer, prepared, TXN_ID_2);
            expectXaControl(testPeer, XA_PREPARE, TXN_ID_2, new Accepted());
            expectXaControl(testPeer, XA_COMMIT, xid(prepared), new Released());
            resource.start(prepared, XAResource.TMNOFLAGS);
            resource.end(prepared, XAResource.TMSUCCESS);
            resource.prepare(prepared);
            assertXaError(XAException.XA_RETRY, () -> resource.commit(prepared, false));

            testPeer.expectTransfer(new ControlMatcher(XA_RECOVER), nullValue(), new Accepted(), true);
            assertXaError(XAException.XAER_RMERR, () -> resource.recover(XAResource.TMSTARTRSCAN));

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testXaRequestsFailWhileOffline() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer()) {
            Xid xid = new TestXid(1);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();
            testPeer.expectBegin();
            expectXaCoordinatorAttach(testPeer);
            expectXaDeclare(testPeer, xid, TXN_ID_1);
            expectXaControl(testPeer, XA_PREPARE, TXN_ID_1, new Accepted());
            testPeer.dropAfterLastHandler();

            JmsXAConnection connection = createFailoverXAConnection("failover.initialReconnectDelay=60000", testPeer);
            CountDownLatch interrupted = new CountDownLatch(1);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionInterrupted(URI remoteURI) {
                    interrupted.countDown();
                }
            });
            XASession session = connection.createXASession();
            XAResource resource = session.getXAResource();

            resource.start(xid, XAResource.TMNOFLAGS);
            resource.end(xid, XAResource.TMSUCCESS);
            resource.prepare(xid);
            assertTrue(interrupted.await(5, TimeUnit.SECONDS), "Should lose the connection");

            assertXaError(XAException.XAER_RMFAIL, () -> resource.commit(xid, false));
            assertXaError(XAException.XAER_RMFAIL, () -> resource.rollback(xid));
            assertXaError(XAException.XAER_RMFAIL, () -> resource.forget(xid));
            assertXaError(XAException.XAER_RMFAIL, () -> resource.recover(XAResource.TMSTARTRSCAN));

            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testXaRequestsTimeOutWhenRequestTimeoutIsInfinite() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer()) {
            Xid late = new TestXid(1);
            Xid notPrepared = new TestXid(2);
            Xid prepared = new TestXid(3);

            JmsXAConnection connection = createXAConnection(testPeer, "jms.xaRequestTimeout=500");

            testPeer.expectBegin();
            XASession session = connection.createXASession();
            XAResource resource = session.getXAResource();

            expectXaCoordinatorAttach(testPeer);
            testPeer.expectTransfer(new ControlMatcher(DECLARE, xid(late)), nullValue(), false, true,
                new Declared().setTxnId(TXN_ID_1), true, 0, 700);
            assertXaError(XAException.XAER_RMFAIL, () -> resource.start(late, XAResource.TMNOFLAGS));

            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(session.createQueue("myQueue"));
            testPeer.expectTransfer(textMessage("outside"));
            producer.send(session.createTextMessage("outside"));

            expectXaDeclare(testPeer, notPrepared, TXN_ID_2);
            resource.start(notPrepared, XAResource.TMNOFLAGS);
            resource.end(notPrepared, XAResource.TMSUCCESS);
            testPeer.expectTransfer(new ControlMatcher(XA_PREPARE, TXN_ID_2), nullValue(), false, false, null, false);
            assertXaError(XAException.XAER_RMFAIL, () -> resource.prepare(notPrepared));

            Binary txnId3 = new Binary(new byte[] { 9 });
            expectXaDeclare(testPeer, prepared, txnId3);
            expectXaControl(testPeer, XA_PREPARE, txnId3, new Accepted());
            resource.start(prepared, XAResource.TMNOFLAGS);
            resource.end(prepared, XAResource.TMSUCCESS);
            resource.prepare(prepared);
            testPeer.expectTransfer(new ControlMatcher(XA_COMMIT, xid(prepared)), nullValue(), false, false, null, false);
            assertXaError(XAException.XA_RETRY, () -> resource.commit(prepared, false));

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    //----- Test support -----------------------------------------------------//

    private interface XaCall {
        void run() throws XAException;
    }

    private static void assertXaError(int errorCode, XaCall call) {
        XAException failure = assertThrows(XAException.class, call::run);
        assertEquals(errorCode, failure.errorCode);
    }

    private JmsXAConnection createXAConnection(TestAmqpPeer peer) throws Exception {
        return createXAConnection(peer, null);
    }

    private JmsXAConnection createXAConnection(TestAmqpPeer peer, String options) throws Exception {
        peer.expectSaslPlain("guest", "guest");
        peer.expectOpen();
        peer.expectBegin();

        String uri = testFixture.buildURI(peer, false, options);
        JmsXAConnection connection = (JmsXAConnection) new JmsConnectionFactory(uri).createXAConnection("guest", "guest");
        connection.setClientID("clientName");
        return connection;
    }

    private JmsXAConnection createFailoverXAConnection(String options, TestAmqpPeer... peers) throws Exception {
        List<String> uris = new ArrayList<>();
        for (TestAmqpPeer peer : peers) {
            uris.add("amqp://localhost:" + peer.getServerPort());
        }
        String uri = "failover:(" + String.join(",", uris) + ")?failover.maxReconnectAttempts=10"
            + (options == null ? "" : "&" + options);
        return (JmsXAConnection) new JmsConnectionFactory(uri).createXAConnection();
    }

    private static CountDownLatch awaitRestored(JmsConnection connection) {
        CountDownLatch restored = new CountDownLatch(1);
        connection.addConnectionListener(new JmsDefaultConnectionListener() {
            @Override
            public void onConnectionRestored(URI remoteURI) {
                restored.countDown();
            }
        });
        return restored;
    }

    private static void expectXaCoordinatorAttach(TestAmqpPeer peer) {
        CoordinatorMatcher coordinator = new CoordinatorMatcher()
            .withCapabilities(arrayContaining(TxnCapability.LOCAL_TXN, XA_CAPABILITY));
        peer.expectSenderAttach(coordinator, false, false);
    }

    private static void expectXaDeclare(TestAmqpPeer peer, Xid xid, Binary txnId) {
        peer.expectTransfer(new ControlMatcher(DECLARE, xid(xid)), nullValue(), new Declared().setTxnId(txnId), true);
    }

    private static void expectXaControl(TestAmqpPeer peer, Object descriptor, Object field, ListDescribedType reply) {
        peer.expectTransfer(new ControlMatcher(descriptor, field), nullValue(), reply, true);
    }

    private static List<Object> xid(Xid xid) {
        return Arrays.asList(XID, Arrays.asList(xid.getFormatId(),
            new Binary(xid.getGlobalTransactionId()), new Binary(xid.getBranchQualifier())));
    }

    private static TransferPayloadCompositeMatcher textMessage(String text) {
        TransferPayloadCompositeMatcher matcher = new TransferPayloadCompositeMatcher();
        matcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
        matcher.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));
        matcher.setPropertiesMatcher(new MessagePropertiesSectionMatcher(true));
        matcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
        return matcher;
    }

    /**
     * Matches the amqp-value body of a message on the coordinator link, with the described
     * types it contains written as lists of descriptor and fields.
     */
    private static final class ControlMatcher extends TypeSafeMatcher<Binary> {

        private final List<Object> expected;

        private ControlMatcher(Object descriptor, Object... fields) {
            this.expected = Arrays.asList(descriptor, Arrays.asList(fields));
        }

        @Override
        protected boolean matchesSafely(Binary payload) {
            Data data = Data.Factory.create();
            data.decode(payload.asByteBuffer());
            return expected.equals(normalize(data.getDescribedType().getDescribed()));
        }

        private static Object normalize(Object value) {
            if (value instanceof DescribedType) {
                DescribedType described = (DescribedType) value;
                return Arrays.asList(described.getDescriptor(), normalize(described.getDescribed()));
            }
            if (value instanceof List) {
                List<Object> result = new ArrayList<>();
                for (Object element : (List<?>) value) {
                    result.add(normalize(element));
                }
                return result;
            }
            return value;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("a control message ").appendValue(expected);
        }

        @Override
        protected void describeMismatchSafely(Binary payload, Description description) {
            Data data = Data.Factory.create();
            data.decode(payload.asByteBuffer());
            description.appendText("was ").appendValue(normalize(data.getDescribedType().getDescribed()));
        }
    }

    private static final class TestXid implements Xid {

        private final byte[] globalTransactionId;
        private final byte[] branchQualifier;

        private TestXid(int id) {
            this.globalTransactionId = new byte[] { (byte) id };
            this.branchQualifier = new byte[] { (byte) id, 0 };
        }

        @Override
        public int getFormatId() {
            return 1;
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
}
