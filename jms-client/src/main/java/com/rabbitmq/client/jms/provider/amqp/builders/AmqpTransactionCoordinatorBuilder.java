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
package com.rabbitmq.client.jms.provider.amqp.builders;

import com.rabbitmq.client.jms.meta.JmsSessionInfo;
import com.rabbitmq.client.jms.provider.amqp.AmqpTransactionContext;
import com.rabbitmq.client.jms.provider.amqp.AmqpTransactionCoordinator;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.amqp.transaction.TxnCapability;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Sender;

/**
 * Resource builder responsible for creating and opening an AmqpTransactionCoordinator instance.
 */
public class AmqpTransactionCoordinatorBuilder extends AmqpResourceBuilder<AmqpTransactionCoordinator, AmqpTransactionContext, JmsSessionInfo, Sender> {

    private static final Symbol XA_CAPABILITY = Symbol.valueOf("rabbitmq:xa");


    public AmqpTransactionCoordinatorBuilder(AmqpTransactionContext parent, JmsSessionInfo resourceInfo) {
        super(parent, resourceInfo);
    }

    @Override
    protected Sender createEndpoint(JmsSessionInfo resourceInfo) {
        Coordinator coordinator = new Coordinator();
        if (resourceInfo.isXa()) {
            coordinator.setCapabilities(TxnCapability.LOCAL_TXN, XA_CAPABILITY);
        } else {
            coordinator.setCapabilities(TxnCapability.LOCAL_TXN);
        }

        Symbol[] outcomes = new Symbol[]{ Accepted.DESCRIPTOR_SYMBOL, Rejected.DESCRIPTOR_SYMBOL, Released.DESCRIPTOR_SYMBOL, Modified.DESCRIPTOR_SYMBOL };

        Source source = new Source();
        source.setOutcomes(outcomes);

        String coordinatorName = "qpid-jms:coordinator:" + resourceInfo.getId().toString();

        Sender sender = getParent().getSession().getEndpoint().sender(coordinatorName);
        sender.setSource(source);
        sender.setTarget(coordinator);
        sender.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        sender.setReceiverSettleMode(ReceiverSettleMode.FIRST);

        return sender;
    }

    @Override
    protected AmqpTransactionCoordinator createResource(AmqpTransactionContext parent, JmsSessionInfo resourceInfo, Sender endpoint) {
        return new AmqpTransactionCoordinator(resourceInfo, endpoint, parent);
    }

    @Override
    protected boolean isOpenedEndpointValid() {
        if (!resourceInfo.isXa()) {
            return true;
        }
        Object remoteTarget = getEndpoint().getRemoteTarget();
        if (remoteTarget instanceof Coordinator) {
            Symbol[] capabilities = ((Coordinator) remoteTarget).getCapabilities();
            if (capabilities != null) {
                for (Symbol capability : capabilities) {
                    if (XA_CAPABILITY.equals(capability)) {
                        return true;
                    }
                }
            }
        }
        // The broker does not support XA.
        return false;
    }

    @Override
    protected boolean isClosePending() {
        // When no link terminus was created, the peer will now detach/close us otherwise
        // we need to validate the returned remote source prior to open completion.
        return getEndpoint().getRemoteTarget() == null;
    }
}
