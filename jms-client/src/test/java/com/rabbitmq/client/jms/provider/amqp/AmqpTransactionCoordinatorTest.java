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

import static org.junit.jupiter.api.Assertions.assertEquals;

import javax.transaction.xa.XAException;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.junit.jupiter.api.Test;

/**
 * How the error conditions of the broker map to XA return codes.
 */
public class AmqpTransactionCoordinatorTest {

    private static int code(String condition) {
        return AmqpTransactionCoordinator.toXaException(
            new ErrorCondition(Symbol.valueOf(condition), "description")).getErrorCode();
    }

    @Test
    public void mapsHeuristicOutcomes() {
        assertEquals(XAException.XA_HEURRB, code("rabbitmq:xa:heuristic-rollback"));
        assertEquals(XAException.XA_HEURCOM, code("rabbitmq:xa:heuristic-commit"));
    }

    @Test
    public void mapsUnknownConditionsToResourceManagerError() {
        assertEquals(XAException.XAER_RMERR, code("rabbitmq:xa:something-else"));
    }
}
