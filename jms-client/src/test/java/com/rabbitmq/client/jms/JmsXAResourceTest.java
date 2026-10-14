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
import static org.junit.jupiter.api.Assertions.assertSame;

import javax.transaction.xa.XAException;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.jms.provider.exceptions.ProviderOperationTimedOutException;
import com.rabbitmq.client.jms.provider.exceptions.ProviderXaException;

/**
 * How the failures of the commit and the rollback of a prepared branch are reported.
 */
public class JmsXAResourceTest {

    private static XAException failure(int errorCode, Throwable cause) {
        XAException result = new XAException(errorCode);
        result.initCause(cause);
        return result;
    }

    @Test
    public void commitOfPreparedBranchKeepsFinalAnswers() {
        XAException unknown = failure(XAException.XAER_NOTA, null);
        XAException heuristicRollback = failure(XAException.XA_HEURRB, null);
        XAException heuristicCommit = failure(XAException.XA_HEURCOM, null);
        assertSame(unknown, JmsXAResource.preparedCommitFailure(unknown));
        assertSame(heuristicRollback, JmsXAResource.preparedCommitFailure(heuristicRollback));
        assertSame(heuristicCommit, JmsXAResource.preparedCommitFailure(heuristicCommit));
    }

    @Test
    public void commitOfPreparedBranchKeepsLostConnection() {
        XAException lost = failure(XAException.XAER_RMFAIL, new IllegalStateException("closed"));
        assertSame(lost, JmsXAResource.preparedCommitFailure(lost));
    }

    @Test
    public void commitOfPreparedBranchRetriesWhenReplyDoesNotArrive() {
        ProviderOperationTimedOutException timeout = new ProviderOperationTimedOutException("timeout");
        XAException result = JmsXAResource.preparedCommitFailure(failure(XAException.XAER_RMFAIL, timeout));
        assertEquals(XAException.XA_RETRY, result.errorCode);
        assertSame(timeout, result.getCause());
    }

    @Test
    public void commitOfPreparedBranchRetriesOtherFailures() {
        int[] codes = {
            XAException.XAER_RMERR, XAException.XAER_PROTO, XAException.XAER_INVAL,
            XAException.XA_RBROLLBACK, XAException.XAER_DUPID
        };
        for (int code : codes) {
            ProviderXaException cause = new ProviderXaException(code, "failure");
            assertEquals(XAException.XA_RETRY,
                JmsXAResource.preparedCommitFailure(failure(code, cause)).errorCode, "code " + code);
        }
    }

    @Test
    public void rollbackOfPreparedBranchKeepsFinalAnswers() {
        int[] codes = {
            XAException.XAER_NOTA, XAException.XA_HEURRB, XAException.XA_HEURCOM, XAException.XAER_RMFAIL
        };
        for (int code : codes) {
            XAException expected = failure(code, null);
            assertSame(expected, JmsXAResource.preparedRollbackFailure(expected));
        }
    }

    @Test
    public void rollbackOfPreparedBranchNeverRetries() {
        for (int code : new int[] {XAException.XAER_RMERR, XAException.XAER_PROTO, XAException.XA_RBROLLBACK}) {
            XAException result = JmsXAResource.preparedRollbackFailure(failure(code, null));
            assertEquals(XAException.XAER_RMFAIL, result.errorCode, "code " + code);
        }
    }
}
