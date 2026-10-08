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

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import com.rabbitmq.client.jms.meta.JmsXaRequest;
import com.rabbitmq.client.jms.meta.JmsXaRequest.Type;
import com.rabbitmq.client.jms.provider.exceptions.ProviderOperationTimedOutException;

/**
 * The resource that a transaction manager uses to control the branch of a session.
 *
 * A session is associated with at most one branch at a time. Preparing, committing and
 * rolling back by xid work on any session of the same user, which a transaction manager
 * uses for recovery.
 */
public class JmsXAResource implements XAResource {

    private enum State {
        ACTIVE, SUSPENDED, ENDED, ROLLBACK_ONLY, PREPARED
    }

    private final JmsXASession session;
    private final JmsConnection connection;
    private final Map<String, State> branches = new HashMap<>();
    private Xid current;

    JmsXAResource(JmsXASession session) {
        this.session = session;
        this.connection = session.getConnection();
    }

    @Override
    public synchronized void start(Xid xid, int flags) throws XAException {
        String key = key(xid);
        State state = branches.get(key);
        switch (flags) {
            case TMNOFLAGS:
                if (current != null) {
                    throw new XAException(XAException.XAER_PROTO);
                }
                if (state != null) {
                    throw new XAException(XAException.XAER_DUPID);
                }
                JmsXaRequest start = new JmsXaRequest(Type.START, session.getSessionId(), xid)
                    .setTransactionId(connection.getNextTransactionId());
                connection.xa(start);
                break;
            case TMJOIN:
            case TMRESUME:
                if (current != null) {
                    throw new XAException(XAException.XAER_PROTO);
                }
                if (state != State.SUSPENDED && state != State.ENDED) {
                    throw new XAException(XAException.XAER_NOTA);
                }
                connection.xa(new JmsXaRequest(Type.RESUME, session.getSessionId(), xid));
                break;
            default:
                throw new XAException(XAException.XAER_INVAL);
        }
        branches.put(key, State.ACTIVE);
        current = xid;
        context().setInBranch(true);
    }

    @Override
    public synchronized void end(Xid xid, int flags) throws XAException {
        String key = key(xid);
        if (current == null || !key(current).equals(key)) {
            throw new XAException(XAException.XAER_PROTO);
        }
        State next;
        switch (flags) {
            case TMSUCCESS:
                next = State.ENDED;
                break;
            case TMSUSPEND:
                next = State.SUSPENDED;
                break;
            case TMFAIL:
                next = State.ROLLBACK_ONLY;
                break;
            default:
                throw new XAException(XAException.XAER_INVAL);
        }
        connection.xa(new JmsXaRequest(Type.END, session.getSessionId(), xid));
        branches.put(key, next);
        current = null;
        context().setInBranch(false);
    }

    @Override
    public synchronized int prepare(Xid xid) throws XAException {
        String key = key(xid);
        State state = branches.get(key);
        if (state == State.ROLLBACK_ONLY) {
            rollbackBranch(xid, key);
            throw new XAException(XAException.XA_RBROLLBACK);
        }
        if (state != State.ENDED) {
            throw new XAException(state == null ? XAException.XAER_NOTA : XAException.XAER_PROTO);
        }
        try {
            connection.xa(new JmsXaRequest(Type.PREPARE, session.getSessionId(), xid));
        } catch (XAException e) {
            branches.remove(key);
            throw e;
        }
        branches.put(key, State.PREPARED);
        return XA_OK;
    }

    @Override
    public synchronized void commit(Xid xid, boolean onePhase) throws XAException {
        String key = key(xid);
        State state = branches.get(key);
        if (onePhase) {
            if (state == State.ROLLBACK_ONLY) {
                rollbackBranch(xid, key);
                throw new XAException(XAException.XA_RBROLLBACK);
            }
            if (state != State.ENDED) {
                throw new XAException(state == null ? XAException.XAER_NOTA : XAException.XAER_PROTO);
            }
            try {
                connection.xa(new JmsXaRequest(Type.COMMIT_ONE_PHASE, session.getSessionId(), xid));
            } finally {
                branches.remove(key);
            }
        } else {
            if (state != null && state != State.PREPARED) {
                throw new XAException(XAException.XAER_PROTO);
            }
            try {
                connection.xa(new JmsXaRequest(Type.COMMIT, session.getSessionId(), xid));
            } catch (XAException e) {
                XAException failure = preparedCommitFailure(e);
                if (failure.errorCode == XAException.XAER_NOTA || failure.errorCode == XAException.XA_HEURRB) {
                    branches.remove(key);
                }
                throw failure;
            }
            branches.remove(key);
        }
    }

    @Override
    public synchronized void rollback(Xid xid) throws XAException {
        String key = key(xid);
        rollbackBranch(xid, key);
    }

    private void rollbackBranch(Xid xid, String key) throws XAException {
        State state = branches.get(key);
        if (current != null && key(current).equals(key)) {
            current = null;
            context().setInBranch(false);
        }
        try {
            connection.xa(new JmsXaRequest(Type.ROLLBACK, session.getSessionId(), xid));
        } catch (XAException e) {
            // A branch that is not open on this session is rolled back by its xid.
            throw state == State.PREPARED || state == null ? preparedRollbackFailure(e) : e;
        } finally {
            branches.remove(key);
        }
    }

    /**
     * A prepared branch can no longer be rolled back by the resource manager, so a failure of
     * its commit must not look like a failed branch to the transaction manager. Only an
     * unknown branch and a heuristic rollback are final answers, and a lost connection is
     * reported as it is. Every other failure, including a reply that does not arrive, is a
     * reason to try again, which is safe because the commit is repeatable.
     */
    static XAException preparedCommitFailure(XAException failure) {
        switch (failure.errorCode) {
            case XAException.XAER_NOTA:
            case XAException.XA_HEURRB:
                return failure;
            case XAException.XAER_RMFAIL:
                if (!(failure.getCause() instanceof ProviderOperationTimedOutException)) {
                    return failure;
                }
                break;
            default:
                break;
        }
        XAException retry = new XAException(XAException.XA_RETRY);
        retry.initCause(failure.getCause() != null ? failure.getCause() : failure);
        return retry;
    }

    /**
     * The same for the rollback of a prepared branch, which cannot return a code to retry, so
     * the failure that is not a final answer is reported as one of the resource manager.
     */
    static XAException preparedRollbackFailure(XAException failure) {
        switch (failure.errorCode) {
            case XAException.XAER_NOTA:
            case XAException.XA_HEURRB:
            case XAException.XAER_RMFAIL:
                return failure;
            default:
                XAException unavailable = new XAException(XAException.XAER_RMFAIL);
                unavailable.initCause(failure.getCause() != null ? failure.getCause() : failure);
                return unavailable;
        }
    }

    @Override
    public void forget(Xid xid) throws XAException {
        connection.xa(new JmsXaRequest(Type.FORGET, session.getSessionId(), xid));
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
        if ((flag & TMSTARTRSCAN) == 0) {
            return new Xid[0];
        }
        List<Xid> xids = new ArrayList<>();
        Xid startAfter = null;
        boolean more = true;
        while (more) {
            JmsXaRequest request = new JmsXaRequest(Type.RECOVER, session.getSessionId(), null)
                .setStartAfter(startAfter);
            connection.xa(request);
            List<Xid> page = request.getRecovered();
            xids.addAll(page);
            more = request.hasMore() && !page.isEmpty();
            if (more) {
                startAfter = page.get(page.size() - 1);
            }
        }
        return xids.toArray(new Xid[0]);
    }

    @Override
    public boolean isSameRM(XAResource xaResource) {
        return this == xaResource;
    }

    @Override
    public int getTransactionTimeout() {
        return 0;
    }

    @Override
    public boolean setTransactionTimeout(int seconds) {
        return false;
    }

    private JmsXATransactionContext context() {
        return (JmsXATransactionContext) session.getTransactionContext();
    }

    private static String key(Xid xid) {
        Base64.Encoder encoder = Base64.getEncoder();
        return xid.getFormatId() + ":" + encoder.encodeToString(xid.getGlobalTransactionId())
            + ":" + encoder.encodeToString(xid.getBranchQualifier());
    }
}
