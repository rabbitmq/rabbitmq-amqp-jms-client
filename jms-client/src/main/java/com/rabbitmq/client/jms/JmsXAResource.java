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
import java.util.concurrent.atomic.AtomicLong;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import com.rabbitmq.client.jms.meta.JmsXaRequest;
import com.rabbitmq.client.jms.meta.JmsXaRequest.Type;
import com.rabbitmq.client.jms.provider.Provider;
import com.rabbitmq.client.jms.provider.ProviderFuture;
import com.rabbitmq.client.jms.provider.exceptions.ProviderOperationTimedOutException;

/**
 * The resource that a transaction manager uses to control the branch of a session.
 *
 * A session is associated with at most one branch at a time. Committing, rolling back
 * and forgetting a prepared branch work on any session in the virtual host, which a
 * transaction manager uses for recovery.
 */
public class JmsXAResource implements XAResource {

    private enum State {
        ACTIVE, SUSPENDED, ENDED, ROLLBACK_ONLY, PREPARED
    }

    private static final class Branch {

        private final Xid xid;
        private final String key;
        private final long interruptions;
        private State state;

        private Branch(Xid xid, String key, long interruptions) {
            this.xid = xid;
            this.key = key;
            this.interruptions = interruptions;
        }
    }

    private final JmsXASession session;
    private final JmsConnection connection;
    private final Map<String, Branch> branches = new HashMap<>();
    private final AtomicLong interruptions = new AtomicLong();
    private volatile Branch current;

    JmsXAResource(JmsXASession session) {
        this.session = session;
        this.connection = session.getConnection();
    }

    @Override
    public synchronized void start(Xid xid, int flags) throws XAException {
        String key = key(xid);
        Branch branch = branches.get(key);
        switch (flags) {
            case TMNOFLAGS:
                if (current != null) {
                    throw new XAException(XAException.XAER_PROTO);
                }
                if (branch != null) {
                    throw new XAException(XAException.XAER_DUPID);
                }
                // Read before the request, so that an interruption during the request loses the branch.
                long interruptionsAtStart = interruptions.get();
                connection.xa(request(Type.START, xid).setTransactionId(connection.getNextTransactionId()));
                branch = new Branch(xid, key, interruptionsAtStart);
                branches.put(key, branch);
                break;
            case TMJOIN:
            case TMRESUME:
                if (current != null) {
                    throw new XAException(XAException.XAER_PROTO);
                }
                if (branch == null || (branch.state != State.SUSPENDED && branch.state != State.ENDED)) {
                    throw new XAException(XAException.XAER_NOTA);
                }
                connection.xa(request(Type.RESUME, xid));
                break;
            default:
                throw new XAException(XAException.XAER_INVAL);
        }
        branch.state = State.ACTIVE;
        current = branch;
    }

    @Override
    public synchronized void end(Xid xid, int flags) throws XAException {
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
        Branch branch = branches.get(key(xid));
        if (branch != null && branch.state == State.SUSPENDED && next != State.SUSPENDED) {
            branch.state = next;
            return;
        }
        if (branch == null || branch != current) {
            throw new XAException(XAException.XAER_PROTO);
        }
        // Dissociated first, so that a recovery during the request does not associate the session again.
        current = null;
        try {
            connection.xa(request(Type.END, xid));
        } catch (XAException e) {
            if (!isLost(branch)) {
                current = branch;
                throw e;
            }
        }
        branch.state = next;
    }

    @Override
    public synchronized int prepare(Xid xid) throws XAException {
        Branch branch = branches.get(key(xid));
        if (branch == null) {
            throw new XAException(XAException.XAER_NOTA);
        }
        if (branch.state == State.ROLLBACK_ONLY) {
            rollbackBranch(xid, branch.key, branch);
            throw new XAException(XAException.XA_RBROLLBACK);
        }
        if (branch.state != State.ENDED) {
            throw new XAException(XAException.XAER_PROTO);
        }
        if (isLost(branch)) {
            branches.remove(branch.key);
            throw new XAException(XAException.XA_RBROLLBACK);
        }
        try {
            connection.xa(request(Type.PREPARE, xid));
        } catch (XAException e) {
            branches.remove(branch.key);
            throw e;
        }
        branch.state = State.PREPARED;
        return XA_OK;
    }

    @Override
    public synchronized void commit(Xid xid, boolean onePhase) throws XAException {
        String key = key(xid);
        Branch branch = branches.get(key);
        if (onePhase) {
            if (branch == null) {
                throw new XAException(XAException.XAER_NOTA);
            }
            if (branch.state == State.ROLLBACK_ONLY) {
                rollbackBranch(xid, key, branch);
                throw new XAException(XAException.XA_RBROLLBACK);
            }
            if (branch.state != State.ENDED) {
                throw new XAException(XAException.XAER_PROTO);
            }
            if (isLost(branch)) {
                branches.remove(key);
                throw new XAException(XAException.XA_RBROLLBACK);
            }
            try {
                connection.xa(request(Type.COMMIT_ONE_PHASE, xid));
            } finally {
                branches.remove(key);
            }
        } else {
            if (branch != null && branch.state != State.PREPARED) {
                throw new XAException(XAException.XAER_PROTO);
            }
            try {
                connection.xa(request(Type.COMMIT, xid));
            } catch (XAException e) {
                XAException failure = preparedCommitFailure(e);
                if (failure.errorCode == XAException.XAER_NOTA || failure.errorCode == XAException.XA_HEURRB
                        || failure.errorCode == XAException.XA_HEURCOM) {
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
        rollbackBranch(xid, key, branches.get(key));
    }

    private void rollbackBranch(Xid xid, String key, Branch branch) throws XAException {
        boolean associated = branch != null && branch == current;
        if (associated) {
            current = null;
        }
        if (branch != null && isLost(branch)) {
            branches.remove(key);
            if (associated) {
                // A recovery of the connection may have associated the session with the branch again.
                try {
                    connection.xa(request(Type.END, xid));
                } catch (XAException ignored) {
                }
            }
            return;
        }
        try {
            connection.xa(request(Type.ROLLBACK, xid));
        } catch (XAException e) {
            // A branch that is not open on this session is rolled back by its xid.
            throw branch == null || branch.state == State.PREPARED ? preparedRollbackFailure(e) : e;
        } finally {
            branches.remove(key);
        }
    }

    /**
     * A prepared branch can no longer be rolled back by the resource manager, so a failure of
     * its commit must not look like a failed branch to the transaction manager. Only an
     * unknown branch and a heuristic outcome are final answers, and a lost connection is
     * reported as it is. Every other failure, including a reply that does not arrive, is a
     * reason to try again, which is safe because the commit is repeatable.
     */
    static XAException preparedCommitFailure(XAException failure) {
        switch (failure.errorCode) {
            case XAException.XAER_NOTA:
            case XAException.XA_HEURRB:
            case XAException.XA_HEURCOM:
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
            case XAException.XA_HEURCOM:
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
        connection.xa(request(Type.FORGET, xid));
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
            JmsXaRequest request = new JmsXaRequest(Type.RECOVER, session.getSessionId(), null, null)
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

    /**
     * The broker rolls back a branch that has not been prepared when its session ends or the
     * connection is lost.
     */
    private boolean isLost(Branch branch) {
        return branch.state != State.PREPARED
            && (branch.interruptions != interruptions.get() || session.isClosed());
    }

    boolean isAssociated() {
        return current != null;
    }

    boolean isAssociatedWithLostBranch() {
        Branch branch = current;
        return branch != null && isLost(branch);
    }

    void onConnectionInterrupted() {
        interruptions.incrementAndGet();
    }

    void onConnectionRecovery(Provider provider) throws Exception {
        Branch branch = current;
        if (branch != null) {
            ProviderFuture future = provider.newProviderFuture();
            provider.xa(request(Type.RESUME, branch.xid), future);
            future.sync();
        }
    }

    private JmsXaRequest request(Type type, Xid xid) {
        return new JmsXaRequest(type, session.getSessionId(), xid, key(xid));
    }

    private static String key(Xid xid) {
        Base64.Encoder encoder = Base64.getEncoder();
        return xid.getFormatId() + ":" + encoder.encodeToString(xid.getGlobalTransactionId())
            + ":" + encoder.encodeToString(xid.getBranchQualifier());
    }
}
