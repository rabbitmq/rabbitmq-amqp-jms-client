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
package com.rabbitmq.client.jms.meta;

import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.Xid;

/**
 * Describes an operation on an XA branch that is sent to the provider.
 */
public final class JmsXaRequest {

    public enum Type {
        /** Starts a new branch. */
        START,
        /** Associates the session with a branch that was ended with a suspend again. */
        RESUME,
        /** Dissociates the session from its branch. */
        END,
        PREPARE,
        COMMIT_ONE_PHASE,
        COMMIT,
        ROLLBACK,
        FORGET,
        RECOVER
    }

    private final Type type;
    private final JmsSessionId sessionId;
    private final Xid xid;
    private JmsTransactionId transactionId;
    private Xid startAfter;
    private final List<Xid> recovered = new ArrayList<>();
    private boolean more;

    public JmsXaRequest(Type type, JmsSessionId sessionId, Xid xid) {
        this.type = type;
        this.sessionId = sessionId;
        this.xid = xid;
    }

    public Type getType() {
        return type;
    }

    /**
     * @return whether any session of the connection can run the operation
     */
    public boolean runsOnAnySession() {
        switch (type) {
            case COMMIT:
            case ROLLBACK:
            case FORGET:
            case RECOVER:
                return true;
            default:
                return false;
        }
    }

    public JmsSessionId getSessionId() {
        return sessionId;
    }

    /**
     * @return the branch, or <code>null</code> for a request that does not refer to one
     */
    public Xid getXid() {
        return xid;
    }

    /**
     * @return the transaction ID that identifies a new branch within the connection
     */
    public JmsTransactionId getTransactionId() {
        return transactionId;
    }

    public JmsXaRequest setTransactionId(JmsTransactionId transactionId) {
        this.transactionId = transactionId;
        return this;
    }

    /**
     * @return the last branch of the previous page of a recover request, if any
     */
    public Xid getStartAfter() {
        return startAfter;
    }

    public JmsXaRequest setStartAfter(Xid startAfter) {
        this.startAfter = startAfter;
        return this;
    }

    /**
     * Set by the provider for a recover request.
     */
    public void setRecovered(List<Xid> xids, boolean more) {
        this.recovered.clear();
        this.recovered.addAll(xids);
        this.more = more;
    }

    public List<Xid> getRecovered() {
        return recovered;
    }

    public boolean hasMore() {
        return more;
    }

    @Override
    public String toString() {
        return "XA " + type + " " + (xid == null ? "" : xid);
    }
}
