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
package com.rabbitmq.client.jms.provider.exceptions;

import javax.transaction.xa.XAException;

import com.rabbitmq.client.jms.provider.ProviderException;

/**
 * Thrown when the remote peer rejects an XA operation with an error that maps to an XA
 * error code.
 */
public class ProviderXaException extends ProviderException {

    private static final long serialVersionUID = 6143329880513526312L;

    private final int errorCode;

    public ProviderXaException(int errorCode, String message) {
        super(message, null);
        this.errorCode = errorCode;
    }

    public ProviderXaException(int errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    /**
     * @return the {@link XAException} error code that the failure maps to.
     */
    public int getErrorCode() {
        return errorCode;
    }
}
