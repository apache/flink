/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.secret.exceptions;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Exception thrown when a requested secret cannot be found in the secret store.
 *
 * <p>This exception is typically thrown by {@link ReadableSecretStore#getSecret(String)} or {@link
 * WritableSecretStore#updateSecret(String, java.util.Map)} when attempting to access or modify a
 * secret that does not exist.
 */
@PublicEvolving
public class SecretNotFoundException extends Exception {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new SecretNotFoundException with the specified detail message.
     *
     * @param message the detail message explaining the reason for the exception
     */
    public SecretNotFoundException(String message) {
        super(message);
    }

    /**
     * Constructs a new SecretNotFoundException with the specified detail message and cause.
     *
     * @param message the detail message explaining the reason for the exception
     * @param cause the cause of the exception
     */
    public SecretNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
