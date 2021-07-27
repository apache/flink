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

package org.apache.flink.streaming.connectors.dynamodb;

/** DynamoDB batch writer response. */
public class ProducerWriteResponse {

    private final String id;
    private final boolean successful;
    private final long elapsedTimeMs;
    public final int numberOfAttempts;

    public final Exception exception;

    public ProducerWriteResponse(
            String id,
            boolean successful,
            int numberOfAttempts,
            Exception attemptException,
            long elapsedTimeMs) {
        this.id = id;
        this.successful = successful;
        this.numberOfAttempts = numberOfAttempts;
        this.elapsedTimeMs = elapsedTimeMs;
        this.exception = attemptException;
    }

    /** Unique request id. */
    public String getId() {
        return id;
    }

    /** Is the write was successful in the end. */
    public boolean isSuccessful() {
        return successful;
    }

    /**
     * Writer exception.
     *
     * @return batch writer exception or null the write was finally successful.
     */
    public Exception getException() {
        return exception;
    }

    /** Total elapsed time of the write. */
    public long getElapsedTimeMs() {
        return this.elapsedTimeMs;
    }

    /** Number of attempts performed. */
    public int getNumberOfAttempts() {
        return numberOfAttempts;
    }
}
