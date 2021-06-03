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

import org.apache.flink.streaming.connectors.dynamodb.batch.BatchRequest;

/**
 * Represents one attempt at writing a batch to the backend. The attempt may or may not be
 * successful. If unsuccessful, an error code and error message are provided. In addition, data
 * about latency are also provided. Each batch may have multiple attempts.
 *
 * @see BatchRequest
 */
public class Attempt {
    private int delay;
    private int duration;
    private String errorMessage;
    private String errorCode;
    private boolean success;

    public Attempt(
            int delay, int duration, String errorMessage, String errorCode, boolean success) {
        this.delay = delay;
        this.duration = duration;
        this.errorMessage = errorMessage;
        this.errorCode = errorCode;
        this.success = success;
    }

    /**
     * @return Delay in milliseconds between the start of this attempt and the previous attempt. If
     *     this is the first attempt, then returns the delay between the message reaching the daemon
     *     and the first attempt.
     */
    public int getDelay() {
        return delay;
    }

    /**
     * @return Duration of the attempt. Mainly consists of network and server latency, but also
     *     includes processing overhead within the daemon.
     */
    public int getDuration() {
        return duration;
    }

    /** @return Error message associated with this attempt. Null if no error (i.e. successful). */
    public String getErrorMessage() {
        return errorMessage;
    }

    /** @return Error code associated with this attempt. Null if no error (i.e. successful). */
    public String getErrorCode() {
        return errorCode;
    }

    /**
     * @return Whether the attempt was successful. If true, then the batch has been confirmed by the
     *     backend.
     */
    public boolean isSuccessful() {
        return success;
    }
}
