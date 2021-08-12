/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.streaming.connectors.dynamodb.retry;

import org.apache.flink.streaming.connectors.dynamodb.batch.BatchWriterAttemptResult;

import java.util.concurrent.ThreadLocalRandom;

/** Default retry policy for a batch writer. */
public class DefaultBatchWriterRetryPolicy implements BatchWriterRetryPolicy {

    /** Maximum backoff time to wait between retries in milliseconds. */
    private static final int MAX_BACKOFF_TIME_MS = 2048;

    /** Base backoff time for jitter. */
    private static final int BASE_BACKOFF_TIME_MS = 128;

    /**
     * Maximum number of attempts before returning a failure. Value -1 means retry forever until
     * successful.
     */
    private static final int DEFAULT_MAX_NUMBER_OF_ATTEMPTS = -1;

    public final int maxNumOfRetryAttempts;

    public DefaultBatchWriterRetryPolicy() {
        this.maxNumOfRetryAttempts = DEFAULT_MAX_NUMBER_OF_ATTEMPTS;
    }

    /**
     * Default retry policy with Backoff and a Full Jitter.
     *
     * @param maxNumOfRetryAttempts to retry. Value -1 means retry forever until successful.
     */
    public DefaultBatchWriterRetryPolicy(int maxNumOfRetryAttempts) {
        this.maxNumOfRetryAttempts = maxNumOfRetryAttempts;
    }

    @Override
    public boolean shouldRetry(BatchWriterAttemptResult attemptResult) {
        return !attemptResult.isFinallySuccessful()
                && !(maxNumOfRetryAttempts >= 0
                        && attemptResult.getAttemptNumber() >= maxNumOfRetryAttempts);
    }

    /**
     * Get backoff time with Full Jitter function (sleep = random_between(0, min(cap, base * 2 *
     * attempt))) https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/.
     */
    @Override
    public int getBackOffTime(BatchWriterAttemptResult attemptResult) {
        return ThreadLocalRandom.current()
                .nextInt(
                        0,
                        Math.min(
                                MAX_BACKOFF_TIME_MS,
                                BASE_BACKOFF_TIME_MS * 2 * attemptResult.getAttemptNumber()));
    }

    @Override
    public boolean isNotRetryableException(Exception e) {
        return DynamoDbExceptionUtils.isNotRetryableException(e);
    }

    @Override
    public boolean isThrottlingException(Exception e) {
        return DynamoDbExceptionUtils.isThrottlingException(e);
    }
}
