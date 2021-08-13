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

import org.apache.flink.streaming.connectors.dynamodb.batch.retry.DefaultBatchWriterRetryPolicy;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for validation in {@link DefaultBatchWriterRetryPolicy}. */
public class DefaultBatchWriterRetryPolicyTest {

    @Test
    public void testShouldNotRetryWithMaximumNumberOfAttempts() {
        int maxNumberOfAttempts = 5;

        WriterAttemptResult attemptResult = new WriterAttemptResult();
        WriterRetryPolicy writerRetryPolicy =
                new DefaultBatchWriterRetryPolicy(maxNumberOfAttempts);

        attemptResult.setAttemptNumber(maxNumberOfAttempts);
        attemptResult.setFinallySuccessful(false);

        assertFalse(writerRetryPolicy.shouldRetry(attemptResult));
    }

    @Test
    public void testShouldNotRetryWhenFinallySuccessful() {
        int maxNumberOfAttempts = 5;

        WriterAttemptResult attemptResult = new WriterAttemptResult();
        WriterRetryPolicy writerRetryPolicy =
                new DefaultBatchWriterRetryPolicy(maxNumberOfAttempts);

        attemptResult.setAttemptNumber(maxNumberOfAttempts - 1);
        attemptResult.setFinallySuccessful(true);

        assertFalse(writerRetryPolicy.shouldRetry(attemptResult));
    }

    @Test
    public void testShouldRetryWhenNotFinallySuccesful() {
        WriterAttemptResult attemptResult = new WriterAttemptResult();
        WriterRetryPolicy writerRetryPolicy = new DefaultBatchWriterRetryPolicy();

        attemptResult.setAttemptNumber(1000);
        attemptResult.setFinallySuccessful(false);

        assertTrue(writerRetryPolicy.shouldRetry(attemptResult));
    }

    @Test
    public void testShouldRetryWhenNotSuccessfulAndNotMaximumNumberOfAttempts() {
        WriterAttemptResult attemptResult = new WriterAttemptResult();
        WriterRetryPolicy writerRetryPolicy = new DefaultBatchWriterRetryPolicy(5);

        attemptResult.setAttemptNumber(2);
        attemptResult.setFinallySuccessful(false);

        assertTrue(writerRetryPolicy.shouldRetry(attemptResult));
    }
}
