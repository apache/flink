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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.table.planner.plan.nodes.exec.spec.LookupJoinHintTestUtil;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil;
import org.apache.flink.table.runtime.operators.join.lookup.ResultRetryStrategy;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.testJsonRoundTrip;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link LookupJoinUtil.RetryLookupOptions}. */
public class RetryLookupOptionsTest {

    @Test
    void testSerdeRetryLookupOptions() throws IOException {
        LookupJoinUtil.RetryLookupOptions retryLookupOptions =
                LookupJoinUtil.RetryLookupOptions.fromJoinHint(
                        LookupJoinHintTestUtil.completeLookupHint);
        testJsonRoundTrip(retryLookupOptions, LookupJoinUtil.RetryLookupOptions.class);

        retryLookupOptions =
                LookupJoinUtil.RetryLookupOptions.fromJoinHint(
                        LookupJoinHintTestUtil.lookupHintWithAsync);
        testJsonRoundTrip(retryLookupOptions, LookupJoinUtil.RetryLookupOptions.class);

        retryLookupOptions =
                LookupJoinUtil.RetryLookupOptions.fromJoinHint(
                        LookupJoinHintTestUtil.lookupHintWithRetry);
        testJsonRoundTrip(retryLookupOptions, LookupJoinUtil.RetryLookupOptions.class);

        retryLookupOptions =
                LookupJoinUtil.RetryLookupOptions.fromJoinHint(
                        LookupJoinHintTestUtil.lookupHintWithTableOnly);
        testJsonRoundTrip(retryLookupOptions, LookupJoinUtil.RetryLookupOptions.class);
    }

    @Test
    void testToRetryStrategy() {
        LookupJoinUtil.RetryLookupOptions retryLookupOptions =
                LookupJoinUtil.RetryLookupOptions.fromJoinHint(
                        LookupJoinHintTestUtil.completeLookupHint);
        assertTrue(retryLookupOptions.toRetryStrategy() != ResultRetryStrategy.NO_RETRY_STRATEGY);

        retryLookupOptions =
                LookupJoinUtil.RetryLookupOptions.fromJoinHint(
                        LookupJoinHintTestUtil.lookupHintWithAsync);
        assertTrue(retryLookupOptions == null);

        retryLookupOptions =
                LookupJoinUtil.RetryLookupOptions.fromJoinHint(
                        LookupJoinHintTestUtil.lookupHintWithRetry);
        assertTrue(retryLookupOptions.toRetryStrategy() != ResultRetryStrategy.NO_RETRY_STRATEGY);

        retryLookupOptions =
                LookupJoinUtil.RetryLookupOptions.fromJoinHint(
                        LookupJoinHintTestUtil.lookupHintWithTableOnly);
        assertTrue(retryLookupOptions == null);
    }
}
