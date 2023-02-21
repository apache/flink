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

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.planner.hint.JoinStrategy;
import org.apache.flink.table.planner.hint.LookupJoinHintOptions;
import org.apache.flink.table.planner.plan.nodes.exec.spec.LookupJoinHintTestUtil;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil;

import org.apache.calcite.rel.hint.RelHint;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.testJsonRoundTrip;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link LookupJoinUtil.AsyncLookupOptions}. */
public class AsyncLookupOptionsTest {

    @Test
    void testSerdeAsyncLookupOptions() throws IOException {
        LookupJoinUtil.AsyncLookupOptions asyncLookupOptions =
                LookupJoinUtil.getMergedAsyncOptions(
                        LookupJoinHintTestUtil.completeLookupHint,
                        TableConfig.getDefault(),
                        ChangelogMode.insertOnly());
        testJsonRoundTrip(asyncLookupOptions, LookupJoinUtil.AsyncLookupOptions.class);

        asyncLookupOptions =
                LookupJoinUtil.getMergedAsyncOptions(
                        LookupJoinHintTestUtil.lookupHintWithAsync,
                        TableConfig.getDefault(),
                        ChangelogMode.insertOnly());
        testJsonRoundTrip(asyncLookupOptions, LookupJoinUtil.AsyncLookupOptions.class);

        asyncLookupOptions =
                LookupJoinUtil.getMergedAsyncOptions(
                        LookupJoinHintTestUtil.lookupHintWithRetry,
                        TableConfig.getDefault(),
                        ChangelogMode.insertOnly());
        testJsonRoundTrip(asyncLookupOptions, LookupJoinUtil.AsyncLookupOptions.class);

        asyncLookupOptions =
                LookupJoinUtil.getMergedAsyncOptions(
                        LookupJoinHintTestUtil.lookupHintWithTableOnly,
                        TableConfig.getDefault(),
                        ChangelogMode.insertOnly());
        testJsonRoundTrip(asyncLookupOptions, LookupJoinUtil.AsyncLookupOptions.class);
    }

    @Test
    void testAsyncLookupOptions() {
        LookupJoinUtil.AsyncLookupOptions asyncLookupOptions =
                LookupJoinUtil.getMergedAsyncOptions(
                        LookupJoinHintTestUtil.completeLookupHint,
                        TableConfig.getDefault(),
                        ChangelogMode.insertOnly());
        assertTrue(asyncLookupOptions.asyncOutputMode == AsyncDataStream.OutputMode.UNORDERED);

        asyncLookupOptions =
                LookupJoinUtil.getMergedAsyncOptions(
                        LookupJoinHintTestUtil.completeLookupHint,
                        TableConfig.getDefault(),
                        ChangelogMode.all());
        assertTrue(asyncLookupOptions.asyncOutputMode == AsyncDataStream.OutputMode.ORDERED);

        asyncLookupOptions =
                LookupJoinUtil.getMergedAsyncOptions(
                        LookupJoinHintTestUtil.lookupHintWithTableOnly,
                        TableConfig.getDefault(),
                        ChangelogMode.insertOnly());
        assertTrue(asyncLookupOptions.asyncOutputMode == AsyncDataStream.OutputMode.ORDERED);
        assertTrue(
                asyncLookupOptions.asyncTimeout
                        == ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT
                                .defaultValue()
                                .toMillis());
        assertTrue(
                asyncLookupOptions.asyncBufferCapacity
                        == ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_BUFFER_CAPACITY
                                .defaultValue());

        TableConfig userConf = TableConfig.getDefault();
        userConf.set(
                ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_OUTPUT_MODE,
                ExecutionConfigOptions.AsyncOutputMode.ALLOW_UNORDERED);
        userConf.set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_BUFFER_CAPACITY, 300);
        Map<String, String> kvOptions = new HashMap<>();
        kvOptions.put(LookupJoinHintOptions.ASYNC_LOOKUP.key(), "true");
        kvOptions.put(LookupJoinHintOptions.ASYNC_CAPACITY.key(), "1000");
        asyncLookupOptions =
                LookupJoinUtil.getMergedAsyncOptions(
                        RelHint.builder(JoinStrategy.LOOKUP.getJoinHintName())
                                .hintOptions(kvOptions)
                                .build(),
                        userConf,
                        ChangelogMode.insertOnly());
        assertTrue(asyncLookupOptions.asyncOutputMode == AsyncDataStream.OutputMode.UNORDERED);
        assertTrue(
                asyncLookupOptions.asyncTimeout
                        == ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT
                                .defaultValue()
                                .toMillis());
        assertTrue(asyncLookupOptions.asyncBufferCapacity == 1000);

        asyncLookupOptions =
                LookupJoinUtil.getMergedAsyncOptions(
                        RelHint.builder(JoinStrategy.LOOKUP.getJoinHintName())
                                .hintOptions(kvOptions)
                                .build(),
                        userConf,
                        ChangelogMode.all());
        assertTrue(asyncLookupOptions.asyncOutputMode == AsyncDataStream.OutputMode.ORDERED);
    }
}
