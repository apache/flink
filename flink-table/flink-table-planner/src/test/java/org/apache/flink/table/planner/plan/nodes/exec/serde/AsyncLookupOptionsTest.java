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
import org.apache.flink.table.api.config.LookupJoinHintOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.planner.hint.JoinStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.spec.LookupJoinHintTestUtil;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil;

import org.apache.calcite.rel.hint.RelHint;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.AsyncOutputMode.ALLOW_UNORDERED;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_KEY_ORDERED;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_OUTPUT_MODE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.testJsonRoundTrip;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link FunctionCallUtil.AsyncOptions}. */
class AsyncLookupOptionsTest {

    @Test
    void testSerdeAsyncLookupOptions() throws IOException {
        FunctionCallUtil.AsyncOptions asyncLookupOptions =
                LookupJoinUtil.getMergedAsyncOptions(
                        LookupJoinHintTestUtil.completeLookupHint,
                        TableConfig.getDefault(),
                        ChangelogMode.insertOnly());
        testJsonRoundTrip(asyncLookupOptions, FunctionCallUtil.AsyncOptions.class);

        asyncLookupOptions =
                LookupJoinUtil.getMergedAsyncOptions(
                        LookupJoinHintTestUtil.lookupHintWithAsync,
                        TableConfig.getDefault(),
                        ChangelogMode.insertOnly());
        testJsonRoundTrip(asyncLookupOptions, FunctionCallUtil.AsyncOptions.class);

        asyncLookupOptions =
                LookupJoinUtil.getMergedAsyncOptions(
                        LookupJoinHintTestUtil.lookupHintWithRetry,
                        TableConfig.getDefault(),
                        ChangelogMode.insertOnly());
        testJsonRoundTrip(asyncLookupOptions, FunctionCallUtil.AsyncOptions.class);

        asyncLookupOptions =
                LookupJoinUtil.getMergedAsyncOptions(
                        LookupJoinHintTestUtil.lookupHintWithTableOnly,
                        TableConfig.getDefault(),
                        ChangelogMode.insertOnly());
        testJsonRoundTrip(asyncLookupOptions, FunctionCallUtil.AsyncOptions.class);
    }

    @Test
    void testAsyncLookupOptions() {
        FunctionCallUtil.AsyncOptions asyncLookupOptions =
                LookupJoinUtil.getMergedAsyncOptions(
                        LookupJoinHintTestUtil.completeLookupHint,
                        TableConfig.getDefault(),
                        ChangelogMode.insertOnly());
        assertThat(asyncLookupOptions.asyncOutputMode)
                .isSameAs(AsyncDataStream.OutputMode.UNORDERED);

        asyncLookupOptions =
                LookupJoinUtil.getMergedAsyncOptions(
                        LookupJoinHintTestUtil.completeLookupHint,
                        TableConfig.getDefault(),
                        ChangelogMode.all());
        assertThat(asyncLookupOptions.asyncOutputMode).isSameAs(AsyncDataStream.OutputMode.ORDERED);
        assertFalse(asyncLookupOptions.keyOrdered);

        asyncLookupOptions =
                LookupJoinUtil.getMergedAsyncOptions(
                        LookupJoinHintTestUtil.lookupHintWithTableOnly,
                        TableConfig.getDefault(),
                        ChangelogMode.insertOnly());
        assertThat(asyncLookupOptions.asyncOutputMode).isSameAs(AsyncDataStream.OutputMode.ORDERED);
        assertThat(asyncLookupOptions.asyncTimeout)
                .isEqualTo(
                        ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT
                                .defaultValue()
                                .toMillis());
        assertThat(asyncLookupOptions.asyncBufferCapacity)
                .isEqualTo(
                        ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_BUFFER_CAPACITY
                                .defaultValue());

        TableConfig userConf = TableConfig.getDefault();
        userConf.set(TABLE_EXEC_ASYNC_LOOKUP_OUTPUT_MODE, ALLOW_UNORDERED);
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
        assertThat(asyncLookupOptions.asyncOutputMode)
                .isSameAs(AsyncDataStream.OutputMode.UNORDERED);
        assertThat(asyncLookupOptions.asyncTimeout)
                .isEqualTo(
                        ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT
                                .defaultValue()
                                .toMillis());
        assertThat(asyncLookupOptions.asyncBufferCapacity).isEqualTo(1000);

        asyncLookupOptions =
                LookupJoinUtil.getMergedAsyncOptions(
                        RelHint.builder(JoinStrategy.LOOKUP.getJoinHintName())
                                .hintOptions(kvOptions)
                                .build(),
                        userConf,
                        ChangelogMode.all());
        assertThat(asyncLookupOptions.asyncOutputMode).isSameAs(AsyncDataStream.OutputMode.ORDERED);
        assertFalse(asyncLookupOptions.keyOrdered);

        TableConfig config = TableConfig.getDefault();
        config.set(TABLE_EXEC_ASYNC_LOOKUP_KEY_ORDERED, true);
        asyncLookupOptions =
                LookupJoinUtil.getMergedAsyncOptions(
                        LookupJoinHintTestUtil.completeLookupHint, config, ChangelogMode.all());
        assertSame(asyncLookupOptions.asyncOutputMode, AsyncDataStream.OutputMode.ORDERED);
        assertTrue(asyncLookupOptions.keyOrdered);

        asyncLookupOptions =
                LookupJoinUtil.getMergedAsyncOptions(
                        RelHint.builder(JoinStrategy.LOOKUP.getJoinHintName())
                                .hintOptions(kvOptions)
                                .build(),
                        config,
                        ChangelogMode.all());
        assertSame(asyncLookupOptions.asyncOutputMode, AsyncDataStream.OutputMode.ORDERED);
        assertFalse(asyncLookupOptions.keyOrdered);

        config.set(TABLE_EXEC_ASYNC_LOOKUP_OUTPUT_MODE, ALLOW_UNORDERED);
        asyncLookupOptions =
                LookupJoinUtil.getMergedAsyncOptions(
                        RelHint.builder(JoinStrategy.LOOKUP.getJoinHintName())
                                .hintOptions(kvOptions)
                                .build(),
                        config,
                        ChangelogMode.insertOnly());
        assertSame(asyncLookupOptions.asyncOutputMode, AsyncDataStream.OutputMode.UNORDERED);
        assertFalse(asyncLookupOptions.keyOrdered);
    }
}
