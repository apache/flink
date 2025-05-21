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

package org.apache.flink.table.runtime.operators.aggregate.async;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.flink.shaded.guava33.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava33.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PerKeyCallbackSequencer}. */
public class PerKeyCallbackSequencerTest {

    private static final RowData KEY1 = GenericRowData.of(1);
    private static final RowData KEY2 = GenericRowData.of(2);

    private Map<RowData, List<Long>> callbackTimestamps = new HashMap<>();
    private TestOpenContext openContext;

    @BeforeEach
    public void setUp() {
        openContext = new TestOpenContext();
        callbackTimestamps.clear();
    }

    @Test
    public void testOneCallback() throws Exception {
        PerKeyCallbackSequencer<Object, KeyedAsyncFunction.OpenContext> sequencer =
                new PerKeyCallbackSequencer<>(this::callback);
        openContext.setCurrentKey(KEY1);
        sequencer.callbackWhenNext(openContext, 123);
        assertThat(callbackTimestamps)
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(KEY1, ImmutableList.of(123L)));
        sequencer.notifyNextWaiter(openContext);
    }

    @Test
    public void testManyCallbacksOneKey() throws Exception {
        PerKeyCallbackSequencer<Object, KeyedAsyncFunction.OpenContext> sequencer =
                new PerKeyCallbackSequencer<>(this::callback);
        openContext.setCurrentKey(KEY1);
        sequencer.callbackWhenNext(openContext, 123);
        sequencer.callbackWhenNext(openContext, 345);
        sequencer.callbackWhenNext(openContext, 567);
        assertThat(callbackTimestamps)
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(KEY1, ImmutableList.of(123L)));
        sequencer.notifyNextWaiter(openContext);
        assertThat(callbackTimestamps)
                .containsExactlyInAnyOrderEntriesOf(
                        ImmutableMap.of(KEY1, ImmutableList.of(123L, 345L)));
        sequencer.notifyNextWaiter(openContext);
        assertThat(callbackTimestamps)
                .containsExactlyInAnyOrderEntriesOf(
                        ImmutableMap.of(KEY1, ImmutableList.of(123L, 345L, 567L)));
    }

    @Test
    public void testManyCallbacksTwoKeys() throws Exception {
        PerKeyCallbackSequencer<Object, KeyedAsyncFunction.OpenContext> sequencer =
                new PerKeyCallbackSequencer<>(this::callback);
        openContext.setCurrentKey(KEY1);
        sequencer.callbackWhenNext(openContext, 123);
        openContext.setCurrentKey(KEY2);
        sequencer.callbackWhenNext(openContext, 222);
        openContext.setCurrentKey(KEY1);
        sequencer.callbackWhenNext(openContext, 345);
        openContext.setCurrentKey(KEY2);
        sequencer.callbackWhenNext(openContext, 333);
        openContext.setCurrentKey(KEY1);
        sequencer.callbackWhenNext(openContext, 567);
        openContext.setCurrentKey(KEY2);
        sequencer.callbackWhenNext(openContext, 444);
        assertThat(callbackTimestamps)
                .containsExactlyInAnyOrderEntriesOf(
                        ImmutableMap.of(
                                KEY1, ImmutableList.of(123L), KEY2, ImmutableList.of(222L)));
        openContext.setCurrentKey(KEY1);
        sequencer.notifyNextWaiter(openContext);
        openContext.setCurrentKey(KEY2);
        sequencer.notifyNextWaiter(openContext);
        assertThat(callbackTimestamps)
                .containsExactlyInAnyOrderEntriesOf(
                        ImmutableMap.of(
                                KEY1,
                                ImmutableList.of(123L, 345L),
                                KEY2,
                                ImmutableList.of(222L, 333L)));
        openContext.setCurrentKey(KEY1);
        sequencer.notifyNextWaiter(openContext);
        openContext.setCurrentKey(KEY2);
        sequencer.notifyNextWaiter(openContext);
        assertThat(callbackTimestamps)
                .containsExactlyInAnyOrderEntriesOf(
                        ImmutableMap.of(
                                KEY1,
                                ImmutableList.of(123L, 345L, 567L),
                                KEY2,
                                ImmutableList.of(222L, 333L, 444L)));
    }

    public void callback(long timestamp, Object data, KeyedAsyncFunction.OpenContext ctx)
            throws Exception {
        callbackTimestamps.compute(
                ctx.currentKey(),
                (rowData, longs) -> {
                    if (longs == null) {
                        longs = new ArrayList<>();
                    }
                    longs.add(timestamp);
                    return longs;
                });
    }
}
