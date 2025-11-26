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

package org.apache.flink.connector.file.table.stream;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

class ProcTimeCommitTriggerTest {

    private static final ListStateDescriptor<Map<String, Long>> STATE_DESC =
            new ListStateDescriptor<>(
                    "pending-partitions-with-time",
                    new MapSerializer<>(StringSerializer.INSTANCE, LongSerializer.INSTANCE));

    @Test
    void loadsPendingPartitionsWhenRestored() throws Exception {
        Map<String, Long> pending = new HashMap<>();
        pending.put("p-1", 1L);
        pending.put("p-2", 2L);
        List<Map<String, Long>> state = new ArrayList<>();
        state.add(pending);

        ListState<Map<String, Long>> listState = Mockito.mock(ListState.class);
        when(listState.get()).thenReturn(state);

        OperatorStateStore stateStore = Mockito.mock(OperatorStateStore.class);
        when(stateStore.getListState(STATE_DESC)).thenReturn(listState);
        ProcessingTimeService timeService = Mockito.mock(ProcessingTimeService.class);

        ProcTimeCommitTrigger trigger =
                new ProcTimeCommitTrigger(true, stateStore, timeService, ctx -> false);

        assertThat(trigger.endInput()).containsExactlyInAnyOrder("p-1", "p-2");
    }

    @Test
    void loadsEmptyPendingPartitionsWhenRestored() throws Exception {
        List<Map<String, Long>> state = new ArrayList<>();

        ListState<Map<String, Long>> listState = Mockito.mock(ListState.class);
        when(listState.get()).thenReturn(state);

        OperatorStateStore stateStore = Mockito.mock(OperatorStateStore.class);
        when(stateStore.getListState(STATE_DESC)).thenReturn(listState);
        ProcessingTimeService timeService = Mockito.mock(ProcessingTimeService.class);

        ProcTimeCommitTrigger trigger =
                new ProcTimeCommitTrigger(true, stateStore, timeService, ctx -> false);

        Assertions.assertTrue(trigger.endInput().isEmpty());
    }
}
