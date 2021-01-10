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

package org.apache.flink.state.api.input;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.state.api.input.splits.OperatorStateInputSplit;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/** Test for operator list state input format. */
public class ListStateInputFormatTest {
    private static ListStateDescriptor<Integer> descriptor =
            new ListStateDescriptor<>("state", Types.INT);

    @Test
    public void testReadListOperatorState() throws Exception {
        try (OneInputStreamOperatorTestHarness<Integer, Void> testHarness = getTestHarness()) {
            testHarness.open();

            testHarness.processElement(1, 0);
            testHarness.processElement(2, 0);
            testHarness.processElement(3, 0);

            OperatorSubtaskState subtaskState = testHarness.snapshot(0, 0);
            OperatorState state = new OperatorState(OperatorIDGenerator.fromUid("uid"), 1, 4);
            state.putState(0, subtaskState);

            OperatorStateInputSplit split =
                    new OperatorStateInputSplit(subtaskState.getManagedOperatorState(), 0);

            ListStateInputFormat<Integer> format = new ListStateInputFormat<>(state, descriptor);

            format.setRuntimeContext(new MockStreamingRuntimeContext(false, 1, 0));
            format.open(split);

            List<Integer> results = new ArrayList<>();

            while (!format.reachedEnd()) {
                results.add(format.nextRecord(0));
            }

            results.sort(Comparator.naturalOrder());

            Assert.assertEquals(
                    "Failed to read correct list state from state backend",
                    Arrays.asList(1, 2, 3),
                    results);
        }
    }

    private OneInputStreamOperatorTestHarness<Integer, Void> getTestHarness() throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new StreamFlatMap<>(new StatefulFunction()), IntSerializer.INSTANCE);
    }

    static class StatefulFunction implements FlatMapFunction<Integer, Void>, CheckpointedFunction {
        ListState<Integer> state;

        @Override
        public void flatMap(Integer value, Collector<Void> out) throws Exception {
            state.add(value);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {}

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state = context.getOperatorStateStore().getListState(descriptor);
        }
    }
}
