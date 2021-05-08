/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/** Test snapshot state with {@link WrappingFunction}. */
public class WrappingFunctionSnapshotRestoreTest {

    @Test
    public void testSnapshotAndRestoreWrappedCheckpointedFunction() throws Exception {

        StreamMap<Integer, Integer> operator =
                new StreamMap<>(new WrappingTestFun(new WrappingTestFun(new InnerTestFun())));

        OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                new OneInputStreamOperatorTestHarness<>(operator);

        testHarness.setup();
        testHarness.open();

        testHarness.processElement(new StreamRecord<>(5, 12L));

        // snapshot and restore from scratch
        OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);

        testHarness.close();

        InnerTestFun innerTestFun = new InnerTestFun();
        operator = new StreamMap<>(new WrappingTestFun(new WrappingTestFun(innerTestFun)));

        testHarness = new OneInputStreamOperatorTestHarness<>(operator);

        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        Assert.assertTrue(innerTestFun.wasRestored);
        testHarness.close();
    }

    @Test
    public void testSnapshotAndRestoreWrappedListCheckpointed() throws Exception {

        StreamMap<Integer, Integer> operator =
                new StreamMap<>(new WrappingTestFun(new WrappingTestFun(new InnerTestFunList())));

        OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                new OneInputStreamOperatorTestHarness<>(operator);

        testHarness.setup();
        testHarness.open();

        testHarness.processElement(new StreamRecord<>(5, 12L));

        // snapshot and restore from scratch
        OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);

        testHarness.close();

        InnerTestFunList innerTestFun = new InnerTestFunList();
        operator = new StreamMap<>(new WrappingTestFun(new WrappingTestFun(innerTestFun)));

        testHarness = new OneInputStreamOperatorTestHarness<>(operator);

        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        Assert.assertTrue(innerTestFun.wasRestored);
        testHarness.close();
    }

    static class WrappingTestFun extends WrappingFunction<MapFunction<Integer, Integer>>
            implements MapFunction<Integer, Integer> {

        private static final long serialVersionUID = 1L;

        public WrappingTestFun(MapFunction<Integer, Integer> wrappedFunction) {
            super(wrappedFunction);
        }

        @Override
        public Integer map(Integer value) throws Exception {
            return value;
        }
    }

    static class InnerTestFun extends AbstractRichFunction
            implements MapFunction<Integer, Integer>, CheckpointedFunction {

        private static final long serialVersionUID = 1L;

        private ListState<Integer> serializableListState;
        private boolean wasRestored;

        public InnerTestFun() {
            wasRestored = false;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            if (!wasRestored) {
                serializableListState.add(42);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            serializableListState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "test-state", IntSerializer.INSTANCE));
            if (context.isRestored()) {
                Iterator<Integer> integers = serializableListState.get().iterator();
                int act = integers.next();
                Assert.assertEquals(42, act);
                Assert.assertFalse(integers.hasNext());
                wasRestored = true;
            }
        }

        @Override
        public Integer map(Integer value) throws Exception {
            return value;
        }
    }

    static class InnerTestFunList extends AbstractRichFunction
            implements MapFunction<Integer, Integer>, ListCheckpointed<Integer> {

        private static final long serialVersionUID = 1L;

        private boolean wasRestored;

        public InnerTestFunList() {
            wasRestored = false;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(42);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            Assert.assertEquals(1, state.size());
            int val = state.get(0);
            Assert.assertEquals(42, val);
            wasRestored = true;
        }

        @Override
        public Integer map(Integer value) throws Exception {
            return value;
        }
    }
}
