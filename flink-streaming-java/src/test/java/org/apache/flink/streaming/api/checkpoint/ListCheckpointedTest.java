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

package org.apache.flink.streaming.api.checkpoint;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link ListCheckpointed}. */
public class ListCheckpointedTest {

    @Test
    public void testUDFReturningNull() throws Exception {
        testUDF(new TestUserFunction(null));
    }

    @Test
    public void testUDFReturningEmpty() throws Exception {
        testUDF(new TestUserFunction(Collections.<Integer>emptyList()));
    }

    @Test
    public void testUDFReturningData() throws Exception {
        testUDF(new TestUserFunction(Arrays.asList(1, 2, 3)));
    }

    private static void testUDF(TestUserFunction userFunction) throws Exception {
        OperatorSubtaskState snapshot;
        try (AbstractStreamOperatorTestHarness<Integer> testHarness =
                createTestHarness(userFunction)) {
            testHarness.open();
            snapshot = testHarness.snapshot(0L, 0L);
            assertFalse(userFunction.isRestored());
        }
        try (AbstractStreamOperatorTestHarness<Integer> testHarness =
                createTestHarness(userFunction)) {
            testHarness.initializeState(snapshot);
            testHarness.open();
            assertTrue(userFunction.isRestored());
        }
    }

    private static AbstractStreamOperatorTestHarness<Integer> createTestHarness(
            TestUserFunction userFunction) throws Exception {
        return new AbstractStreamOperatorTestHarness<>(new StreamMap<>(userFunction), 1, 1, 0);
    }

    private static class TestUserFunction extends RichMapFunction<Integer, Integer>
            implements ListCheckpointed<Integer> {

        private static final long serialVersionUID = -8981369286399531925L;

        private final List<Integer> expected;
        private boolean restored;

        public TestUserFunction(List<Integer> expected) {
            this.expected = expected;
            this.restored = false;
        }

        @Override
        public Integer map(Integer value) throws Exception {
            return value;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return expected;
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            if (null != expected) {
                Assert.assertEquals(expected, state);
            } else {
                assertTrue(state.isEmpty());
            }
            restored = true;
        }

        public boolean isRestored() {
            return restored;
        }
    }
}
