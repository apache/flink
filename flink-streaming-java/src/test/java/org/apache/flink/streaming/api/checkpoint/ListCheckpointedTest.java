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

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ListCheckpointedTest {

	@Test
	public void testUDFReturningNull() throws Exception {
		TestUserFunction userFunction = new TestUserFunction(null);
		AbstractStreamOperatorTestHarness<Integer> testHarness =
				new AbstractStreamOperatorTestHarness<>(new TestOperator(userFunction), 1, 1, 0);
		testHarness.open();
		OperatorStateHandles snapshot = testHarness.snapshot(0L, 0L);
		testHarness.initializeState(snapshot);
		Assert.assertTrue(userFunction.isRestored());
	}

	@Test
	public void testUDFReturningEmpty() throws Exception {
		TestUserFunction userFunction = new TestUserFunction(Collections.<Integer>emptyList());
		AbstractStreamOperatorTestHarness<Integer> testHarness =
				new AbstractStreamOperatorTestHarness<>(new TestOperator(userFunction), 1, 1, 0);
		testHarness.open();
		OperatorStateHandles snapshot = testHarness.snapshot(0L, 0L);
		testHarness.initializeState(snapshot);
		Assert.assertTrue(userFunction.isRestored());
	}

	@Test
	public void testUDFReturningData() throws Exception {
		TestUserFunction userFunction = new TestUserFunction(Arrays.asList(1, 2, 3));
		AbstractStreamOperatorTestHarness<Integer> testHarness =
				new AbstractStreamOperatorTestHarness<>(new TestOperator(userFunction), 1, 1, 0);
		testHarness.open();
		OperatorStateHandles snapshot = testHarness.snapshot(0L, 0L);
		testHarness.initializeState(snapshot);
		Assert.assertTrue(userFunction.isRestored());
	}


	private static class TestUserFunction extends AbstractRichFunction implements ListCheckpointed<Integer> {

		private final List<Integer> expected;
		private boolean restored;

		public TestUserFunction(List<Integer> expected) {
			this.expected = expected;
			this.restored = false;
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
				Assert.assertTrue(state.isEmpty());
			}
			restored = true;
		}

		public boolean isRestored() {
			return restored;
		}
	}

	private static class TestOperator extends AbstractUdfStreamOperator<Integer, TestUserFunction> {

		private final StreamTask<?, ?> containingTask;
		private final CloseableRegistry closeableRegistry;

		public TestOperator(TestUserFunction userFunction) throws Exception {
			super(userFunction);
			this.closeableRegistry = new CloseableRegistry();
			this.containingTask = mock(StreamTask.class);
			when(containingTask.getCancelables()).thenReturn(closeableRegistry);
		}

		@Override
		public StreamTask<?, ?> getContainingTask() {
			return containingTask;
		}
	}
}