/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link RecreateOnResetOperatorCoordinator}.
 */
public class RecreateOnResetOperatorCoordinatorTest {
	private static final OperatorID OPERATOR_ID = new OperatorID(1234L, 5678L);
	private static final int NUM_SUBTASKS = 1;

	@Test
	public void testQuiesceableContextNotQuiesced() throws TaskNotRunningException {
		MockOperatorCoordinatorContext context = new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
		RecreateOnResetOperatorCoordinator.QuiesceableContext quiesceableContext =
				new RecreateOnResetOperatorCoordinator.QuiesceableContext(context);

		TestingEvent event = new TestingEvent();
		quiesceableContext.sendEvent(event, 0);
		quiesceableContext.failJob(new Exception());

		assertEquals(OPERATOR_ID, quiesceableContext.getOperatorId());
		assertEquals(NUM_SUBTASKS, quiesceableContext.currentParallelism());
		assertEquals(Collections.singletonList(event), context.getEventsToOperatorBySubtaskId(0));
		assertTrue(context.isJobFailed());
	}

	@Test
	public void testQuiescedContext() throws TaskNotRunningException {
		MockOperatorCoordinatorContext context = new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
		RecreateOnResetOperatorCoordinator.QuiesceableContext quiesceableContext =
				new RecreateOnResetOperatorCoordinator.QuiesceableContext(context);

		quiesceableContext.quiesce();
		quiesceableContext.sendEvent(new TestingEvent(), 0);
		quiesceableContext.failJob(new Exception());

		assertEquals(OPERATOR_ID, quiesceableContext.getOperatorId());
		assertEquals(NUM_SUBTASKS, quiesceableContext.currentParallelism());
		assertTrue(context.getEventsToOperator().isEmpty());
		assertFalse(context.isJobFailed());
	}

	@Test
	public void testResetToCheckpoint() throws Exception {
		TestingCoordinatorProvider provider = new TestingCoordinatorProvider();
		MockOperatorCoordinatorContext context = new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
		RecreateOnResetOperatorCoordinator coordinator = createCoordinator(provider, context);

		RecreateOnResetOperatorCoordinator.QuiesceableContext contextBeforeReset = coordinator.getQuiesceableContext();
		TestingOperatorCoordinator internalCoordinatorBeforeReset = getInternalCoordinator(coordinator);

		byte[] stateToRestore = new byte[0];
		coordinator.resetToCheckpoint(stateToRestore);

		assertTrue(contextBeforeReset.isQuiesced());
		assertNull(internalCoordinatorBeforeReset.getLastRestoredCheckpointState());

		TestingOperatorCoordinator internalCoordinatorAfterReset = getInternalCoordinator(coordinator);
		assertEquals(stateToRestore, internalCoordinatorAfterReset.getLastRestoredCheckpointState());
	}

	// ---------------

	private RecreateOnResetOperatorCoordinator createCoordinator(
			TestingCoordinatorProvider provider,
			OperatorCoordinator.Context context) {
		return (RecreateOnResetOperatorCoordinator) provider.create(context);
	}

	private TestingOperatorCoordinator getInternalCoordinator(RecreateOnResetOperatorCoordinator coordinator) {
		return (TestingOperatorCoordinator) coordinator.getInternalCoordinator();
	}

	// ---------------

	private static class TestingCoordinatorProvider extends RecreateOnResetOperatorCoordinator.Provider {
		private static final long serialVersionUID = 4184184580789587013L;

		public TestingCoordinatorProvider() {
			super(OPERATOR_ID);
		}

		@Override
		protected OperatorCoordinator getCoordinator(OperatorCoordinator.Context context) {
			return new TestingOperatorCoordinator(context);
		}
	}

	private static class TestingEvent implements OperatorEvent {
		private static final long serialVersionUID = -3289352911927668275L;
	}
}
