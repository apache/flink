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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.PartitionRequestManager;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.taskmanager.TaskActions;

import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link InputGateUtil}.
 */
public class InputGateUtilTest {

	@Test
	public void testCreateEmptyInputGate() {
		try {
			InputGateUtil.createInputGate(new InputGate[0]);
			fail();
		} catch (Throwable t) {
			assertEquals("No such input gate.", t.getMessage());
		}

		try {
			InputGateUtil.createInputGate(new InputGate[0], new InputGate[0]);
			fail();
		} catch (Throwable t) {
			assertEquals("No such input gate.", t.getMessage());
		}
	}

	@Test
	public void testCreateInputGate() {
		{
			InputGate inputGate1 = createSingleInputGate(1);
			assertEquals(inputGate1, InputGateUtil.createInputGate(new InputGate[]{inputGate1}));
			assertEquals(inputGate1, InputGateUtil.createInputGate(new InputGate[0], new InputGate[]{inputGate1}));
		}

		{
			InputGate inputGate1 = createSingleInputGate(1);
			InputGate inputGate2 = createSingleInputGate(2);
			InputGate gate = InputGateUtil.createInputGate(new InputGate[]{inputGate1, inputGate2});

			assertTrue(gate instanceof UnionInputGate);
			UnionInputGate unionGate = (UnionInputGate) gate;
			assertEquals(2, unionGate.getSubInputGateCount());
			assertEquals(inputGate1, unionGate.getSubInputGate(0));
			assertEquals(inputGate2, unionGate.getSubInputGate(1));
		}

		{
			InputGate inputGate1 = createSingleInputGate(1);
			InputGate inputGate2 = createSingleInputGate(2);
			InputGate inputGate3 = createSingleInputGate(3);
			InputGate gate = InputGateUtil.createInputGate(new InputGate[]{inputGate1, inputGate2}, new InputGate[]{inputGate3});

			assertTrue(gate instanceof UnionInputGate);
			UnionInputGate unionGate = (UnionInputGate) gate;
			assertEquals(3, unionGate.getSubInputGateCount());
			assertEquals(inputGate1, unionGate.getSubInputGate(0));
			assertEquals(inputGate2, unionGate.getSubInputGate(1));
			assertEquals(inputGate3, unionGate.getSubInputGate(2));
		}

		{
			InputGate inputGate1 = createSingleInputGate(1);
			InputGate inputGate2 = createSingleInputGate(2);
			InputGate inputGate3 = createSingleInputGate(3);
			InputGate gate = InputGateUtil.createInputGate(Arrays.asList(inputGate1, inputGate2), Arrays.asList(inputGate3));

			assertTrue(gate instanceof UnionInputGate);
			UnionInputGate unionGate = (UnionInputGate) gate;
			assertEquals(3, unionGate.getSubInputGateCount());
			assertEquals(inputGate1, unionGate.getSubInputGate(0));
			assertEquals(inputGate2, unionGate.getSubInputGate(1));
			assertEquals(inputGate3, unionGate.getSubInputGate(2));
		}
	}

	// ------------------------------------------------------------------------

	private SingleInputGate createSingleInputGate(int numChannels) {
		return createSingleInputGate(numChannels, new PartitionRequestManager(Integer.MAX_VALUE, 1));
	}

	private SingleInputGate createSingleInputGate(int numChannels, PartitionRequestManager partitionRequestManager) {
		final String testTaskName = "Test Task ";
		return new SingleInputGate(
			testTaskName, new JobID(),
			new IntermediateDataSetID(), ResultPartitionType.PIPELINED,
			0, numChannels,
			mock(TaskActions.class),
			UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup(),
			partitionRequestManager,
			Executors.newSingleThreadExecutor(),
			true,
			false);
	}
}
