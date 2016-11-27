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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ResultPartitionTest {

	/**
	 * Tests the schedule or update consumers message sending behaviour depending on the relevant flags.
	 */
	@Test
	public void testSendScheduleOrUpdateConsumersMessage() throws Exception {
		{
			// Pipelined, send message => notify
			ResultPartitionConsumableNotifier notifier = mock(ResultPartitionConsumableNotifier.class);
			ResultPartition partition = createPartition(notifier, ResultPartitionType.PIPELINED, true);
			partition.add(TestBufferFactory.createBuffer(), 0, true);
			verify(notifier, times(1)).notifyPartitionConsumable(any(JobID.class), any(ResultPartitionID.class));
		}

		{
			// Pipelined, don't send message => don't notify
			ResultPartitionConsumableNotifier notifier = mock(ResultPartitionConsumableNotifier.class);
			ResultPartition partition = createPartition(notifier, ResultPartitionType.PIPELINED, false);
			partition.add(TestBufferFactory.createBuffer(), 0, true);
			verify(notifier, never()).notifyPartitionConsumable(any(JobID.class), any(ResultPartitionID.class));
		}

		{
			// Blocking, send message => don't notify
			ResultPartitionConsumableNotifier notifier = mock(ResultPartitionConsumableNotifier.class);
			ResultPartition partition = createPartition(notifier, ResultPartitionType.BLOCKING, true);
			partition.add(TestBufferFactory.createBuffer(), 0, true);
			verify(notifier, never()).notifyPartitionConsumable(any(JobID.class), any(ResultPartitionID.class));
		}

		{
			// Blocking, don't send message => don't notify
			ResultPartitionConsumableNotifier notifier = mock(ResultPartitionConsumableNotifier.class);
			ResultPartition partition = createPartition(notifier, ResultPartitionType.BLOCKING, false);
			partition.add(TestBufferFactory.createBuffer(), 0, true);
			verify(notifier, never()).notifyPartitionConsumable(any(JobID.class), any(ResultPartitionID.class));
		}
	}

	/**
	 * Tests that the pipelined bounded queue length config is correctly forwarded.
	 */
	@Test
	public void testPipelinedBoundedQueueLengthConfig() throws Exception {
		int queueLength = 28;

		ResultPartition partition = new ResultPartition(
			"The Owner",
			new JobID(),
			new ResultPartitionID(),
			ResultPartitionType.PIPELINED_BOUNDED,
			1,
			new ResultPartitionManager(),
			mock(ResultPartitionConsumableNotifier.class),
			new IOManagerAsync(),
			false,
			queueLength);

		PipelinedSubpartition subpartition = (PipelinedSubpartition) partition.getSubPartition(0);
		assertEquals(queueLength, subpartition.getMaxAllowedQueueLength());
	}

	// ------------------------------------------------------------------------

	private static ResultPartition createPartition(
		ResultPartitionConsumableNotifier notifier,
		ResultPartitionType type,
		boolean sendScheduleOrUpdateConsumersMessage) {
		return new ResultPartition(
			"TestTask",
			new JobID(),
			new ResultPartitionID(),
			type,
			1,
			mock(ResultPartitionManager.class),
			notifier,
			mock(IOManager.class),
			sendScheduleOrUpdateConsumersMessage,
			0);
	}
}
