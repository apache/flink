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

package org.apache.flink.runtime.io.network.partition.consumer;

import com.google.common.base.Optional;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionLocation;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.util.TestTaskEvent;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SingleInputGateTest {

	@Test
	@SuppressWarnings("unchecked")
	public void testBackwardsEventWithUninitializedChannel() throws Exception {
		// Setup environment
		final TaskEventDispatcher taskEventDispatcher = mock(TaskEventDispatcher.class);
		when(taskEventDispatcher.publish(any(ResultPartitionID.class), any(TaskEvent.class))).thenReturn(true);

		final ResultSubpartitionView iterator = mock(ResultSubpartitionView.class);
		when(iterator.getNextBuffer()).thenReturn(new Buffer(new MemorySegment(new byte[1024]), mock(BufferRecycler.class)));

		final ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
		when(partitionManager.getSubpartition(any(ResultPartitionID.class), anyInt(), any(Optional.class))).thenReturn(iterator);

		// Setup reader with one local and one unknown input channel
		final IntermediateDataSetID resultId = new IntermediateDataSetID();

		final SingleInputGate inputGate = new SingleInputGate(resultId, 0, 2);
		final BufferPool bufferPool = mock(BufferPool.class);
		when(bufferPool.getNumberOfRequiredMemorySegments()).thenReturn(2);

		inputGate.setBufferPool(bufferPool);

		// Local
		ResultPartitionID localPartitionId = new ResultPartitionID(new IntermediateResultPartitionID(), new ExecutionAttemptID());

		InputChannel local = new LocalInputChannel(inputGate, 0, localPartitionId, partitionManager, taskEventDispatcher);

		// Unknown
		ResultPartitionID unknownPartitionId = new ResultPartitionID(new IntermediateResultPartitionID(), new ExecutionAttemptID());

		InputChannel unknown = new UnknownInputChannel(inputGate, 1, unknownPartitionId, partitionManager, taskEventDispatcher, mock(ConnectionManager.class));

		// Set channels
		inputGate.setInputChannel(localPartitionId.getPartitionId(), local);
		inputGate.setInputChannel(unknownPartitionId.getPartitionId(), unknown);

		// Request partitions
		inputGate.requestPartitions();

		// Only the local channel can request
		verify(partitionManager, times(1)).getSubpartition(any(ResultPartitionID.class), anyInt(), any(Optional.class));

		// Send event backwards and initialize unknown channel afterwards
		final TaskEvent event = new TestTaskEvent();
		inputGate.sendTaskEvent(event);

		// Only the local channel can send out the event
		verify(taskEventDispatcher, times(1)).publish(any(ResultPartitionID.class), any(TaskEvent.class));

		// After the update, the pending event should be send to local channel
		inputGate.updateInputChannel(new InputChannelDeploymentDescriptor(new ResultPartitionID(unknownPartitionId.getPartitionId(), unknownPartitionId.getProducerId()), ResultPartitionLocation.createLocal()));

		verify(partitionManager, times(2)).getSubpartition(any(ResultPartitionID.class), anyInt(), any(Optional.class));
		verify(taskEventDispatcher, times(2)).publish(any(ResultPartitionID.class), any(TaskEvent.class));
	}
}
