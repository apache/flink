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

package org.apache.flink.runtime.execution;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.deployment.ChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.GateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.IntermediateResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolFactory;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.protocols.AccumulatorProtocol;
import org.apache.flink.runtime.taskmanager.Task;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(Task.class)
public class RuntimeEnvironmentTest {

	@Test
	public void testCleanupAfterPartialSetup() {
		try {
			BufferPoolFactory factory = mock(BufferPoolFactory.class);

			BufferPool bufferPool = mock(BufferPool.class);

			when(factory.createBufferPool(anyInt(), anyBoolean()))
					.thenReturn(bufferPool)
					.thenThrow(new IllegalStateException());

			try {
				createRuntimeEnvironment(1, 1, 1, 1, false, factory);
				Assert.fail("Didn't throw expected exception.");
			}
			catch (Throwable t) {
				// OK => expected exception
			}

			// Verify that everything is cleaned up after partial failure
			verify(bufferPool, times(2)).destroy();
		}
		catch (Throwable t) {
			t.printStackTrace();
			Assert.fail(t.getMessage());
		}
	}

	@Test
	public void testCreateBufferPoolsForInputAndOutput() {
		try {
			BufferPoolFactory factory = mock(BufferPoolFactory.class);

			createRuntimeEnvironment(1, 1, 1, 1, false, factory);

			// Create one buffer pool for output side and one per input gate
			verify(factory, times(1 + 1)).createBufferPool(anyInt(), anyBoolean());
		}
		catch (Throwable t) {
			t.printStackTrace();
			Assert.fail(t.getMessage());
		}
	}

	@Test
	public void testCleanupAfterException() {
		try {
			BufferPoolFactory factory = mock(BufferPoolFactory.class);

			final List<BufferPool> createdBufferPools = new ArrayList();

			when(factory.createBufferPool(anyInt(), anyBoolean()))
					.thenAnswer(new Answer<BufferPool>() {
						@Override
						public BufferPool answer(InvocationOnMock invocationOnMock) throws Throwable {
							BufferPool bufferPool = mock(BufferPool.class);
							createdBufferPools.add(bufferPool);
							return bufferPool;
						}
					});

			RuntimeEnvironment env = createRuntimeEnvironment(1, 1, 1, 1, true, factory);

			env.run();

			for (BufferPool bufferPool : createdBufferPools) {
				verify(bufferPool, times(1)).destroy();
			}
		}
		catch (Throwable t) {
			t.printStackTrace();
			Assert.fail(t.getMessage());
		}
	}

	private RuntimeEnvironment createRuntimeEnvironment(
			int numReaders, int numInputChannelsPerReader, int numProducedPartitions, int numQueues,
			boolean invokeThrowsException, BufferPoolFactory bufferPoolFactory) throws Exception {

		// TODO this only works for one input and one output gate atm, but should be updated
		// accordingly with the removal of input/output gates
		List<GateDeploymentDescriptor> inputGates = new ArrayList<GateDeploymentDescriptor>();
		List<IntermediateResultPartitionDeploymentDescriptor> producedPartitions = new ArrayList<IntermediateResultPartitionDeploymentDescriptor>();

		for (int i = 0; i < numReaders; i++) {
			List<ChannelDeploymentDescriptor> cdd = new ArrayList<ChannelDeploymentDescriptor>();
			for (int j = 0; j < numInputChannelsPerReader; j++) {
				cdd.add(new ChannelDeploymentDescriptor(new InputChannelID(), new InputChannelID()));
			}

			inputGates.add(new GateDeploymentDescriptor());
		}

		for (int i = 0; i < numProducedPartitions; i++) {
			List<ChannelDeploymentDescriptor> cdd = new ArrayList<ChannelDeploymentDescriptor>();
			for (int j = 0; j < numQueues; j++) {
				cdd.add(new ChannelDeploymentDescriptor(new InputChannelID(), new InputChannelID()));
			}

			List<IntermediateResultPartitionDeploymentDescriptor> irpdd = new ArrayList<IntermediateResultPartitionDeploymentDescriptor>();
			IntermediateResultPartitionDeploymentDescriptor irp = Mockito.mock(IntermediateResultPartitionDeploymentDescriptor.class);
			Mockito.when(irp.getGdd()).thenReturn(new GateDeploymentDescriptor(cdd));
			irpdd.add(irp);

			producedPartitions.add(irp);
		}

		String className = invokeThrowsException
				? ExceptionThrowingDummyTask.class.getName()
				: DummyTask.class.getName();

		TaskDeploymentDescriptor tdd = new TaskDeploymentDescriptor(
				new JobID(), new JobVertexID(), new ExecutionAttemptID(), "Dummy task", 0, 1,
				new Configuration(), new Configuration(), className, producedPartitions, inputGates,
				new ArrayList<BlobKey>(), 0);

		return new RuntimeEnvironment(mock(Task.class), tdd, getClass().getClassLoader(),
				mock(MemoryManager.class), mock(IOManager.class), bufferPoolFactory, ,
				mock(TaskEventDispatcher.class),
				mock(InputSplitProvider.class), mock(AccumulatorProtocol.class));
	}

	private static class ExceptionThrowingDummyTask extends AbstractInvokable {

		@Override
		public void registerInputOutput() {
		}

		@Override
		public void invoke() throws Exception {
			throw new RuntimeException("Test exception");
		}
	}

	private static class DummyTask extends AbstractInvokable {

		@Override
		public void registerInputOutput() {
		}

		@Override
		public void invoke() throws Exception {
		}
	}
}
