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
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.reader.RecordReader;
import org.apache.flink.runtime.io.network.api.writer.IntermediateResultWriter;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolFactory;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.runtime.io.network.partition.ChannelID;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.protocols.AccumulatorProtocol;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.types.IntegerRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
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

			when(factory.createBufferPool(any(BufferPoolOwner.class), anyInt(), anyBoolean()))
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
			verify(bufferPool, times(1)).destroy();
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
			verify(factory, times(1 + 1)).createBufferPool(any(BufferPoolOwner.class), anyInt(), anyBoolean());
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

			when(factory.createBufferPool(any(BufferPoolOwner.class), anyInt(), anyBoolean()))
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
			int numInputGates, int numInputChannelsPerInputGate,
			int numOutputGates, int numOutputChannelsPerOutputGate,
			boolean invokeThrowsException, BufferPoolFactory bufferPoolFactory) throws Exception {

		// TODO this only works for one input and one output gate atm, but should be updated
		// accordingly with the removal of input/output gates
		List<GateDeploymentDescriptor> inputGates = new ArrayList<GateDeploymentDescriptor>();
		List<GateDeploymentDescriptor> outputGates = new ArrayList<GateDeploymentDescriptor>();

		for (int i = 0; i < numInputGates; i++) {
			List<ChannelDeploymentDescriptor> cdd = new ArrayList<ChannelDeploymentDescriptor>();
			for (int j = 0; j < numInputChannelsPerInputGate; j++) {
				cdd.add(new ChannelDeploymentDescriptor(new ChannelID(), new ChannelID()));
			}

			inputGates.add(new GateDeploymentDescriptor());
		}

		for (int i = 0; i < numOutputGates; i++) {
			List<ChannelDeploymentDescriptor> cdd = new ArrayList<ChannelDeploymentDescriptor>();
			for (int j = 0; j < numOutputChannelsPerOutputGate; j++) {
				cdd.add(new ChannelDeploymentDescriptor(new ChannelID(), new ChannelID()));
			}

			outputGates.add(new GateDeploymentDescriptor());
		}

		String className = invokeThrowsException
				? ExceptionThrowingDummyTask.class.getName()
				: DummyTask.class.getName();

		TaskDeploymentDescriptor tdd = new TaskDeploymentDescriptor(
				new JobID(), new JobVertexID(), new ExecutionAttemptID(), "Dummy task", 0, 1,
				new Configuration(), new Configuration(), className, inputGates, outputGates,
				new ArrayList<BlobKey>(), 0);

		return new RuntimeEnvironment(mock(Task.class), tdd, getClass().getClassLoader(),
				mock(MemoryManager.class), mock(IOManager.class), bufferPoolFactory,
				mock(InputSplitProvider.class), mock(AccumulatorProtocol.class));
	}

	private static class ExceptionThrowingDummyTask extends AbstractInvokable {

		@Override
		public void registerInputOutput() {
			new RecordReader<IntegerRecord>(this, IntegerRecord.class);
			new IntermediateResultWriter(this);
		}

		@Override
		public void invoke() throws Exception {
			throw new RuntimeException("Test exception");
		}
	}

	private static class DummyTask extends AbstractInvokable {

		@Override
		public void registerInputOutput() {
			new RecordReader<IntegerRecord>(this, IntegerRecord.class);
			new IntermediateResultWriter(this);
		}

		@Override
		public void invoke() throws Exception {
		}
	}
}
