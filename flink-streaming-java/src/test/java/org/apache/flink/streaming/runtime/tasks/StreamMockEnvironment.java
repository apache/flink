/**
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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.api.serialization.AdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Future;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StreamMockEnvironment implements Environment {

	private final TaskInfo taskInfo;

	private final MemoryManager memManager;

	private final IOManager ioManager;

	private final InputSplitProvider inputSplitProvider;

	private final Configuration jobConfiguration;

	private final Configuration taskConfiguration;

	private final List<InputGate> inputs;

	private final List<ResultPartitionWriter> outputs;

	private final JobID jobID = new JobID();

	private final BroadcastVariableManager bcVarManager = new BroadcastVariableManager();

	private final AccumulatorRegistry accumulatorRegistry;

	private final int bufferSize;

	public StreamMockEnvironment(Configuration jobConfig, Configuration taskConfig, long memorySize,
									MockInputSplitProvider inputSplitProvider, int bufferSize) {
		this.taskInfo = new TaskInfo("", 0, 1, 0);
		this.jobConfiguration = jobConfig;
		this.taskConfiguration = taskConfig;
		this.inputs = new LinkedList<InputGate>();
		this.outputs = new LinkedList<ResultPartitionWriter>();

		this.memManager = new MemoryManager(memorySize, 1);
		this.ioManager = new IOManagerAsync();
		this.inputSplitProvider = inputSplitProvider;
		this.bufferSize = bufferSize;

		this.accumulatorRegistry = new AccumulatorRegistry(jobID, getExecutionId());
	}

	public void addInputGate(InputGate gate) {
		inputs.add(gate);
	}

	public <T> void addOutput(final Queue<Object> outputList, final TypeSerializer<T> serializer) {
		try {
			// The record-oriented writers wrap the buffer writer. We mock it
			// to collect the returned buffers and deserialize the content to
			// the output list
			BufferProvider mockBufferProvider = mock(BufferProvider.class);
			when(mockBufferProvider.requestBufferBlocking()).thenAnswer(new Answer<Buffer>() {

				@Override
				public Buffer answer(InvocationOnMock invocationOnMock) throws Throwable {
					return new Buffer(
							MemorySegmentFactory.allocateUnpooledSegment(bufferSize),
							mock(BufferRecycler.class));
				}
			});

			ResultPartitionWriter mockWriter = mock(ResultPartitionWriter.class);
			when(mockWriter.getNumberOfOutputChannels()).thenReturn(1);
			when(mockWriter.getBufferProvider()).thenReturn(mockBufferProvider);

			final RecordDeserializer<DeserializationDelegate<T>> recordDeserializer = new AdaptiveSpanningRecordDeserializer<DeserializationDelegate<T>>();
			final NonReusingDeserializationDelegate<T> delegate = new NonReusingDeserializationDelegate<T>(serializer);

			// Add records from the buffer to the output list
			doAnswer(new Answer<Void>() {

				@Override
				public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
					Buffer buffer = (Buffer) invocationOnMock.getArguments()[0];

					recordDeserializer.setNextBuffer(buffer);

					while (recordDeserializer.hasUnfinishedData()) {
						RecordDeserializer.DeserializationResult result = recordDeserializer.getNextRecord(delegate);

						if (result.isFullRecord()) {
							outputList.add(delegate.getInstance());
						}

						if (result == RecordDeserializer.DeserializationResult.LAST_RECORD_FROM_BUFFER
								|| result == RecordDeserializer.DeserializationResult.PARTIAL_RECORD) {
							break;
						}
					}

					return null;
				}
			}).when(mockWriter).writeBuffer(any(Buffer.class), anyInt());

			// Add events to the output list
			doAnswer(new Answer<Void>() {

				@Override
				public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
					AbstractEvent event = (AbstractEvent) invocationOnMock.getArguments()[0];

					outputList.add(event);
					return null;
				}
			}).when(mockWriter).writeEvent(any(AbstractEvent.class), anyInt());

			doAnswer(new Answer<Void>() {

				@Override
				public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
					AbstractEvent event = (AbstractEvent) invocationOnMock.getArguments()[0];

					outputList.add(event);
					return null;
				}
			}).when(mockWriter).writeEventToAllChannels(any(AbstractEvent.class));

			outputs.add(mockWriter);
		}
		catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}

	@Override
	public Configuration getTaskConfiguration() {
		return this.taskConfiguration;
	}

	@Override
	public MemoryManager getMemoryManager() {
		return this.memManager;
	}

	@Override
	public IOManager getIOManager() {
		return this.ioManager;
	}

	@Override
	public JobID getJobID() {
		return this.jobID;
	}

	@Override
	public Configuration getJobConfiguration() {
		return this.jobConfiguration;
	}

	@Override
	public InputSplitProvider getInputSplitProvider() {
		return this.inputSplitProvider;
	}

	@Override
	public TaskInfo getTaskInfo() {
		return this.taskInfo;
	}

	@Override
	public ClassLoader getUserClassLoader() {
		return getClass().getClassLoader();
	}

	@Override
	public Map<String, Future<Path>> getDistributedCacheEntries() {
		return Collections.emptyMap();
	}

	@Override
	public ResultPartitionWriter getWriter(int index) {
		return outputs.get(index);
	}

	@Override
	public ResultPartitionWriter[] getAllWriters() {
		return outputs.toArray(new ResultPartitionWriter[outputs.size()]);
	}

	@Override
	public InputGate getInputGate(int index) {
		return inputs.get(index);
	}

	@Override
	public InputGate[] getAllInputGates() {
		InputGate[] gates = new InputGate[inputs.size()];
		inputs.toArray(gates);
		return gates;
	}

	@Override
	public JobVertexID getJobVertexId() {
		return new JobVertexID(new byte[16]);
	}

	@Override
	public ExecutionAttemptID getExecutionId() {
		return new ExecutionAttemptID(0L, 0L);
	}

	@Override
	public BroadcastVariableManager getBroadcastVariableManager() {
		return this.bcVarManager;
	}

	@Override
	public AccumulatorRegistry getAccumulatorRegistry() {
		return accumulatorRegistry;
	}

	@Override
	public void acknowledgeCheckpoint(long checkpointId) {
	}

	@Override
	public void acknowledgeCheckpoint(long checkpointId, StateHandle<?> state) {
	}

	@Override
	public TaskManagerRuntimeInfo getTaskManagerInfo() {
		return new TaskManagerRuntimeInfo("localhost", new UnmodifiableConfiguration(new Configuration()));
	}
}

