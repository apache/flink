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

package org.apache.flink.runtime.operators.testutils;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.partition.consumer.IteratorWrappingTestSingleInputGate;
import org.apache.flink.runtime.io.network.api.serialization.AdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memorymanager.DefaultMemoryManager;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.types.Record;
import org.apache.flink.util.MutableObjectIterator;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockEnvironment implements Environment {
	
	private final MemoryManager memManager;

	private final IOManager ioManager;

	private final InputSplitProvider inputSplitProvider;

	private final Configuration jobConfiguration;

	private final Configuration taskConfiguration;

	private final List<InputGate> inputs;

	private final List<ResultPartitionWriter> outputs;

	private final JobID jobID = new JobID();

	private final BroadcastVariableManager bcVarManager = new BroadcastVariableManager();

	private final int bufferSize;

	public MockEnvironment(long memorySize, MockInputSplitProvider inputSplitProvider, int bufferSize) {
		this.jobConfiguration = new Configuration();
		this.taskConfiguration = new Configuration();
		this.inputs = new LinkedList<InputGate>();
		this.outputs = new LinkedList<ResultPartitionWriter>();

		this.memManager = new DefaultMemoryManager(memorySize, 1);
		this.ioManager = new IOManagerAsync();
		this.inputSplitProvider = inputSplitProvider;
		this.bufferSize = bufferSize;
	}

	public IteratorWrappingTestSingleInputGate<Record> addInput(MutableObjectIterator<Record> inputIterator) {
		try {
			final IteratorWrappingTestSingleInputGate<Record> reader = new IteratorWrappingTestSingleInputGate<Record>(bufferSize, Record.class, inputIterator);

			inputs.add(reader.getInputGate());

			return reader;
		}
		catch (Throwable t) {
			throw new RuntimeException("Error setting up mock readers: " + t.getMessage(), t);
		}
	}

	public void addOutput(final List<Record> outputList) {
		try {
			// The record-oriented writers wrap the buffer writer. We mock it
			// to collect the returned buffers and deserialize the content to
			// the output list
			BufferProvider mockBufferProvider = mock(BufferProvider.class);
			when(mockBufferProvider.requestBufferBlocking()).thenAnswer(new Answer<Buffer>() {

				@Override
				public Buffer answer(InvocationOnMock invocationOnMock) throws Throwable {
					return new Buffer(new MemorySegment(new byte[bufferSize]), mock(BufferRecycler.class));
				}
			});

			ResultPartitionWriter mockWriter = mock(ResultPartitionWriter.class);
			when(mockWriter.getNumberOfOutputChannels()).thenReturn(1);
			when(mockWriter.getBufferProvider()).thenReturn(mockBufferProvider);

			final Record record = new Record();
			final RecordDeserializer<Record> deserializer = new AdaptiveSpanningRecordDeserializer<Record>();

			// Add records from the buffer to the output list
			doAnswer(new Answer<Void>() {

				@Override
				public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
					Buffer buffer = (Buffer) invocationOnMock.getArguments()[0];

					deserializer.setNextBuffer(buffer);

					while (deserializer.hasUnfinishedData()) {
						RecordDeserializer.DeserializationResult result = deserializer.getNextRecord(record);

						if (result.isFullRecord()) {
							outputList.add(record.createCopy());
						}

						if (result == RecordDeserializer.DeserializationResult.LAST_RECORD_FROM_BUFFER
								|| result == RecordDeserializer.DeserializationResult.PARTIAL_RECORD) {
							break;
						}
					}

					return null;
				}
			}).when(mockWriter).writeBuffer(any(Buffer.class), anyInt());

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
	public int getNumberOfSubtasks() {
		return 1;
	}

	@Override
	public int getIndexInSubtaskGroup() {
		return 0;
	}

	@Override
	public InputSplitProvider getInputSplitProvider() {
		return this.inputSplitProvider;
	}

	@Override
	public String getTaskName() {
		return null;
	}

	@Override
	public String getTaskNameWithSubtasks() {
		return null;
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
	public void reportAccumulators(Map<String, Accumulator<?, ?>> accumulators) {
		// discard, this is only for testing
	}
}
