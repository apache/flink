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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.record.RecordSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.partition.consumer.IteratorWrappingTestSingleInputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.types.Record;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.After;
import org.junit.Assert;

import java.util.List;


public abstract class StreamTaskTestBase {

	protected long memorySize = 0;

	protected StreamMockEnvironment mockEnv;

	public void initEnvironment(long memorySize, int bufferSize) {
		this.memorySize = memorySize;
		this.mockEnv = new StreamMockEnvironment(this.memorySize, new MockInputSplitProvider(), bufferSize);
	}

	public IteratorWrappingTestSingleInputGate<Record> addInput(MutableObjectIterator<Record> input, int groupId) {
		final IteratorWrappingTestSingleInputGate<Record> reader = addInput(input, groupId, true);

		return reader;
	}

	public IteratorWrappingTestSingleInputGate<Record> addInput(MutableObjectIterator<Record> input, int groupId, boolean read) {
		final IteratorWrappingTestSingleInputGate<Record> reader = this.mockEnv.addInput(input);
		TaskConfig conf = new TaskConfig(this.mockEnv.getTaskConfiguration());
		conf.addInputToGroup(groupId);
		conf.setInputSerializer(RecordSerializerFactory.get(), groupId);

		if (read) {
			reader.read();
		}

		return reader;
	}

	public <T> void addOutput(List<T> output, TypeSerializer<T> serializer) {
		this.mockEnv.addOutput(output, serializer);
		TaskConfig conf = new TaskConfig(this.mockEnv.getTaskConfiguration());
		conf.addOutputShipStrategy(ShipStrategyType.FORWARD);
		conf.setOutputSerializer(RecordSerializerFactory.get());
	}

	public Configuration getConfiguration() {
		return this.mockEnv.getTaskConfiguration();
	}

	public StreamConfig getStreamConfig() {
		return new StreamConfig(this.mockEnv.getTaskConfiguration());
	}

	public void registerTask(AbstractInvokable task) {
		task.setEnvironment(this.mockEnv);
		task.registerInputOutput();
	}

	public MemoryManager getMemoryManager() {
		return this.mockEnv.getMemoryManager();
	}

	@After
	public void shutdownIOManager() throws Exception {
		this.mockEnv.getIOManager().shutdown();
		Assert.assertTrue("IO Manager has not properly shut down.", this.mockEnv.getIOManager().isProperlyShutDown());
	}

	@After
	public void shutdownMemoryManager() throws Exception {
		if (this.memorySize > 0) {
			MemoryManager memMan = getMemoryManager();
			if (memMan != null) {
				Assert.assertTrue("Memory Manager managed memory was not completely freed.", memMan.verifyEmpty());
				memMan.shutdown();
			}
		}
	}
}

