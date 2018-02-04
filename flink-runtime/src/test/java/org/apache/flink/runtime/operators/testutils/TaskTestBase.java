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

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.partition.consumer.IteratorWrappingTestSingleInputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.Driver;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.testutils.recordutils.RecordSerializerFactory;
import org.apache.flink.types.Record;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.TestLogger;

import org.junit.After;

import java.util.List;

public abstract class TaskTestBase extends TestLogger {

	protected long memorySize = 0;

	protected MockInputSplitProvider inputSplitProvider;

	protected MockEnvironment mockEnv;

	public void initEnvironment(long memorySize, int bufferSize) {
		this.memorySize = memorySize;
		this.inputSplitProvider = new MockInputSplitProvider();
		TestTaskStateManager taskStateManager = new TestTaskStateManager();
		this.mockEnv = new MockEnvironment("mock task", this.memorySize, this.inputSplitProvider, bufferSize, taskStateManager);
	}

	public IteratorWrappingTestSingleInputGate<Record> addInput(MutableObjectIterator<Record> input, int groupId) {
		return addInput(input, groupId, true);
	}

	public IteratorWrappingTestSingleInputGate<Record> addInput(MutableObjectIterator<Record> input, int groupId, boolean read) {
		final IteratorWrappingTestSingleInputGate<Record> reader = this.mockEnv.addInput(input);
		TaskConfig conf = new TaskConfig(this.mockEnv.getTaskConfiguration());
		conf.addInputToGroup(groupId);
		conf.setInputSerializer(RecordSerializerFactory.get(), groupId);

		if (read) {
			reader.notifyNonEmpty();
		}

		return reader;
	}

	public void addOutput(List<Record> output) {
		this.mockEnv.addOutput(output);
		TaskConfig conf = new TaskConfig(this.mockEnv.getTaskConfiguration());
		conf.addOutputShipStrategy(ShipStrategyType.FORWARD);
		conf.setOutputSerializer(RecordSerializerFactory.get());
	}

	public TaskConfig getTaskConfig() {
		return new TaskConfig(this.mockEnv.getTaskConfiguration());
	}

	public Configuration getConfiguration() {
		return this.mockEnv.getTaskConfiguration();
	}

	public void registerTask(
			@SuppressWarnings("rawtypes") Class<? extends Driver> driver,
			Class<? extends RichFunction> stubClass) {

		final TaskConfig config = new TaskConfig(this.mockEnv.getTaskConfiguration());
		config.setDriver(driver);
		config.setStubWrapper(new UserCodeClassWrapper<>(stubClass));
	}

	public void registerFileOutputTask(Class<? extends FileOutputFormat<Record>> stubClass, String outPath) {
		registerFileOutputTask(InstantiationUtil.instantiate(stubClass, FileOutputFormat.class), outPath);
	}

	public void registerFileOutputTask(FileOutputFormat<Record> outputFormat, String outPath) {
		TaskConfig dsConfig = new TaskConfig(this.mockEnv.getTaskConfiguration());

		outputFormat.setOutputFilePath(new Path(outPath));
		outputFormat.setWriteMode(WriteMode.OVERWRITE);

		dsConfig.setStubWrapper(new UserCodeObjectWrapper<>(outputFormat));
	}

	public void registerFileInputTask(AbstractInvokable inTask,
			Class<? extends DelimitedInputFormat<Record>> stubClass, String inPath, String delimiter)
	{
		DelimitedInputFormat<Record> format;
		try {
			format = stubClass.newInstance();
		}
		catch (Throwable t) {
			throw new RuntimeException("Could not instantiate test input format.", t);
		}

		format.setFilePath(inPath);
		format.setDelimiter(delimiter);

		TaskConfig dsConfig = new TaskConfig(this.mockEnv.getTaskConfiguration());
		dsConfig.setStubWrapper(new UserCodeObjectWrapper<>(format));

		this.inputSplitProvider.addInputSplits(inPath, 5);
	}

	public MemoryManager getMemoryManager() {
		return this.mockEnv.getMemoryManager();
	}

	@After
	public void shutdown() {
		mockEnv.close();
	}
}
