/**
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

import java.util.List;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.java.record.io.DelimitedInputFormat;
import org.apache.flink.api.java.record.io.FileOutputFormat;
import org.apache.flink.api.java.typeutils.runtime.record.RecordSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.operators.DataSinkTask;
import org.apache.flink.runtime.operators.DataSourceTask;
import org.apache.flink.runtime.operators.PactDriver;
import org.apache.flink.runtime.operators.RegularPactTask;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.types.Record;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.After;
import org.junit.Assert;

public abstract class TaskTestBase {
	
	protected long memorySize = 0;

	protected MockInputSplitProvider inputSplitProvider;

	protected MockEnvironment mockEnv;

	public void initEnvironment(long memorySize, int bufferSize) {
		this.memorySize = memorySize;
		this.inputSplitProvider = new MockInputSplitProvider();
		this.mockEnv = new MockEnvironment(this.memorySize, this.inputSplitProvider, bufferSize);
	}

	public void addInput(MutableObjectIterator<Record> input, int groupId) {
		this.mockEnv.addInput(input);
		TaskConfig conf = new TaskConfig(this.mockEnv.getTaskConfiguration());
		conf.addInputToGroup(groupId);
		conf.setInputSerializer(RecordSerializerFactory.get(), groupId);
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

	public void registerTask(AbstractInvokable task, @SuppressWarnings("rawtypes") Class<? extends PactDriver> driver, Class<? extends RichFunction> stubClass) {
		final TaskConfig config = new TaskConfig(this.mockEnv.getTaskConfiguration());
		config.setDriver(driver);
		config.setStubWrapper(new UserCodeClassWrapper<RichFunction>(stubClass));
		
		task.setEnvironment(this.mockEnv);

		if (task instanceof RegularPactTask<?, ?>) {
			((RegularPactTask<?, ?>) task).setUserCodeClassLoader(getClass().getClassLoader());
		}

		task.registerInputOutput();
	}

	public void registerTask(AbstractInvokable task) {
		task.setEnvironment(this.mockEnv);
		task.registerInputOutput();
	}

	public void registerFileOutputTask(AbstractInvokable outTask, Class<? extends FileOutputFormat> stubClass, String outPath) {
		registerFileOutputTask(outTask, InstantiationUtil.instantiate(stubClass, FileOutputFormat.class), outPath);
	}
	
	public void registerFileOutputTask(AbstractInvokable outTask, FileOutputFormat outputFormat, String outPath) {
		TaskConfig dsConfig = new TaskConfig(this.mockEnv.getTaskConfiguration());
		
		outputFormat.setOutputFilePath(new Path(outPath));
		outputFormat.setWriteMode(WriteMode.OVERWRITE);

		dsConfig.setStubWrapper(new UserCodeObjectWrapper<FileOutputFormat>(outputFormat));

		outTask.setEnvironment(this.mockEnv);

		if (outTask instanceof DataSinkTask<?>) {
			((DataSinkTask<?>) outTask).setUserCodeClassLoader(getClass().getClassLoader());
		}

		outTask.registerInputOutput();
	}

	public void registerFileInputTask(AbstractInvokable inTask,
			Class<? extends DelimitedInputFormat> stubClass, String inPath, String delimiter)
	{
		DelimitedInputFormat format;
		try {
			format = stubClass.newInstance();
		}
		catch (Throwable t) {
			throw new RuntimeException("Could not instantiate test input format.", t);
		}
		
		format.setFilePath(inPath);
		format.setDelimiter(delimiter);
		
		TaskConfig dsConfig = new TaskConfig(this.mockEnv.getTaskConfiguration());
		dsConfig.setStubWrapper(new UserCodeObjectWrapper<DelimitedInputFormat>(format));
		
		this.inputSplitProvider.addInputSplits(inPath, 5);

		inTask.setEnvironment(this.mockEnv);

		if (inTask instanceof DataSourceTask<?>) {
			((DataSourceTask<?>) inTask).setUserCodeClassLoader(getClass().getClassLoader());
		}
		inTask.registerInputOutput();
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
