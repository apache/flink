/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.test.util;

import java.util.List;

import org.junit.After;
import org.junit.Assert;

import eu.stratosphere.api.common.functions.Function;
import eu.stratosphere.api.common.operators.util.UserCodeClassWrapper;
import eu.stratosphere.api.common.operators.util.UserCodeObjectWrapper;
import eu.stratosphere.api.java.record.io.DelimitedInputFormat;
import eu.stratosphere.api.java.record.io.FileOutputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FileSystem.WriteMode;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.api.java.typeutils.runtime.record.RecordSerializerFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.DataSinkTask;
import eu.stratosphere.pact.runtime.task.DataSourceTask;
import eu.stratosphere.pact.runtime.task.PactDriver;
import eu.stratosphere.pact.runtime.task.RegularPactTask;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.InstantiationUtil;
import eu.stratosphere.util.MutableObjectIterator;

public abstract class TaskTestBase {
	
	protected long memorySize = 0;

	protected MockInputSplitProvider inputSplitProvider;

	protected MockEnvironment mockEnv;

	public void initEnvironment(long memorySize) {
		this.memorySize = memorySize;
		this.inputSplitProvider = new MockInputSplitProvider();
		this.mockEnv = new MockEnvironment(this.memorySize, this.inputSplitProvider);
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

	public void registerTask(AbstractTask task, @SuppressWarnings("rawtypes") Class<? extends PactDriver> driver, Class<? extends Function> stubClass) {
		final TaskConfig config = new TaskConfig(this.mockEnv.getTaskConfiguration());
		config.setDriver(driver);
		config.setStubWrapper(new UserCodeClassWrapper<Function>(stubClass));
		
		task.setEnvironment(this.mockEnv);

		if (task instanceof RegularPactTask<?, ?>) {
			((RegularPactTask<?, ?>) task).setUserCodeClassLoader(getClass().getClassLoader());
		}

		task.registerInputOutput();
	}

	public void registerTask(AbstractTask task) {
		task.setEnvironment(this.mockEnv);
		task.registerInputOutput();
	}

	public void registerFileOutputTask(AbstractOutputTask outTask, Class<? extends FileOutputFormat> stubClass, String outPath)
	{
		registerFileOutputTask(outTask, InstantiationUtil.instantiate(stubClass, FileOutputFormat.class), outPath);
	}
	
	public void registerFileOutputTask(AbstractOutputTask outTask, FileOutputFormat outputFormat, String outPath) {
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

	public void registerFileInputTask(AbstractInputTask<?> inTask,
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
