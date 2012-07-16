/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.test.util;

import java.util.List;

import junit.framework.Assert;

import org.junit.After;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy;
import eu.stratosphere.pact.runtime.task.DataSinkTask;
import eu.stratosphere.pact.runtime.task.DataSourceTask;
import eu.stratosphere.pact.runtime.task.PactDriver;
import eu.stratosphere.pact.runtime.task.RegularPactTask;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public abstract class TaskTestBase
{
	protected long memorySize = 0;

	protected MockInputSplitProvider inputSplitProvider;

	protected MockEnvironment mockEnv;

	public void initEnvironment(long memorySize)
	{
		this.memorySize = memorySize;
		this.inputSplitProvider = new MockInputSplitProvider();
		this.mockEnv = new MockEnvironment(this.memorySize, this.inputSplitProvider);
	}

	public void addInput(MutableObjectIterator<PactRecord> input, int groupId) {
		this.mockEnv.addInput(input);
	}

	public void addOutput(List<PactRecord> output) {
		this.mockEnv.addOutput(output);
		new TaskConfig(this.mockEnv.getTaskConfiguration()).addOutputShipStrategy(ShipStrategy.FORWARD);
	}

	public TaskConfig getTaskConfig() {
		return new TaskConfig(this.mockEnv.getTaskConfiguration());
	}

	public Configuration getConfiguration() {
		return this.mockEnv.getTaskConfiguration();
	}

	public void registerTask(AbstractTask task, @SuppressWarnings("rawtypes") Class<? extends PactDriver> driver, Class<? extends Stub> stubClass)
	{
		final TaskConfig config = new TaskConfig(this.mockEnv.getTaskConfiguration());
		config.setDriver(driver);
		config.setStubClass(stubClass);
		
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

	public void registerFileOutputTask(AbstractOutputTask outTask,
			Class<? extends FileOutputFormat> stubClass, String outPath)
	{
		TaskConfig dsConfig = new TaskConfig(this.mockEnv.getTaskConfiguration());

		dsConfig.setStubClass(stubClass);
		dsConfig.setStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, outPath);

		outTask.setEnvironment(this.mockEnv);

		if (outTask instanceof DataSinkTask<?>) {
			((DataSinkTask<?>) outTask).setUserCodeClassLoader(getClass().getClassLoader());
		}

		outTask.registerInputOutput();
	}

	public void registerFileInputTask(AbstractInputTask<?> inTask,
			Class<? extends DelimitedInputFormat> stubClass, String inPath, String delimiter)
	{
		TaskConfig dsConfig = new TaskConfig(this.mockEnv.getTaskConfiguration());
		dsConfig.setStubClass(stubClass);
		dsConfig.setStubParameter(FileInputFormat.FILE_PARAMETER_KEY, inPath);
		dsConfig.setStubParameter(DelimitedInputFormat.RECORD_DELIMITER, delimiter);

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
	public void shutdownIOManager() throws Exception
	{
		this.mockEnv.getIOManager().shutdown();
		Assert.assertTrue("IO Manager has not properly shut down.", this.mockEnv.getIOManager().isProperlyShutDown());
	}

	@After
	public void shutdownMemoryManager() throws Exception
	{
		if (this.memorySize > 0) {
			MemoryManager memMan = getMemoryManager();
			if (memMan != null) {
				Assert.assertTrue("Memory Manager managed memory was not completely freed.", memMan.verifyEmpty());
				memMan.shutdown();
			}
		}
	}
}
