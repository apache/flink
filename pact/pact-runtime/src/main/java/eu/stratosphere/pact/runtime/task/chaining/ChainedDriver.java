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

package eu.stratosphere.pact.runtime.task.chaining;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.RuntimeContext;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.runtime.task.RegularPactTask;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.udf.RuntimeUDFContext;

/**
 * The interface to be implemented by drivers that do not run in an own pact task context, but are chained to other
 * tasks.
 */
public abstract class ChainedDriver<IT, OT> implements Collector<IT> {

	protected TaskConfig config;

	protected String taskName;

	protected Collector<OT> outputCollector;
	
	protected ClassLoader userCodeClassLoader;

	
	public void setup(TaskConfig config, String taskName, Collector<OT> outputCollector,
			AbstractInvokable parent, ClassLoader userCodeClassLoader)
	{
		this.config = config;
		this.taskName = taskName;
		this.outputCollector = outputCollector;
		this.userCodeClassLoader = userCodeClassLoader;

		setup(parent);
	}

	public abstract void setup(AbstractInvokable parent);

	public abstract void openTask() throws Exception;

	public abstract void closeTask() throws Exception;

	public abstract void cancelTask();

	public abstract Stub getStub();

	public abstract String getTaskName();

	@Override
	public abstract void collect(IT record);

	
	protected RuntimeContext getRuntimeContext(AbstractInvokable parent, String name) {
		if (parent instanceof RegularPactTask) {
			return ((RegularPactTask<?, ?>) parent).getRuntimeContext(name);
		} else {
			Environment env = parent.getEnvironment();
			return new RuntimeUDFContext(name, env.getCurrentNumberOfSubtasks(), env.getIndexInSubtaskGroup());
		}
	}

	@SuppressWarnings("unchecked")
	public void setOutputCollector(Collector<?> outputCollector) {
		this.outputCollector = (Collector<OT>) outputCollector;
	}

	public Collector<OT> getOutputCollector() {
		return outputCollector;
	}

}
