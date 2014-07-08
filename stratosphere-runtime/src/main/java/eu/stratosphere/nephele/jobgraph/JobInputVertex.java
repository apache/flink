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

package eu.stratosphere.nephele.jobgraph;

import eu.stratosphere.api.common.io.InputFormat;
import eu.stratosphere.api.common.operators.util.UserCodeObjectWrapper;
import eu.stratosphere.api.common.operators.util.UserCodeWrapper;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.io.InputSplit;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

import java.io.DataInput;
import java.io.IOException;

public class JobInputVertex extends AbstractJobInputVertex {
	private volatile InputFormat<?, ? extends InputSplit> inputFormat = null;

	/**
	 * Creates a new job input vertex with the specified name.
	 * 
	 * @param name
	 *        The name of the new job file input vertex.
	 * @param id
	 *        The ID of this vertex.
	 * @param jobGraph
	 *        The job graph this vertex belongs to.
	 */
	public JobInputVertex(final String name, final JobVertexID id, final JobGraph jobGraph) {
		super(name, id, jobGraph);
	}

	/**
	 * Creates a new job file input vertex with the specified name.
	 * 
	 * @param name
	 *        The name of the new job file input vertex.
	 * @param jobGraph
	 *        The job graph this vertex belongs to.
	 */
	public JobInputVertex(final String name, final JobGraph jobGraph) {
		super(name, null, jobGraph);
	}

	/**
	 * Creates a new job file input vertex.
	 * 
	 * @param jobGraph
	 *        The job graph this vertex belongs to.
	 */
	public JobInputVertex(final JobGraph jobGraph) {
		super(null, null, jobGraph);
	}

	/**
	 * Sets the class of the vertex's input task.
	 *
	 * @param inputClass
	 *        The class of the vertex's input task.
	 */
	public void setInputClass(final Class<? extends AbstractInputTask<?>> inputClass) {
		this.invokableClass = inputClass;
	}

	/**
	 * Returns the class of the vertex's input task.
	 * 
	 * @return the class of the vertex's input task or <code>null</code> if no task has yet been set
	 */
	@SuppressWarnings("unchecked")
	public Class<? extends AbstractInputTask<?>> getInputClass() {
		return (Class<? extends AbstractInputTask<?>>) this.invokableClass;
	}

	public void setInputFormat(UserCodeWrapper<? extends InputFormat<?, ? extends InputSplit>> inputFormatWrapper) {
		TaskConfig config = new TaskConfig(this.getConfiguration());
		config.setStubWrapper(inputFormatWrapper);

		inputFormat = inputFormatWrapper.getUserCodeObject();
	}

	public void setInputFormat(InputFormat<?, ? extends InputSplit> inputFormat) {
		this.inputFormat = inputFormat;

		UserCodeWrapper<? extends InputFormat<?, ? extends InputSplit>> wrapper = new
				UserCodeObjectWrapper<InputFormat<?, ? extends InputSplit>>(inputFormat);
		TaskConfig config = new TaskConfig(this.getConfiguration());
		config.setStubWrapper(wrapper);
	}

	public void setInputFormatParameters(Configuration inputFormatParameters){
		TaskConfig config = new TaskConfig(this.getConfiguration());
		config.setStubParameters(inputFormatParameters);

		if(inputFormat == null){
			throw new RuntimeException("There is no input format set in job vertex: " + this.getID());
		}

		inputFormat.configure(inputFormatParameters);
	}

	public void setOutputSerializer(TypeSerializerFactory<?> factory){
		TaskConfig config = new TaskConfig(this.getConfiguration());
		config.setOutputSerializer(factory);
	}


	@Override
	public void read(final DataInput input) throws IOException{
		super.read(input);

		// load input format wrapper from the config
		ClassLoader cl = null;

		try{
			cl = LibraryCacheManager.getClassLoader(this.getJobGraph().getJobID());
		}
		catch (IOException ioe) {
			throw new RuntimeException("Usercode ClassLoader could not be obtained for job: " +
					this.getJobGraph().getJobID(), ioe);
		}

		final Configuration config = this.getConfiguration();
		config.setClassLoader(cl);
		final TaskConfig taskConfig = new TaskConfig(config);

		inputFormat = taskConfig.<InputFormat<?, InputSplit>>getStubWrapper(cl).getUserCodeObject(InputFormat.class,
				cl);

		inputFormat.configure(taskConfig.getStubParameters());
	}

	@Override
	public Class<? extends InputSplit> getInputSplitType() {
		if(inputFormat == null){
			throw new RuntimeException("No input format has been set for job vertex: "+ this.getID());
		}

		return inputFormat.getInputSplitType();
	}

	@Override
	public InputSplit[] getInputSplits(int minNumSplits) throws IOException {
		if(inputFormat == null){
			throw new RuntimeException("No input format has been set for job vertex: "+ this.getID());
		}

		return inputFormat.createInputSplits(minNumSplits);
	}
}
