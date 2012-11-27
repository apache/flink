/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.jobgraph;

import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.IllegalConfigurationException;
import eu.stratosphere.nephele.util.StringUtils;

public class JobGenericInputVertex extends JobInputVertex {
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
	public JobGenericInputVertex(final String name, final JobVertexID id, final JobGraph jobGraph) {
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
	public JobGenericInputVertex(final String name, final JobGraph jobGraph) {
		super(name, null, jobGraph);
	}

	/**
	 * Creates a new job file input vertex.
	 * 
	 * @param jobGraph
	 *        The job graph this vertex belongs to.
	 */
	public JobGenericInputVertex(final JobGraph jobGraph) {
		super(null, null, jobGraph);
	}

	/**
	 * Sets the class of the vertex's input task.
	 * 
	 * @param inputClass
	 *        The class of the vertex's input task.
	 */
	public void setInputClass(final Class<? extends AbstractInputTask<?>> inputClass) {
		this.invokableClassName = inputClass.getName();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void checkConfiguration(final AbstractInvokable invokable) throws IllegalConfigurationException
	{
		// see if the task itself has a valid configuration
		// because this is user code running on the master, we embed it in a catch-all block
		try {
			invokable.checkConfiguration();
		} catch (IllegalConfigurationException icex) {
			throw icex; // simply forward
		} catch (Throwable t) {
			throw new IllegalConfigurationException("Checking the invokable's configuration caused an error: "
				+ StringUtils.stringifyException(t));
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMaximumNumberOfSubtasks(final AbstractInvokable invokable)
	{
		return invokable.getMaximumNumberOfSubtasks();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMinimumNumberOfSubtasks(final AbstractInvokable invokable) {

		return invokable.getMinimumNumberOfSubtasks();
	}
}
