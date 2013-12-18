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

import eu.stratosphere.nephele.template.AbstractInputTask;

public class JobInputVertex extends AbstractJobInputVertex {

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
}
