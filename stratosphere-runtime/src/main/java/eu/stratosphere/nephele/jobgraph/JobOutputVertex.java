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

import eu.stratosphere.nephele.template.AbstractOutputTask;

/**
 * A JobOutputVertex is a specific subtype of a {@link AbstractJobOutputVertex} and is designed
 * for Nephele tasks which sink data in a not further specified way. As every job output vertex,
 * a JobOutputVertex must not have any further output.
 * 
 * @author warneke
 */
public class JobOutputVertex extends AbstractJobOutputVertex {

	/**
	 * Creates a new job file output vertex with the specified name.
	 * 
	 * @param name
	 *        the name of the new job file output vertex
	 * @param id
	 *        the ID of this vertex
	 * @param jobGraph
	 *        the job graph this vertex belongs to
	 */
	public JobOutputVertex(final String name, final JobVertexID id, final JobGraph jobGraph) {
		super(name, id, jobGraph);
	}

	/**
	 * Creates a new job file output vertex with the specified name.
	 * 
	 * @param name
	 *        the name of the new job file output vertex
	 * @param jobGraph
	 *        the job graph this vertex belongs to
	 */
	public JobOutputVertex(final String name, final JobGraph jobGraph) {
		super(name, null, jobGraph);
	}

	/**
	 * Creates a new job file input vertex.
	 * 
	 * @param jobGraph
	 *        the job graph this vertex belongs to
	 */
	public JobOutputVertex(final JobGraph jobGraph) {
		super(null, null, jobGraph);
	}

	/**
	 * Sets the class of the vertex's output task.
	 * 
	 * @param outputClass
	 *        The class of the vertex's output task.
	 */
	public void setOutputClass(final Class<? extends AbstractOutputTask> outputClass) {
		this.invokableClass = outputClass;
	}

	/**
	 * Returns the class of the vertex's output task.
	 * 
	 * @return The class of the vertex's output task or <code>null</code> if no task has yet been set.
	 */
	@SuppressWarnings("unchecked")
	public Class<? extends AbstractOutputTask> getOutputClass() {
		return (Class<? extends AbstractOutputTask>) this.invokableClass;
	}
}
