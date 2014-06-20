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

/**
 * A JobTaskVertex is the vertex type for regular tasks (with both input and output) in Nephele.
 * Tasks running inside a JobTaskVertex must specify at least one record reader and one record writer.
 * 
 */
public class JobTaskVertex extends AbstractJobVertex {

	/**
	 * Creates a new job task vertex with the specified name.
	 * 
	 * @param name
	 *        the name for the new job task vertex
	 * @param jobGraph
	 *        the job graph this vertex belongs to
	 */
	public JobTaskVertex(String name, JobGraph jobGraph) {
		this(name, null, jobGraph);
	}
	
	public JobTaskVertex(String name, JobVertexID id, JobGraph jobGraph) {
		super(name, id, jobGraph);
		jobGraph.addVertex(this);
	}

	/**
	 * Creates a new job task vertex.
	 * 
	 * @param jobGraph
	 *        the job graph this vertex belongs to
	 */
	public JobTaskVertex(JobGraph jobGraph) {
		this(null, jobGraph);
	}
}
