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

import eu.stratosphere.core.io.InputSplit;

/**
 * An abstract base class for input vertices.
 */
public abstract class AbstractJobInputVertex extends AbstractJobVertex {

	/**
	 * Constructs a new job input vertex with the given name.
	 * 
	 * @param name
	 *        the name of the new job input vertex
	 * @param jobGraph
	 *        the job graph this vertex belongs to
	 */
	protected AbstractJobInputVertex(String name, JobGraph jobGraph) {
		this(name, null, jobGraph);
	}
	
	/**
	 * Constructs a new job input vertex with the given name.
	 * 
	 * @param name
	 *        the name of the new job input vertex
	 * @param id
	 *        the ID of this vertex
	 * @param jobGraph
	 *        the job graph this vertex belongs to
	 */
	protected AbstractJobInputVertex(String name, JobVertexID id, JobGraph jobGraph) {
		super(name, id, jobGraph);

		jobGraph.addVertex(this);
	}

	/**
	 * Returns the input split type of the input splits created by this input vertex
	 *
	 * @return input split type class
	 */
	public abstract Class<? extends InputSplit> getInputSplitType();

	/**
	 * Computes the input splits created by this input vertex
	 *
	 * @param minNumSplits Number of minimal input splits
	 * @return Array of input splits
	 * @throws Exception
	 */
	public abstract InputSplit[] getInputSplits(int minNumSplits) throws Exception;
}
