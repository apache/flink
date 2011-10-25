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

package eu.stratosphere.nephele.executiongraph;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * An execution pipeline is a scheduling abstraction which defines a set of {@link ExecutionVertex} objects which must
 * be deployed together. An {@link ExecutionVertex} always belongs to exactly one execution pipeline.
 * <p>
 * This class is not thread-safe.
 * 
 * @author warneke
 */
public final class ExecutionPipeline {

	/**
	 * The set of vertices belonging to this execution pipeline.
	 */
	private final Set<ExecutionVertex> vertices = new HashSet<ExecutionVertex>();

	/**
	 * Adds the given {@link ExecutionVertex} to this pipeline.
	 * 
	 * @param vertex
	 *        the vertex to be added to this pipeline
	 */
	void addToPipeline(final ExecutionVertex vertex) {

		if (!this.vertices.add(vertex)) {
			throw new IllegalStateException("Vertex " + vertex + " has already been added to pipeline " + this);
		}
	}

	/**
	 * Removes the given {@link ExecutionVertex} from this pipeline.
	 * 
	 * @param vertex
	 *        the vertex to be removed from this pipeline.
	 */
	void removeFromPipeline(final ExecutionVertex vertex) {

		if (!this.vertices.remove(vertex)) {
			throw new IllegalStateException("Vertex " + vertex + " was not part of the pipeline " + this);
		}
	}
	
}
