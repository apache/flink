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

package eu.stratosphere.nephele.executiongraph;

import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.util.UnmodifiableIterator;

/**
 * An execution pipeline is a scheduling abstraction which defines a set of {@link ExecutionVertex} objects which must
 * be deployed together. An {@link ExecutionVertex} always belongs to exactly one execution pipeline.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class ExecutionPipeline {

	/**
	 * The set of vertices belonging to this execution pipeline.
	 */
	private final CopyOnWriteArrayList<ExecutionVertex> vertices = new CopyOnWriteArrayList<ExecutionVertex>();

	/**
	 * Adds the given {@link ExecutionVertex} to this pipeline.
	 * 
	 * @param vertex
	 *        the vertex to be added to this pipeline
	 */
	void addToPipeline(final ExecutionVertex vertex) {

		if (!this.vertices.addIfAbsent(vertex)) {
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

	/**
	 * Returns an {@link Iterator} to the vertices which are part of this pipeline.
	 * 
	 * @return an iterator to the vertices of this pipeline
	 */
	public Iterator<ExecutionVertex> iterator() {

		return new UnmodifiableIterator<ExecutionVertex>(this.vertices.iterator());
	}

	/**
	 * Checks if the pipeline is currently finishing its execution, i.e. all vertices contained in the pipeline have
	 * switched to the <code>FINISHING</code> or <code>FINISHED</code> state.
	 * 
	 * @return <code>true</code> if the pipeline is currently finishing, <code>false</code> otherwise
	 */
	public boolean isFinishing() {

		final Iterator<ExecutionVertex> it = this.vertices.iterator();
		while (it.hasNext()) {

			final ExecutionState state = it.next().getExecutionState();
			if (state != ExecutionState.FINISHING && state != ExecutionState.FINISHED) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Sets the given allocated resource for all vertices included in this pipeline.
	 * 
	 * @param resource
	 *        the allocated resource to set for all vertices included in this pipeline
	 */
	public void setAllocatedResource(final AllocatedResource resource) {

		final Iterator<ExecutionVertex> it = this.vertices.iterator();
		while (it.hasNext()) {
			final ExecutionVertex vertex = it.next();
			vertex.setAllocatedResource(resource);
		}
	}

	/**
	 * Updates the execution state for all vertices included in this pipeline.
	 * 
	 * @param executionState
	 *        the execution state to set for all vertices included in this pipeline
	 */
	public void updateExecutionState(final ExecutionState executionState) {

		final Iterator<ExecutionVertex> it = this.vertices.iterator();
		while (it.hasNext()) {
			final ExecutionVertex vertex = it.next();
			vertex.updateExecutionState(executionState);
		}

	}
}
