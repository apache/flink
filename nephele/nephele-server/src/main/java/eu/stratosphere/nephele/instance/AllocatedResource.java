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

package eu.stratosphere.nephele.instance;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import eu.stratosphere.nephele.executiongraph.ExecutionVertex;

/**
 * An allocated resource object unambiguously defines the
 * hardware resources which have been assigned to an {@link eu.stratosphere.nephele.executiongraph.ExecutionVertex} for
 * executing a task. The allocated resource is comprised of an {@link eu.stratosphere.nephele.instance.AbstractInstance}
 * which identifies the node the task is scheduled to run on as well as an
 * {@link eu.stratosphere.nephele.instance.AllocationID} which determines the resources the task is scheduled to
 * allocate within the node.
 * <p>
 * The class is thread-safe.
 * 
 * @author warneke
 */
public final class AllocatedResource {

	/**
	 * The instance a task is scheduled to run on.
	 */
	private final AbstractInstance instance;

	/**
	 * The instance type this allocated resource represents.
	 */
	private final InstanceType instanceType;

	/**
	 * The allocation ID identifying the resources within the instance
	 * which the task is expected to run on.
	 */
	private final AllocationID allocationID;

	/**
	 * The set stores the execution vertices which are currently scheduled to run this resource.
	 */
	private final Set<ExecutionVertex> assignedVertices = Collections
		.newSetFromMap(new ConcurrentHashMap<ExecutionVertex, Boolean>());

	/**
	 * Constructs a new allocated resource object.
	 * 
	 * @param instance
	 *        the instance a task is scheduled to run on.
	 * @param instanceType
	 *        the instance type this allocated resource represents
	 * @param allocationID
	 *        the allocation ID identifying the allocated resources within the instance
	 */
	public AllocatedResource(final AbstractInstance instance, final InstanceType instanceType,
			final AllocationID allocationID) {
		this.instance = instance;
		this.instanceType = instanceType;
		this.allocationID = allocationID;
	}

	/**
	 * Returns the instance a task is scheduled to run on.
	 * 
	 * @return the instance a task is scheduled to run on
	 */
	public AbstractInstance getInstance() {
		return this.instance;
	}

	/**
	 * Returns the allocation ID which identifies the resource allocated within the assigned instance.
	 * 
	 * @return the allocation ID or <code>null</code> if the assigned instance is of type {@link DummyInstance}
	 */
	public AllocationID getAllocationID() {
		return this.allocationID;
	}

	/**
	 * Returns the instance type this allocated resource represents.
	 * 
	 * @return the instance type this allocated resource represents
	 */
	public InstanceType getInstanceType() {
		return this.instanceType;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {

		if (obj instanceof AllocatedResource) {

			final AllocatedResource allocatedResource = (AllocatedResource) obj;
			if (!this.instance.equals(allocatedResource.getInstance())) {
				return false;
			}

			if (this.allocationID == null) {
				if (allocatedResource.getAllocationID() != null) {
					return false;
				}
			} else {
				if (!this.allocationID.equals(allocatedResource.getAllocationID())) {
					return false;
				}
			}

			if (this.instanceType == null) {
				if (allocatedResource.instance != null) {
					return false;
				}
			} else {
				if (!this.instanceType.equals(allocatedResource.getInstanceType())) {
					return false;
				}
			}

			return true;
		}

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {

		if (this.allocationID == null) {
			return 0;
		}

		return this.allocationID.hashCode();
	}

	/**
	 * Assigns the given execution vertex to this allocated resource.
	 * 
	 * @param vertex
	 *        the vertex to assign to this resource
	 */
	public void assignVertexToResource(final ExecutionVertex vertex) {

		if (!this.assignedVertices.add(vertex)) {
			throw new IllegalStateException("The vertex " + vertex + " has already been assigned to resource " + this);
		}
	}

	/**
	 * Returns an iterator over all execution vertices currently assigned to this allocated resource.
	 * 
	 * @return an iterator over all execution vertices currently assigned to this allocated resource
	 */
	public Iterator<ExecutionVertex> assignedVertices() {

		return this.assignedVertices.iterator();
	}

	/**
	 * Removes the given execution vertex from this allocated resource.
	 * 
	 * @param vertex
	 *        the execution to be removed
	 */
	public void removeVertexFromResource(final ExecutionVertex vertex) {

		if (!this.assignedVertices.remove(vertex)) {
			throw new IllegalStateException("The vertex " + vertex + " has not been assigned to resource " + this);
		}
	}
}
