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

package eu.stratosphere.nephele.instance;

import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocationID;

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
public class AllocatedResource {

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
}
