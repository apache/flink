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

import eu.stratosphere.nephele.instance.AllocatedResource;

/**
 * Classes implementing the {@link VertexAssignmentListener} interface can register for notifications about changes in
 * the assignment of an {@link ExecutionVertex} to an {@link AllocatedResource}.
 * 
 * @author warneke
 */
public interface VertexAssignmentListener {

	/**
	 * Called when an {@link ExecutionVertex} has been assigned to an {@link AllocatedResource}.
	 * 
	 * @param id
	 *        the ID of the vertex which has been reassigned
	 * @param newAllocatedResource
	 *        the allocated resource the vertex is now assigned to
	 */
	void vertexAssignmentChanged(ExecutionVertexID id, AllocatedResource newAllocatedResource);
}
