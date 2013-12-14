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

package eu.stratosphere.nephele.jobmanager.splitassigner;

import eu.stratosphere.core.io.InputSplit;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;

/**
 * The input split assigner interface must be implemented by every component which is supposed to dynamically offer
 * input splits to the input vertices of a job at runtime.
 * 
 * @author warneke
 */
public interface InputSplitAssigner {

	/**
	 * Registers an input vertex with the input split assigner.
	 * 
	 * @param groupVertex
	 *        the input vertex to register
	 */
	void registerGroupVertex(ExecutionGroupVertex groupVertex);

	/**
	 * Unregisters an input vertex from the input split assigner. All resources allocated to the input vertex are freed
	 * as part of this operation.
	 * 
	 * @param groupVertex
	 *        the input vertex to unregister
	 */
	void unregisterGroupVertex(ExecutionGroupVertex groupVertex);

	/**
	 * Returns the next input split that shall be consumed by the given input vertex.
	 * 
	 * @param vertex
	 *        the vertex for which the next input split to be consumed shall be determined
	 * @return the next input split to be consumed or <code>null</code> if no more splits shall be consumed by the given
	 *         vertex
	 */
	InputSplit getNextInputSplit(ExecutionVertex vertex);
}
