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

package eu.stratosphere.nephele.protocols;

import java.io.IOException;

import eu.stratosphere.core.protocols.VersionedProtocol;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.pact.runtime.iterative.event.AllWorkersDoneEvent;

/**
 * The iteration instruction protocol is implemented by the task manager. The JobManager can
 * use it to request the start of the next superstep or to request termination of
 * an iteration.
 */
public interface IterationInstructionProtocol extends VersionedProtocol {

	/**
	 * Requests the start of the next superstep. It also contains the latest global aggregators inside
	 * of the AllWorkersDoneEvent
	 */
	void startNextSuperstep(ExecutionVertexID headVertexId, AllWorkersDoneEvent allWorkersDoneEvent)
			throws IOException;

	/**
	 * Requests the termination of an iteration head
	 */
	void terminate(ExecutionVertexID headVertexId)
			throws IOException;
}
