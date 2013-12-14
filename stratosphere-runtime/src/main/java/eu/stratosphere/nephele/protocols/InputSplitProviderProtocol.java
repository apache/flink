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

package eu.stratosphere.nephele.protocols;

import java.io.IOException;

import eu.stratosphere.core.protocols.VersionedProtocol;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobmanager.splitassigner.InputSplitWrapper;
import eu.stratosphere.nephele.types.IntegerRecord;

/**
 * The input split provider protocol is used to facilitate RPC calls related to the lazy split assignment which Nephele
 * applies to provide better load balancing properties.
 * 
 * @author warneke
 */
public interface InputSplitProviderProtocol extends VersionedProtocol {

	/**
	 * Requests the next split to be consumed by the task with the given execution vertex ID.
	 * 
	 * @param jobID
	 *        the ID of the job the task to retrieve the next input split for belongs to
	 * @param vertexID
	 *        the ID of the task to retrieve the next input split for
	 * @param sequenceNumber
	 *        a sequence number, starting at 0 and increased by the task on each request
	 * @return a wrapper containing the next input split. The wrapped input split may also be <code>null</code> in case
	 *         no more input splits shall be consumed by the task with the given execution vertex ID
	 * @throws IOException
	 *         thrown if an I/O error occurs while retrieving the new input split
	 */
	InputSplitWrapper requestNextInputSplit(JobID jobID, ExecutionVertexID vertexID, IntegerRecord sequenceNumber)
			throws IOException;
}
