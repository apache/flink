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

import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.protocols.VersionedProtocol;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ConnectionInfoLookupResponse;

/**
 * The channel lookup protocol can be used to resolve the ID of
 * a network channel to the network connection information for the task manager
 * which manages the channel.
 * 
 * @author warneke
 */
public interface ChannelLookupProtocol extends VersionedProtocol {

	/**
	 * Resolves the given channel ID to the network connection information for
	 * the task manager which manages the channel.
	 * 
	 * @param jobID
	 *        the ID of the job the channel ID belongs to
	 * @param targetChannelID
	 *        the ID of the channel to resolve
	 * @return the lookup response containing the connection info and a return code
	 */
	ConnectionInfoLookupResponse lookupConnectionInfo(JobID jobID, ChannelID targetChannelID) throws IOException;
}
