/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.runtime.protocols;

import java.io.IOException;

import org.apache.flink.core.protocols.VersionedProtocol;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.io.network.ConnectionInfoLookupResponse;
import org.apache.flink.runtime.io.network.channels.ChannelID;
import org.apache.flink.runtime.jobgraph.JobID;

/**
 * The channel lookup protocol can be used to resolve the ID of an output channel to all recipients which shall receive
 * the data of this channel.
 * 
 */
public interface ChannelLookupProtocol extends VersionedProtocol {

	/**
	 * Retrieves all recipients of a data for the given <code>sourceChannelID</code>.
	 * 
	 * @param caller
	 *        the {@link InstanceConnectionInfo} object of the task manager which calls this method
	 * @param jobID
	 *        the ID of the job the channel ID belongs to
	 * @param sourceChannelID
	 *        the ID of the channel to resolve
	 * @return the lookup response containing the connection info and a return code
	 * @throws IOException
	 *         thrown if an error occurs during the IPC call
	 */
	ConnectionInfoLookupResponse lookupConnectionInfo(InstanceConnectionInfo caller, JobID jobID,
			ChannelID sourceChannelID) throws IOException;
}
