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

package eu.stratosphere.nephele.multicast;

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ConnectionInfoLookupResponse;

/**
 * This class contains ConnectionInfoLookupResponse objects containing local, as well as remote receivers for all
 * instances within a certain job-specific multicast tree.
 * 
 * @author casp
 */
public class MulticastForwardingTable {

	private final Map<InstanceConnectionInfo, ConnectionInfoLookupResponse> forwardingTable = new HashMap<InstanceConnectionInfo, ConnectionInfoLookupResponse>();

	/**
	 * Returns the related ConnectionInfoLookupResponse for the calling Instance.
	 * 
	 * @param caller
	 * @return
	 */
	public ConnectionInfoLookupResponse getConnectionInfo(InstanceConnectionInfo caller) {
		if (this.forwardingTable.containsKey(caller)) {
			return this.forwardingTable.get(caller);
		} else {
			return null;
		}
	}

	protected void addConnectionInfo(InstanceConnectionInfo caller, ConnectionInfoLookupResponse response) {
		this.forwardingTable.put(caller, response);
	}

}
