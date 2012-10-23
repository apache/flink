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

package eu.stratosphere.nephele.multicast;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ConnectionInfoLookupResponse;
import eu.stratosphere.nephele.taskmanager.bytebuffered.RemoteReceiver;

/**
 * This class contains ConnectionInfoLookupResponse objects containing local, as well as remote receivers for all
 * instances within a certain job-specific multicast tree.
 * 
 * @author casp
 * @author warneke
 */
public class MulticastForwardingTable {

	private static final String ARROW = " -> ";

	private final Map<InstanceConnectionInfo, ConnectionInfoLookupResponse> forwardingTable;

	MulticastForwardingTable(final Map<InstanceConnectionInfo, ConnectionInfoLookupResponse> forwardingTable) {
		this.forwardingTable = Collections.unmodifiableMap(forwardingTable);
	}

	ConnectionInfoLookupResponse getConnectionInfo(InstanceConnectionInfo caller) {
		return this.forwardingTable.get(caller);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		final StringBuilder sb = new StringBuilder();

		final Iterator<Map.Entry<InstanceConnectionInfo, ConnectionInfoLookupResponse>> it = this.forwardingTable
			.entrySet().iterator();

		while (it.hasNext()) {

			final Map.Entry<InstanceConnectionInfo, ConnectionInfoLookupResponse> entry = it.next();
			final InstanceConnectionInfo key = entry.getKey();
			final ConnectionInfoLookupResponse value = entry.getValue();
			final String sourceString = key.toString() + ARROW;
			final String fillString = getFillString(sourceString.length());
			boolean sourcePrinted = false;

			final List<ChannelID> localTargets = value.getLocalTargets();
			final Iterator<ChannelID> localIt = localTargets.iterator();
			while (localIt.hasNext()) {

				if (!sourcePrinted) {
					sourcePrinted = true;
					sb.append(sourceString);
				} else {
					sb.append(fillString);
				}

				sb.append(localIt.next());
				sb.append('\n');
			}

			final List<RemoteReceiver> remoteTargets = value.getRemoteTargets();
			final Iterator<RemoteReceiver> remoteIt = remoteTargets.iterator();
			while (remoteIt.hasNext()) {

				if (!sourcePrinted) {
					sourcePrinted = true;
					sb.append(sourceString);
				} else {
					sb.append(fillString);
				}

				sb.append(remoteIt.next());
				sb.append('\n');
			}
		}

		return sb.toString();
	}

	private static String getFillString(final int length) {

		final StringBuilder sb = new StringBuilder(length);
		for (int i = 0; i < length; ++i) {
			sb.append(' ');
		}

		return sb.toString();
	}
}
