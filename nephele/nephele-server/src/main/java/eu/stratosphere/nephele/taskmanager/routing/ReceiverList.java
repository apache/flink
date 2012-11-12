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

package eu.stratosphere.nephele.taskmanager.routing;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;

/**
 * A receiver list contains all recipients of a transfer envelope. Their are two different types of receivers: Local
 * receivers identified by {@link ChannelID} objects and remote receivers identified by {@link InetAddress} objects.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
final class ReceiverList {

	private final List<ChannelID> localReceivers;

	private final List<RemoteReceiver> remoteReceivers;

	ReceiverList(final ConnectionInfoLookupResponse cilr) {

		this.localReceivers = cilr.getLocalTargets();
		this.remoteReceivers = cilr.getRemoteTargets();
	}

	ReceiverList(final ChannelContext channelContext) {

		if (channelContext.getType() != ChannelType.INMEMORY) {
			throw new IllegalArgumentException(
				"Transfer envelope receiver lists can only be constructed from in-memory channels.");
		}

		final List<ChannelID> lr = new ArrayList<ChannelID>(1);
		lr.add(channelContext.getConnectedChannelID());

		this.localReceivers = Collections.unmodifiableList(lr);
		this.remoteReceivers = Collections.emptyList();
	}

	boolean hasLocalReceivers() {

		return (!this.localReceivers.isEmpty());
	}

	boolean hasRemoteReceivers() {

		return (!this.remoteReceivers.isEmpty());
	}

	int getTotalNumberOfReceivers() {

		return (this.localReceivers.size() + this.remoteReceivers.size());
	}

	List<RemoteReceiver> getRemoteReceivers() {

		return this.remoteReceivers;
	}

	List<ChannelID> getLocalReceivers() {

		return this.localReceivers;
	}
}