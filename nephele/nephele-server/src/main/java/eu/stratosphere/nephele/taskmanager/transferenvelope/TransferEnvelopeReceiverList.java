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

package eu.stratosphere.nephele.taskmanager.transferenvelope;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ChannelContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ConnectionInfoLookupResponse;
import eu.stratosphere.nephele.taskmanager.bytebuffered.RemoteReceiver;

/**
 * A transfer envelope receiver list contains all recipients of a transfer envelope. Their are three different types of
 * receivers: Local receivers identified by {@link ChannelID} objects, remote receivers identified by
 * {@link InetAddress} objects and finally checkpoints which are identified by
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class TransferEnvelopeReceiverList {

	private final List<ChannelID> localReceivers;

	private final List<RemoteReceiver> remoteReceivers;

	public TransferEnvelopeReceiverList(final ConnectionInfoLookupResponse cilr) {

		this.localReceivers = cilr.getLocalTargets();
		this.remoteReceivers = cilr.getRemoteTargets();
	}

	public TransferEnvelopeReceiverList(final ChannelContext channelContext) {

		if (channelContext.getType() != ChannelType.INMEMORY) {
			throw new IllegalArgumentException(
				"Transfer envelope receiver lists can only be constructed from in-memory channels.");
		}

		final List<ChannelID> lr = new ArrayList<ChannelID>(1);
		lr.add(channelContext.getConnectedChannelID());

		this.localReceivers = Collections.unmodifiableList(lr);
		this.remoteReceivers = Collections.emptyList();
	}

	public boolean hasLocalReceivers() {

		return (!this.localReceivers.isEmpty());
	}

	public boolean hasRemoteReceivers() {

		return (!this.remoteReceivers.isEmpty());
	}

	public int getTotalNumberOfReceivers() {

		return (this.localReceivers.size() + this.remoteReceivers.size());
	}

	public List<RemoteReceiver> getRemoteReceivers() {

		return this.remoteReceivers;
	}

	public List<ChannelID> getLocalReceivers() {

		return this.localReceivers;
	}
}