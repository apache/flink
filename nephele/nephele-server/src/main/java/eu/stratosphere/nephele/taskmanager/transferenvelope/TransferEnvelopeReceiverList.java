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

package eu.stratosphere.nephele.taskmanager.transferenvelope;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.channels.AbstractChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ConnectionInfoLookupResponse;

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

	private final List<InetSocketAddress> remoteReceivers;

	public TransferEnvelopeReceiverList(final ConnectionInfoLookupResponse cilr) {

		this.localReceivers = Collections.unmodifiableList(cilr.getLocalTargets());

		final List<InetSocketAddress> tmpList = new ArrayList<InetSocketAddress>(cilr.getRemoteTargets().size());

		final Iterator<InstanceConnectionInfo> remoteIt = cilr.getRemoteTargets().iterator();
		while (remoteIt.hasNext()) {
			final InstanceConnectionInfo ici = remoteIt.next();
			tmpList.add(new InetSocketAddress(ici.getAddress(), ici.getDataPort()));
		}

		this.remoteReceivers = Collections.unmodifiableList(tmpList);
	}

	public TransferEnvelopeReceiverList(final AbstractChannel inMemoryChannel) {

		if (inMemoryChannel.getType() != ChannelType.INMEMORY) {
			throw new IllegalArgumentException(
				"Transfer envelope receiver lists can only be constructed from in-memory channels.");
		}

		final List<ChannelID> lr = new ArrayList<ChannelID>(1);
		lr.add(inMemoryChannel.getConnectedChannelID());

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

	public List<InetSocketAddress> getRemoteReceivers() {

		return this.remoteReceivers;
	}

	public List<ChannelID> getLocalReceivers() {

		return this.localReceivers;
	}
}
