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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.channels.ChannelID;
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

	private final static class ReadOnlyIterator<T> implements Iterator<T> {

		private final Iterator<T> iterator;

		private ReadOnlyIterator(Iterator<T> iterator) {
			this.iterator = iterator;
		}

		@Override
		public boolean hasNext() {

			return this.iterator.hasNext();
		}

		@Override
		public T next() {

			return this.iterator.next();
		}

		@Override
		public void remove() {
			// Ignore remove calls
			throw new IllegalStateException("remove called on ReadOnlyIterator");
		}
	}

	private final Set<ChannelID> localReceivers = new HashSet<ChannelID>();

	private final Set<InetSocketAddress> remoteReceivers = new HashSet<InetSocketAddress>();

	public TransferEnvelopeReceiverList(final ConnectionInfoLookupResponse cilr) {

		final Iterator<ChannelID> localIt = cilr.getLocalTargets().iterator();
		while (localIt.hasNext()) {
			this.localReceivers.add(localIt.next());
		}

		final Iterator<InstanceConnectionInfo> remoteIt = cilr.getRemoteTargets().iterator();
		while (remoteIt.hasNext()) {
			final InstanceConnectionInfo ici = remoteIt.next();
			this.remoteReceivers.add(new InetSocketAddress(ici.getAddress(), ici.getDataPort()));
		}
	}

	// TODO: Check points

	public boolean hasLocalReceivers() {

		return (!this.localReceivers.isEmpty());
	}

	public boolean hasRemoteReceivers() {

		return (!this.remoteReceivers.isEmpty());
	}

	public int getTotalNumberOfReceivers() {

		return (this.localReceivers.size() + this.remoteReceivers.size());
	}

	public Iterator<InetSocketAddress> getRemoteReceivers() {

		return new ReadOnlyIterator<InetSocketAddress>(this.remoteReceivers.iterator());
	}

	public Iterator<ChannelID> getLocalReceivers() {

		return new ReadOnlyIterator<ChannelID>(this.localReceivers.iterator());
	}
}
