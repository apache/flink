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

package eu.stratosphere.runtime.io.network;

import java.net.InetAddress;

import eu.stratosphere.runtime.io.channels.ChannelID;
import eu.stratosphere.runtime.io.network.ConnectionInfoLookupResponse;
import eu.stratosphere.runtime.io.network.RemoteReceiver;

/**
 * A transfer envelope receiver list contains all recipients of a transfer envelope. Their are three different types of
 * receivers: Local receivers identified by {@link ChannelID} objects, remote receivers identified by
 * {@link InetAddress} objects and finally checkpoints which are identified by
 * <p>
 * This class is thread-safe.
 * 
 */
public class EnvelopeReceiverList {

	private final ChannelID localReceiver;

	private final RemoteReceiver remoteReceiver;

	public EnvelopeReceiverList(ConnectionInfoLookupResponse cilr) {
		this.localReceiver = cilr.getLocalTarget();
		this.remoteReceiver = cilr.getRemoteTarget();
	}

	public EnvelopeReceiverList(ChannelID localReceiver) {
		this.localReceiver = localReceiver;
		this.remoteReceiver = null;
	}

	public EnvelopeReceiverList(RemoteReceiver remoteReceiver) {
		this.localReceiver = null;
		this.remoteReceiver = remoteReceiver;
	}

	public boolean hasLocalReceiver() {
		return this.localReceiver != null;
	}

	public boolean hasRemoteReceiver() {
		return this.remoteReceiver != null;
	}

	public int getTotalNumberOfReceivers() {
		return (this.localReceiver == null ? 0 : 1) + (this.remoteReceiver == null ? 0 : 1);
	}

	public RemoteReceiver getRemoteReceiver() {
		return this.remoteReceiver;
	}

	public ChannelID getLocalReceiver() {
		return this.localReceiver;
	}
	
	@Override
	public String toString() {
		return "local receiver: " + this.localReceiver + ", remote receiver: " + this.remoteReceiver;
	}
}
