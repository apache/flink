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

package eu.stratosphere.nephele.taskmanager.bytebuffered;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.util.EnumUtils;

public class ConnectionInfoLookupResponse implements KryoSerializable {

	public enum ReturnCode {
		NOT_FOUND, FOUND_AND_RECEIVER_READY, FOUND_BUT_RECEIVER_NOT_READY, JOB_IS_ABORTING
	};

	// was request successful?
	private ReturnCode returnCode;

	/**
	 * Contains next-hop instances, this instance must forward multicast transmissions to.
	 */
	private ArrayList<RemoteReceiver> remoteTargets = null;

	/**
	 * Contains local ChannelIDs, multicast packets must be forwarded to.
	 */
	private ArrayList<ChannelID> localTargets = null;

	public ConnectionInfoLookupResponse(final ReturnCode returnCode) {
		this.returnCode = returnCode;
	}

	/**
	 * The default constructor required by kryo.
	 */
	@SuppressWarnings("unused")
	private ConnectionInfoLookupResponse() {
		this.returnCode = null;
	}

	public void addRemoteTarget(final RemoteReceiver remote) {

		if (this.remoteTargets == null) {
			this.remoteTargets = new ArrayList<RemoteReceiver>();
		}

		this.remoteTargets.add(remote);
	}

	public void addLocalTarget(final ChannelID local) {

		if (this.localTargets == null) {
			this.localTargets = new ArrayList<ChannelID>();
		}

		this.localTargets.add(local);
	}

	public List<RemoteReceiver> getRemoteTargets() {

		if (this.remoteTargets == null) {
			return Collections.emptyList();
		}

		return Collections.unmodifiableList(this.remoteTargets);
	}

	public List<ChannelID> getLocalTargets() {

		if (this.localTargets == null) {
			return Collections.emptyList();
		}

		return Collections.unmodifiableList(this.localTargets);
	}

	public boolean receiverNotFound() {

		return (this.returnCode == ReturnCode.NOT_FOUND);
	}

	public boolean receiverNotReady() {

		return (this.returnCode == ReturnCode.FOUND_BUT_RECEIVER_NOT_READY);
	}

	public boolean receiverReady() {

		return (this.returnCode == ReturnCode.FOUND_AND_RECEIVER_READY);
	}

	public boolean isJobAborting() {

		return (this.returnCode == ReturnCode.JOB_IS_ABORTING);
	}

	public static ConnectionInfoLookupResponse createReceiverFoundAndReady(final ChannelID targetChannelID) {

		final ConnectionInfoLookupResponse response = new ConnectionInfoLookupResponse(
			ReturnCode.FOUND_AND_RECEIVER_READY);
		response.addLocalTarget(targetChannelID);

		return response;
	}

	public static ConnectionInfoLookupResponse createReceiverFoundAndReady(final RemoteReceiver remoteReceiver) {

		final ConnectionInfoLookupResponse response = new ConnectionInfoLookupResponse(
			ReturnCode.FOUND_AND_RECEIVER_READY);
		response.addRemoteTarget(remoteReceiver);

		return response;
	}

	/**
	 * Constructor used to generate a plain ConnectionInfoLookupResponse object to be filled with multicast targets.
	 * 
	 * @return
	 */
	public static ConnectionInfoLookupResponse createReceiverFoundAndReady() {

		final ConnectionInfoLookupResponse response = new ConnectionInfoLookupResponse(
			ReturnCode.FOUND_AND_RECEIVER_READY);

		return response;
	}

	public static ConnectionInfoLookupResponse createReceiverNotFound() {
		final ConnectionInfoLookupResponse response = new ConnectionInfoLookupResponse(ReturnCode.NOT_FOUND);

		return response;
	}

	public static ConnectionInfoLookupResponse createReceiverNotReady() {
		final ConnectionInfoLookupResponse response = new ConnectionInfoLookupResponse(
			ReturnCode.FOUND_BUT_RECEIVER_NOT_READY);

		return response;
	}

	public static ConnectionInfoLookupResponse createJobIsAborting() {
		final ConnectionInfoLookupResponse response = new ConnectionInfoLookupResponse(ReturnCode.JOB_IS_ABORTING);

		return response;
	}

	@Override
	public String toString() {
		StringBuilder returnstring = new StringBuilder();
		returnstring.append("local targets (total: " + this.localTargets.size() + "):\n");
		for (ChannelID i : this.localTargets) {
			returnstring.append(i + "\n");
		}
		returnstring.append("remote targets: (total: " + this.remoteTargets.size() + "):\n");
		for (final RemoteReceiver rr : this.remoteTargets) {
			returnstring.append(rr + "\n");
		}
		return returnstring.toString();
	}

	@Override
	public void write(final Kryo kryo, final Output output) {

		EnumUtils.writeEnum(output, this.returnCode);

		kryo.writeObjectOrNull(output, this.remoteTargets, ArrayList.class);
		kryo.writeObjectOrNull(output, this.localTargets, ArrayList.class);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void read(final Kryo kryo, final Input input) {

		this.returnCode = EnumUtils.readEnum(input, ReturnCode.class);

		this.remoteTargets = kryo.readObjectOrNull(input, ArrayList.class);
		this.localTargets = kryo.readObjectOrNull(input, ArrayList.class);
	}
}
