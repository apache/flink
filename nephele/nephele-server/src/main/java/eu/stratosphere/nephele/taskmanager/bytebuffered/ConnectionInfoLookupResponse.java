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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.util.EnumUtils;
import eu.stratosphere.nephele.util.SerializableArrayList;

public class ConnectionInfoLookupResponse implements IOReadableWritable {

	private enum ReturnCode {
		NOT_FOUND, FOUND_AND_RECEIVER_READY, FOUND_BUT_RECEIVER_NOT_READY
	};

	// was request successful?
	private ReturnCode returnCode;

	/**
	 * Contains next-hop instances, this instance must forward multicast transmissions to.
	 */
	private final SerializableArrayList<InstanceConnectionInfo> remoteTargets = new SerializableArrayList<InstanceConnectionInfo>();

	/**
	 * Contains local ChannelIDs, multicast packets must be forwarded to.
	 */
	private final SerializableArrayList<ChannelID> localTargets = new SerializableArrayList<ChannelID>();

	public ConnectionInfoLookupResponse() {
		this.returnCode = ReturnCode.NOT_FOUND;
	}

	public void addRemoteTarget(InstanceConnectionInfo remote) {
		this.remoteTargets.add(remote);
	}

	public void addLocalTarget(ChannelID local) {
		this.localTargets.add(local);
	}

	private void setReturnCode(ReturnCode code) {
		this.returnCode = code;
	}

	public List<InstanceConnectionInfo> getRemoteTargets() {
		return this.remoteTargets;
	}

	public List<ChannelID> getLocalTargets() {
		return this.localTargets;
	}

	@Override
	public void read(DataInput in) throws IOException {

		this.localTargets.read(in);
		this.remoteTargets.read(in);

		this.returnCode = EnumUtils.readEnum(in, ReturnCode.class);
	}

	@Override
	public void write(DataOutput out) throws IOException {

		this.localTargets.write(out);
		this.remoteTargets.write(out);

		EnumUtils.writeEnum(out, this.returnCode);

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

	public static ConnectionInfoLookupResponse createReceiverFoundAndReady(ChannelID targetChannelID) {

		final ConnectionInfoLookupResponse response = new ConnectionInfoLookupResponse();
		response.setReturnCode(ReturnCode.FOUND_AND_RECEIVER_READY);
		response.addLocalTarget(targetChannelID);

		return response;
	}

	public static ConnectionInfoLookupResponse createReceiverFoundAndReady(InstanceConnectionInfo instanceConnectionInfo) {

		final ConnectionInfoLookupResponse response = new ConnectionInfoLookupResponse();
		response.setReturnCode(ReturnCode.FOUND_AND_RECEIVER_READY);
		response.addRemoteTarget(instanceConnectionInfo);

		return response;
	}
	
	/**
	 * Constructor used to generate a plain ConnectionInfoLookupResponse object to be filled with multicast targets.
	 * @return
	 */
	public static ConnectionInfoLookupResponse createReceiverFoundAndReady() {

		final ConnectionInfoLookupResponse response = new ConnectionInfoLookupResponse();
		response.setReturnCode(ReturnCode.FOUND_AND_RECEIVER_READY);

		return response;
	}	

	public static ConnectionInfoLookupResponse createReceiverNotFound() {
		final ConnectionInfoLookupResponse response = new ConnectionInfoLookupResponse();
		response.setReturnCode(ReturnCode.NOT_FOUND);

		return response;
	}

	public static ConnectionInfoLookupResponse createReceiverNotReady() {
		final ConnectionInfoLookupResponse response = new ConnectionInfoLookupResponse();
		response.setReturnCode(ReturnCode.FOUND_BUT_RECEIVER_NOT_READY);

		return response;
	}
}
