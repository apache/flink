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

package org.apache.flink.runtime.io.network;

import java.io.Serializable;

import org.apache.flink.runtime.io.network.channels.ChannelID;

public class ConnectionInfoLookupResponse implements Serializable {

	private static final long serialVersionUID = 3961171754642077522L;

	
	private enum ReturnCode {
		NOT_FOUND, FOUND_AND_RECEIVER_READY, FOUND_BUT_RECEIVER_NOT_READY, JOB_IS_ABORTING
	};

	// was request successful?
	private ReturnCode returnCode;
	
	private RemoteReceiver remoteTarget;

	private ChannelID localTarget;
	
	public ConnectionInfoLookupResponse() {}

	public ConnectionInfoLookupResponse(ReturnCode code) {
		this.returnCode = code;
		this.remoteTarget = null;
		this.localTarget = null;
	}
	
	public ConnectionInfoLookupResponse(ReturnCode code, ChannelID localTarget) {
		this.returnCode = code;
		this.remoteTarget = null;
		this.localTarget = localTarget;
	}
	
	public ConnectionInfoLookupResponse(ReturnCode code, RemoteReceiver receiver) {
		this.returnCode = code;
		this.remoteTarget = receiver;
		this.localTarget = null;
	}

	public RemoteReceiver getRemoteTarget() {
		return this.remoteTarget;
	}

	public ChannelID getLocalTarget() {
		return this.localTarget;
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

	
	public static ConnectionInfoLookupResponse createReceiverFoundAndReady(ChannelID targetChannelID) {
		return new ConnectionInfoLookupResponse(ReturnCode.FOUND_AND_RECEIVER_READY, targetChannelID);
	}

	public static ConnectionInfoLookupResponse createReceiverFoundAndReady(RemoteReceiver remoteReceiver) {
		return new ConnectionInfoLookupResponse(ReturnCode.FOUND_AND_RECEIVER_READY, remoteReceiver);
	}

	public static ConnectionInfoLookupResponse createReceiverNotFound() {
		return new ConnectionInfoLookupResponse(ReturnCode.NOT_FOUND);
	}

	public static ConnectionInfoLookupResponse createReceiverNotReady() {
		return new ConnectionInfoLookupResponse(ReturnCode.FOUND_BUT_RECEIVER_NOT_READY);
	}

	public static ConnectionInfoLookupResponse createJobIsAborting() {
		return new ConnectionInfoLookupResponse(ReturnCode.JOB_IS_ABORTING);
	}

	
	@Override
	public String toString() {
		return this.returnCode.name() + ", local target: " + this.localTarget + ", remoteTarget: " + this.remoteTarget;
	}
}
