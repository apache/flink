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

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.util.EnumUtils;

public class ConnectionInfoLookupResponse implements IOReadableWritable {

	private enum ReturnCode {
		NOT_FOUND, FOUND_AND_RECEIVER_READY, FOUND_BUT_RECEIVER_NOT_READY
	};

	private ReturnCode returnCode;

	private InstanceConnectionInfo instanceConnectionInfo;

	public ConnectionInfoLookupResponse(ReturnCode returnCode, InstanceConnectionInfo instanceConnectionInfo) {
		this.returnCode = returnCode;
		this.instanceConnectionInfo = instanceConnectionInfo;
	}

	public ConnectionInfoLookupResponse() {
		this.returnCode = ReturnCode.NOT_FOUND;
		this.instanceConnectionInfo = null;
	}

	public InstanceConnectionInfo getInstanceConnectionInfo() {
		return this.instanceConnectionInfo;
	}

	@Override
	public void read(DataInput in) throws IOException {

		if (in.readBoolean()) {
			this.instanceConnectionInfo = new InstanceConnectionInfo();
			this.instanceConnectionInfo.read(in);
		} else {
			this.instanceConnectionInfo = null;
		}

		this.returnCode = EnumUtils.readEnum(in, ReturnCode.class);
	}

	@Override
	public void write(DataOutput out) throws IOException {

		if (this.instanceConnectionInfo != null) {
			out.writeBoolean(true);
			this.instanceConnectionInfo.write(out);
		} else {
			out.writeBoolean(false);
		}

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

	public static ConnectionInfoLookupResponse createReceiverFoundAndReady(InstanceConnectionInfo instanceConnectionInfo) {
		return new ConnectionInfoLookupResponse(ReturnCode.FOUND_AND_RECEIVER_READY, instanceConnectionInfo);
	}

	public static ConnectionInfoLookupResponse createReceiverNotFound() {
		return new ConnectionInfoLookupResponse(ReturnCode.NOT_FOUND, null);
	}

	public static ConnectionInfoLookupResponse createReceiverNotReady() {
		return new ConnectionInfoLookupResponse(ReturnCode.FOUND_BUT_RECEIVER_NOT_READY, null);
	}
}
