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

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link RemoteAddress} identifies a connection to a remote task manager by
 * the socket address and a connection index. This allows multiple connections
 * to be distinguished by their connection index.
 * <p>
 * The connection index is assigned by the {@link IntermediateResult} and
 * ensures that it is safe to multiplex multiple data transfers over the same
 * physical TCP connection.
 */
public class RemoteAddress implements IOReadableWritable, Serializable {

	private InetSocketAddress address;

	private int connectionIndex;

	public RemoteAddress(InstanceConnectionInfo connectionInfo, int connectionIndex) {
		this(new InetSocketAddress(connectionInfo.address(), connectionInfo.dataPort()), connectionIndex);
	}

	public RemoteAddress(InetSocketAddress address, int connectionIndex) {
		this.address = checkNotNull(address);
		checkArgument(connectionIndex >= 0);
		this.connectionIndex = connectionIndex;
	}

	public InetSocketAddress getAddress() {
		return address;
	}

	public int getConnectionIndex() {
		return connectionIndex;
	}

	@Override
	public int hashCode() {
		return address.hashCode() + (31 * connectionIndex);
	}

	@Override
	public boolean equals(Object other) {
		if (other.getClass() != RemoteAddress.class) {
			return false;
		}

		final RemoteAddress ra = (RemoteAddress) other;
		if (!ra.getAddress().equals(address) || ra.getConnectionIndex() != connectionIndex) {
			return false;
		}

		return true;
	}

	@Override
	public String toString() {
		return address + " [" + connectionIndex + "]";
	}

	// ------------------------------------------------------------------------
	// Serialization
	// ------------------------------------------------------------------------

	public RemoteAddress() {
		this.address = null;
		this.connectionIndex = -1;
	}

	@Override
	public void write(final DataOutputView out) throws IOException {
		final InetAddress ia = address.getAddress();
		out.writeInt(ia.getAddress().length);
		out.write(ia.getAddress());
		out.writeInt(address.getPort());

		out.writeInt(connectionIndex);
	}

	@Override
	public void read(final DataInputView in) throws IOException {
		final byte[] addressBytes = new byte[in.readInt()];
		in.readFully(addressBytes);

		final InetAddress ia = InetAddress.getByAddress(addressBytes);
		int port = in.readInt();

		address = new InetSocketAddress(ia, port);
		connectionIndex = in.readInt();
	}
}
