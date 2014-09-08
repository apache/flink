/**
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
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Objects of this class uniquely identify a connection to a remote {@link TaskManager}.
 * 
 */
public final class RemoteReceiver implements IOReadableWritable {

	/**
	 * The address of the connection to the remote {@link TaskManager}.
	 */
	private InetSocketAddress connectionAddress;

	/**
	 * The index of the connection to the remote {@link TaskManager}.
	 */
	private int connectionIndex;

	/**
	 * Constructs a new remote receiver object.
	 * 
	 * @param connectionAddress
	 *        the address of the connection to the remote {@link TaskManager}
	 * @param connectionIndex
	 *        the index of the connection to the remote {@link TaskManager}
	 */
	public RemoteReceiver(final InetSocketAddress connectionAddress, final int connectionIndex) {
		if (connectionAddress == null) {
			throw new IllegalArgumentException("Argument connectionAddress must not be null");
		}
		if (connectionIndex < 0) {
			throw new IllegalArgumentException("Argument connectionIndex must be a non-negative integer number");
		}

		this.connectionAddress = connectionAddress;
		this.connectionIndex = connectionIndex;
	}

	/**
	 * Default constructor for serialization/deserialization.
	 */
	public RemoteReceiver() {
		this.connectionAddress = null;
		this.connectionIndex = -1;
	}

	/**
	 * Returns the address of the connection to the remote {@link TaskManager}.
	 * 
	 * @return the address of the connection to the remote {@link TaskManager}
	 */
	public InetSocketAddress getConnectionAddress() {
		return this.connectionAddress;
	}

	/**
	 * Returns the index of the connection to the remote {@link TaskManager}.
	 * 
	 * @return the index of the connection to the remote {@link TaskManager}
	 */
	public int getConnectionIndex() {
		return this.connectionIndex;
	}


	@Override
	public int hashCode() {
		return this.connectionAddress.hashCode() + (31 * this.connectionIndex);
	}


	@Override
	public boolean equals(final Object obj) {

		if (!(obj instanceof RemoteReceiver)) {
			return false;
		}

		final RemoteReceiver rr = (RemoteReceiver) obj;
		if (!this.connectionAddress.equals(rr.connectionAddress)) {
			return false;
		}

		if (this.connectionIndex != rr.connectionIndex) {
			return false;
		}

		return true;
	}


	@Override
	public void write(final DataOutputView out) throws IOException {

		final InetAddress ia = this.connectionAddress.getAddress();
		out.writeInt(ia.getAddress().length);
		out.write(ia.getAddress());
		out.writeInt(this.connectionAddress.getPort());

		out.writeInt(this.connectionIndex);
	}


	@Override
	public void read(final DataInputView in) throws IOException {
		final int addr_length = in.readInt();
		final byte[] address = new byte[addr_length];
		in.readFully(address);

		InetAddress ia = InetAddress.getByAddress(address);
		int port = in.readInt();
		this.connectionAddress = new InetSocketAddress(ia, port);

		this.connectionIndex = in.readInt();
	}


	@Override
	public String toString() {
		return this.connectionAddress + " (" + this.connectionIndex + ")";
	}
}