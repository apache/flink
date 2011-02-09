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

package eu.stratosphere.nephele.instance;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * This class encapsulates all connection information necessary to
 * connect to the instance's task manager.
 * 
 * @author warneke
 */
public class InstanceConnectionInfo implements IOReadableWritable {

	/**
	 * The network address the instance's task manager binds its sockets to.
	 */
	private InetAddress inetAddress = null;

	/**
	 * The port the instance's task manager runs its IPC service on.
	 */
	private int ipcPort = 0;

	/**
	 * The port the instance's task manager expects to receive transfer envelopes on.
	 */
	private int dataPort = 0;

	/**
	 * The host name of the instance.
	 */
	private String hostname = null;

	/**
	 * Constructs a new instance connection info object.
	 * 
	 * @param inetAddress
	 *        the network address the instance's task manager binds its sockets to.
	 * @param ipcPort
	 *        the port instance's task manager runs its IPC service on
	 * @param dataPort
	 *        the port instance's task manager expects to receive transfer envelopes on.
	 */
	public InstanceConnectionInfo(InetAddress inetAddress, int ipcPort, int dataPort) {

		this.inetAddress = inetAddress;
		this.hostname = inetAddress.getHostName();
		this.ipcPort = ipcPort;
		this.dataPort = dataPort;
	}

	/**
	 * Constructs a new instance connection info object.
	 * 
	 * @param inetAddress
	 *        the network address the instance's task manager binds its sockets to.
	 * @param ipcPort
	 *        the port instance's task manager runs its IPC service on
	 * @param dataPort
	 *        the port instance's task manager expects to receive transfer envelopes on.
	 * @param hostname
	 *        the host name of the instance
	 */
	public InstanceConnectionInfo(InetAddress inetAddress, int ipcPort, int dataPort, String hostname) {

		this.inetAddress = inetAddress;
		this.hostname = inetAddress.getHostName();
		this.ipcPort = ipcPort;
		this.dataPort = dataPort;
	}

	/**
	 * Constructs an empty {@link InstanceConnectionInfo} object.
	 */
	public InstanceConnectionInfo() {
	}

	/**
	 * Returns the port instance's task manager runs its IPC service on.
	 * 
	 * @return the port instance's task manager runs its IPC service on
	 */
	public int getIPCPort() {
		return this.ipcPort;
	}

	/**
	 * Returns the port instance's task manager expects to receive transfer envelopes on.
	 * 
	 * @return the port instance's task manager expects to receive transfer envelopes on
	 */
	public int getDataPort() {
		return this.dataPort;
	}

	/**
	 * Returns the network address the instance's task manager binds its sockets to.
	 * 
	 * @return the network address the instance's task manager binds its sockets to
	 */
	public InetAddress getAddress() {

		return this.inetAddress;
	}

	/**
	 * Returns the host name of the instance.
	 * 
	 * @return the host name of the instance
	 */
	public String getHostName() {

		return this.hostname;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(DataInput in) throws IOException {

		final int addr_length = in.readInt();
		byte[] address = new byte[addr_length];
		in.readFully(address);
		this.hostname = StringRecord.readString(in);

		try {
			this.inetAddress = InetAddress.getByAddress(address);
		} catch (UnknownHostException uhe) {
			throw new IOException(StringUtils.stringifyException(uhe));
		}

		this.ipcPort = in.readInt();
		this.dataPort = in.readInt();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException {

		out.writeInt(this.inetAddress.getAddress().length);
		out.write(this.inetAddress.getAddress());
		StringRecord.writeString(out, this.hostname);
		out.writeInt(this.ipcPort);
		out.writeInt(this.dataPort);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		String iaString;
		if (this.hostname != null) {
			iaString = this.hostname;
		} else {
			iaString = inetAddress.toString();
			iaString = iaString.replace("/", "");
		}

		return iaString;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object obj) {

		if (obj instanceof InstanceConnectionInfo) {

			InstanceConnectionInfo ici = (InstanceConnectionInfo) obj;
			if (!this.inetAddress.equals(ici.getAddress())) {
				return false;
			}

			if (this.ipcPort != ici.getIPCPort()) {
				return false;
			}

			if (this.dataPort != ici.getDataPort()) {
				return false;
			}

			return true;
		}

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {

		return this.inetAddress.hashCode();
	}
}
