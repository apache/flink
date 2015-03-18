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

package org.apache.flink.runtime.instance;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class encapsulates the connection information of a TaskManager.
 * It describes the host where the TaskManager operates and its server port
 * for data exchange. This class also contains utilities to work with the
 * TaskManager's host name, which is used to localize work assignments.
 */
public class InstanceConnectionInfo implements IOReadableWritable, Comparable<InstanceConnectionInfo>, java.io.Serializable {

	private static final long serialVersionUID = -8254407801276350716L;
	
	private static final Logger LOG = LoggerFactory.getLogger(InstanceConnectionInfo.class);
	

	/**
	 * The network address the instance's task manager binds its sockets to.
	 */
	private InetAddress inetAddress;

	/**
	 * The port the instance's task manager expects to receive transfer envelopes on.
	 */
	private int dataPort;

	/**
	 * The fully qualified host name of the instance.
	 */
	private String fqdnHostName;
	
	/**
	 * The hostname, derived from the fully qualified host name.
	 */
	private String hostName;


	/**
	 * Constructs a new instance connection info object. The constructor will attempt to retrieve the instance's
	 * host name and domain name through the operating system's lookup mechanisms.
	 * 
	 * @param inetAddress
	 *        the network address the instance's task manager binds its sockets to
	 * @param dataPort
	 *        the port instance's task manager expects to receive transfer envelopes on
	 */
	public InstanceConnectionInfo(InetAddress inetAddress, int dataPort) {
		if (inetAddress == null) {
			throw new IllegalArgumentException("Argument inetAddress must not be null");
		}
		if (dataPort <= 0) {
			throw new IllegalArgumentException("Argument dataPort must be greater than zero");
		}

		this.dataPort = dataPort;
		this.inetAddress = inetAddress;
		
		// get FQDN hostname on this TaskManager.
		try {
			this.fqdnHostName = this.inetAddress.getCanonicalHostName();
		}
		catch (Throwable t) {
			LOG.warn("Unable to determine the canonical hostname. Input split assignment (such as " +
					"for HDFS files) may be non-local when the canonical hostname is missing.");
			LOG.debug("getCanonicalHostName() Exception:", t);
			this.fqdnHostName = this.inetAddress.getHostAddress();
		}

		if (this.fqdnHostName.equals(this.inetAddress.getHostAddress())) {
			// this happens when the name lookup fails, either due to an exception,
			// or because no hostname can be found for the address
			// take IP textual representation
			this.hostName = this.fqdnHostName;
			LOG.warn("No hostname could be resolved for the IP address {}, using IP address as host name. "
					+ "Local input split assignment (such as for HDFS files) may be impacted.");
		}
		else {
			this.hostName = NetUtils.getHostnameFromFQDN(this.fqdnHostName);
		}
	}

	/**
	 * Constructs an empty object.
	 */
	public InstanceConnectionInfo() {}


	/**
	 * Returns the port instance's task manager expects to receive transfer envelopes on.
	 * 
	 * @return the port instance's task manager expects to receive transfer envelopes on
	 */
	public int dataPort() {
		return this.dataPort;
	}

	/**
	 * Returns the network address the instance's task manager binds its sockets to.
	 * 
	 * @return the network address the instance's task manager binds its sockets to
	 */
	public InetAddress address() {
		return this.inetAddress;
	}

	/**
	 * Returns the fully-qualified domain name the TaskManager. If the name could not be
	 * determined, the return value will be a textual representation of the TaskManager's IP address.
	 * 
	 * @return The fully-qualified domain name of the TaskManager.
	 */
	public String getFQDNHostname() {
		return this.fqdnHostName;
	}

	/**
	 * Gets the hostname of the TaskManager. The hostname derives from the fully qualified
	 * domain name (FQDN, see {@link #getFQDNHostname()}):
	 * <ul>
	 *     <li>If the FQDN is the textual IP address, then the hostname is also the IP address</li>
	 *     <li>If the FQDN has only one segment (such as "localhost", or "host17"), then this is
	 *         used as the hostname.</li>
	 *     <li>If the FQDN has multiple segments (such as "worker3.subgroup.company.net"), then the first
	 *         segment (here "worker3") will be used as the hostname.</li>
	 * </ul>
	 *
	 * @return The hostname of the TaskManager.
	 */
	public String getHostname() {
		return hostName;
	}

	/**
	 * Gets the IP address where the TaskManager operates.
	 *
	 * @return The IP address.
	 */
	public String getInetAdress() {
		return this.inetAddress.toString();
	}

	// --------------------------------------------------------------------------------------------
	// Serialization
	// --------------------------------------------------------------------------------------------

	@Override
	public void read(DataInputView in) throws IOException {

		final int addr_length = in.readInt();
		byte[] address = new byte[addr_length];
		in.readFully(address);
		
		this.dataPort = in.readInt();
		
		this.fqdnHostName = StringUtils.readNullableString(in);
		this.hostName = StringUtils.readNullableString(in);

		try {
			this.inetAddress = InetAddress.getByAddress(address);
		} catch (UnknownHostException e) {
			throw new IOException("This lookup should never fail.", e);
		}
	}


	@Override
	public void write(final DataOutputView out) throws IOException {
		out.writeInt(this.inetAddress.getAddress().length);
		out.write(this.inetAddress.getAddress());
		
		out.writeInt(this.dataPort);
		
		StringUtils.writeNullableString(fqdnHostName, out);
		StringUtils.writeNullableString(hostName, out);
	}

	// --------------------------------------------------------------------------------------------
	// Utilities
	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return getFQDNHostname() + " (dataPort=" + dataPort + ")";
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof InstanceConnectionInfo) {
			InstanceConnectionInfo other = (InstanceConnectionInfo) obj;
			return this.dataPort == other.dataPort &&
					this.inetAddress.equals(other.inetAddress);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return this.inetAddress.hashCode() +
				17*dataPort;
	}

	@Override
	public int compareTo(InstanceConnectionInfo o) {
		// decide based on address first
		byte[] thisAddress = this.inetAddress.getAddress();
		byte[] otherAddress = o.inetAddress.getAddress();
		
		if (thisAddress.length < otherAddress.length) {
			return -1;
		} else if (thisAddress.length > otherAddress.length) {
			return 1;
		} else {
			for (int i = 0; i < thisAddress.length; i++) {
				byte tb = thisAddress[i];
				byte ob = otherAddress[i];
				if (tb < ob) {
					return -1;
				} else if (tb > ob) {
					return 1;
				}
			}
		}
		
		// addresses are identical, decide based on ports.
		if (this.dataPort < o.dataPort) {
			return -1;
		} else if (this.dataPort > o.dataPort) {
			return 1;
		} else {
			return 0;
		}
	}
}
