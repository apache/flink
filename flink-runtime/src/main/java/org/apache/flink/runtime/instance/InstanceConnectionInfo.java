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
 * This class encapsulates all connection information necessary to connect to the instance's task manager.
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
	 * The hostname
	 */
	private String hostName;
	
	/**
	 * This flag indicates if the FQDN hostname cound not be resolved and is represented
	 * as an IP address (string).
	 */
	private boolean fqdnHostNameIsIP = false;


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
		} catch (Throwable t) {
			LOG.warn("Unable to determine hostname for TaskManager. The performance might be degraded since HDFS input split assignment is not possible");
			if(LOG.isDebugEnabled()) {
				LOG.debug("getCanonicalHostName() Exception", t);
			}
			// could not determine host name, so take IP textual representation
			this.fqdnHostName = inetAddress.getHostAddress();
			this.fqdnHostNameIsIP = true;
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
	 * Returns the host name of the instance. If the host name could not be determined, the return value will be a
	 * textual representation of the instance's IP address.
	 * 
	 * @return the host name of the instance
	 */
	public String getFQDNHostname() {
		return this.fqdnHostName;
	}
	
	public String getHostname() {
		if(hostName == null) {
			String fqdn = getFQDNHostname();
			if(this.fqdnHostNameIsIP) { // fqdn to hostname translation is pointless if FQDN is an ip address.
				hostName = fqdn;
			} else {
				hostName = NetUtils.getHostnameFromFQDN(fqdn);
			}
		}
		return hostName;
	}

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
		this.fqdnHostNameIsIP = in.readBoolean();

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
		out.writeBoolean(fqdnHostNameIsIP);
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
