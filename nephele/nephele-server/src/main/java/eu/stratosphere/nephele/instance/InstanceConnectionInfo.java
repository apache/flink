/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import java.net.InetAddress;

/**
 * This class encapsulates all connection information necessary to connect to the instance's task manager.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class InstanceConnectionInfo implements Comparable<InstanceConnectionInfo> {

	/**
	 * The network address the instance's task manager binds its sockets to.
	 */
	private final InetAddress inetAddress;

	/**
	 * The port the instance's task manager runs its IPC service on.
	 */
	private final int ipcPort;

	/**
	 * The port the instance's task manager expects to receive transfer envelopes on.
	 */
	private final int dataPort;

	/**
	 * The host name of the instance.
	 */
	private final String hostName;

	/**
	 * The domain name of the instance.
	 */
	private final String domainName;

	/**
	 * Constructs a new instance connection info object. The constructor will attempt to retrieve the instance's
	 * hostname and domain name through the operating system's lookup mechanisms.
	 * 
	 * @param inetAddress
	 *        the network address the instance's task manager binds its sockets to
	 * @param ipcPort
	 *        the port instance's task manager runs its IPC service on
	 * @param dataPort
	 *        the port instance's task manager expects to receive transfer envelopes on
	 */
	public InstanceConnectionInfo(final InetAddress inetAddress, final int ipcPort, final int dataPort) {

		if (inetAddress == null) {
			throw new IllegalArgumentException("Argument inetAddress must not be null");
		}

		if (ipcPort <= 0) {
			throw new IllegalArgumentException("Argument ipcPort must be greater than zero");
		}

		if (dataPort <= 0) {
			throw new IllegalArgumentException("Argument dataPort must be greater than zero");
		}

		this.inetAddress = inetAddress;

		final String hostAddStr = inetAddress.getHostAddress();
		final String fqdn = inetAddress.getCanonicalHostName();

		if (hostAddStr.equals(fqdn)) {
			this.hostName = fqdn;
			this.domainName = null;
		} else {

			// Look for the first dot in the FQDN
			final int firstDot = fqdn.indexOf('.');
			if (firstDot == -1) {
				this.hostName = fqdn;
				this.domainName = null;
			} else {
				this.hostName = fqdn.substring(0, firstDot);
				this.domainName = fqdn.substring(firstDot + 1);
			}
		}

		this.ipcPort = ipcPort;
		this.dataPort = dataPort;
	}

	/**
	 * Constructs a new instance connection info object.
	 * 
	 * @param inetAddress
	 *        the network address the instance's task manager binds its sockets to
	 * @param hostName
	 *        the host name of the instance
	 * @param domainName
	 *        the domain name of the instance
	 * @param ipcPort
	 *        the port instance's task manager runs its IPC service on
	 * @param dataPort
	 *        the port instance's task manager expects to receive transfer envelopes on.
	 */
	public InstanceConnectionInfo(final InetAddress inetAddress, final String hostName, final String domainName,
			final int ipcPort, final int dataPort) {

		if (inetAddress == null) {
			throw new IllegalArgumentException("Argument inetAddress must not be null");
		}

		if (hostName == null) {
			throw new IllegalArgumentException("Argument hostName must not be null");
		}

		if (ipcPort <= 0) {
			throw new IllegalArgumentException("Argument ipcPort must be greater than zero");
		}

		if (dataPort <= 0) {
			throw new IllegalArgumentException("Argument dataPort must be greater than zero");
		}

		this.inetAddress = inetAddress;
		this.hostName = hostName;
		this.domainName = domainName;
		this.ipcPort = ipcPort;
		this.dataPort = dataPort;
	}

	/**
	 * Default constructor required by kryo.
	 */
	@SuppressWarnings("unused")
	private InstanceConnectionInfo() {
		this.inetAddress = null;
		this.hostName = null;
		this.domainName = null;
		this.ipcPort = 0;
		this.dataPort = 0;
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
	 * Returns the host name of the instance. If the host name could not be determined, the return value will be a
	 * textual representation of the instance's IP address.
	 * 
	 * @return the host name of the instance
	 */
	public String getHostName() {

		return this.hostName;
	}

	/**
	 * Returns the domain name of the instance.
	 * 
	 * @return the domain name of the instance or <code>null</code> if the domain name could not be determined
	 */
	public String getDomainName() {

		return this.domainName;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		String iaString;
		if (this.hostName != null) {
			iaString = this.hostName;
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
	public boolean equals(final Object obj) {

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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compareTo(final InstanceConnectionInfo o) {

		return this.getAddress().getHostName()
			.compareTo(((InstanceConnectionInfo) o).getAddress().getHostName());
	}

}
