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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.util.NetUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class encapsulates the connection information of a TaskManager.
 * It describes the host where the TaskManager operates and its server port
 * for data exchange. This class also contains utilities to work with the
 * TaskManager's host name, which is used to localize work assignments.
 */
public class TaskManagerLocation implements Comparable<TaskManagerLocation>, java.io.Serializable {

	private static final long serialVersionUID = -8254407801276350716L;

	private static final Logger LOG = LoggerFactory.getLogger(TaskManagerLocation.class);

	// ------------------------------------------------------------------------

	/** The ID of the resource in which the TaskManager is started. This can be for example
	 * the YARN container ID, Mesos container ID, or any other unique identifier. */
	private final ResourceID resourceID;

	/** The network address that the TaskManager binds its sockets to */
	private final InetAddress inetAddress;

	/** The fully qualified host name of the TaskManager */
	private final String fqdnHostName;

	/** The pure hostname, derived from the fully qualified host name. */
	private final String hostName;
	
	/** The port that the TaskManager receive data transport connection requests at */
	private final int dataPort;

	/** The toString representation, eagerly constructed and cached to avoid repeated string building */  
	private final String stringRepresentation;

	/**
	 * Constructs a new instance connection info object. The constructor will attempt to retrieve the instance's
	 * host name and domain name through the operating system's lookup mechanisms.
	 * 
	 * @param inetAddress
	 *        the network address the instance's task manager binds its sockets to
	 * @param dataPort
	 *        the port instance's task manager expects to receive transfer envelopes on
	 */
	@VisibleForTesting
	public TaskManagerLocation(ResourceID resourceID, InetAddress inetAddress, int dataPort) {
		// -1 indicates a local instance connection info
		checkArgument(dataPort > 0 || dataPort == -1, "dataPort must be > 0, or -1 (local)");

		this.resourceID = checkNotNull(resourceID);
		this.inetAddress = checkNotNull(inetAddress);
		this.dataPort = dataPort;

		// get FQDN hostname on this TaskManager.
		this.fqdnHostName = getFqdnHostName(inetAddress);

		this.hostName = getHostName(inetAddress);

		this.stringRepresentation = String.format(
				"%s @ %s (dataPort=%d)", resourceID, fqdnHostName, dataPort);
	}

	public static TaskManagerLocation fromUnresolvedLocation(final UnresolvedTaskManagerLocation unresolvedLocation)
		throws UnknownHostException {
		return new TaskManagerLocation(
			unresolvedLocation.getResourceID(),
			InetAddress.getByName(unresolvedLocation.getExternalAddress()),
			unresolvedLocation.getDataPort());
	}

	// ------------------------------------------------------------------------
	//  Getters
	// ------------------------------------------------------------------------

	/**
	 * Gets the ID of the resource in which the TaskManager is started. The format of this depends
	 * on how the TaskManager is started:
	 * <ul>
	 *     <li>If the TaskManager is started via YARN, this is the YARN container ID.</li>
	 *     <li>If the TaskManager is started via Mesos, this is the Mesos container ID.</li>
	 *     <li>If the TaskManager is started in standalone mode, or via a MiniCluster, this is a random ID.</li>
	 *     <li>Other deployment modes can set the resource ID in other ways.</li>
	 * </ul>
	 * 
	 * @return The ID of the resource in which the TaskManager is started
	 */
	public ResourceID getResourceID() {
		return resourceID;
	}

	/**
	 * Returns the port instance's task manager expects to receive transfer envelopes on.
	 * 
	 * @return the port instance's task manager expects to receive transfer envelopes on
	 */
	public int dataPort() {
		return dataPort;
	}

	/**
	 * Returns the network address the instance's task manager binds its sockets to.
	 * 
	 * @return the network address the instance's task manager binds its sockets to
	 */
	public InetAddress address() {
		return inetAddress;
	}

	/**
	 * Gets the IP address where the TaskManager operates.
	 *
	 * @return The IP address.
	 */
	public String addressString() {
		return inetAddress.toString();
	}

	/**
	 * Returns the fully-qualified domain name the TaskManager. If the name could not be
	 * determined, the return value will be a textual representation of the TaskManager's IP address.
	 * 
	 * @return The fully-qualified domain name of the TaskManager.
	 */
	public String getFQDNHostname() {
		return fqdnHostName;
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
	 * Gets the fully qualified hostname of the TaskManager based on the network address.
	 *
	 * @param inetAddress the network address that the TaskManager binds its sockets to
	 * @return fully qualified hostname of the TaskManager
	 */
	private static String getFqdnHostName(InetAddress inetAddress) {
		String fqdnHostName;
		try {
			fqdnHostName = inetAddress.getCanonicalHostName();
		} catch (Throwable t) {
			LOG.warn("Unable to determine the canonical hostname. Input split assignment (such as " +
				"for HDFS files) may be non-local when the canonical hostname is missing.");
			LOG.debug("getCanonicalHostName() Exception:", t);
			fqdnHostName = inetAddress.getHostAddress();
		}

		return fqdnHostName;
	}

	/**
	 * Gets the hostname of the TaskManager based on the network address.
	 *
	 * @param inetAddress the network address that the TaskManager binds its sockets to
	 * @return hostname of the TaskManager
	 */
	public static String getHostName(InetAddress inetAddress) {
		String hostName;
		String fqdnHostName = getFqdnHostName(inetAddress);

		if (fqdnHostName.equals(inetAddress.getHostAddress())) {
			// this happens when the name lookup fails, either due to an exception,
			// or because no hostname can be found for the address
			// take IP textual representation
			hostName = fqdnHostName;
			LOG.warn("No hostname could be resolved for the IP address {}, using IP address as host name. "
				+ "Local input split assignment (such as for HDFS files) may be impacted.", inetAddress.getHostAddress());
		} else {
			hostName = NetUtils.getHostnameFromFQDN(fqdnHostName);
		}

		return hostName;
	}

	// --------------------------------------------------------------------------------------------
	// Utilities
	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return stringRepresentation;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		else if (obj != null && obj.getClass() == TaskManagerLocation.class) {
			TaskManagerLocation that = (TaskManagerLocation) obj;
			return this.resourceID.equals(that.resourceID) &&
					this.inetAddress.equals(that.inetAddress) &&
					this.dataPort == that.dataPort;
		}
		else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return resourceID.hashCode() + 
				17 * inetAddress.hashCode() +
				129 * dataPort;
	}

	@Override
	public int compareTo(@Nonnull TaskManagerLocation o) {
		// decide based on resource ID first
		int resourceIdCmp = this.resourceID.getResourceIdString().compareTo(o.resourceID.getResourceIdString());
		if (resourceIdCmp != 0) {
			return resourceIdCmp;
		}

		// decide based on ip address next
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
