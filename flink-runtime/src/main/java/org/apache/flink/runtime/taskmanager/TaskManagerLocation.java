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

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class encapsulates the connection information of a TaskManager. It describes the host where
 * the TaskManager operates and its server port for data exchange. This class also contains
 * utilities to work with the TaskManager's host name, which is used to localize work assignments.
 */
public class TaskManagerLocation implements Comparable<TaskManagerLocation>, java.io.Serializable {

    private static final long serialVersionUID = -8254407801276350716L;

    private static final Logger LOG = LoggerFactory.getLogger(TaskManagerLocation.class);

    // ------------------------------------------------------------------------

    /**
     * The ID of the resource in which the TaskManager is started. This can be for example the YARN
     * container ID, Mesos container ID, or any other unique identifier.
     */
    private final ResourceID resourceID;

    /** The network address that the TaskManager binds its sockets to. */
    private final InetAddress inetAddress;

    /** The supplier for fully qualified host name and pure hostname. */
    private final HostNameSupplier hostNameSupplier;

    /** The port that the TaskManager receive data transport connection requests at. */
    private final int dataPort;

    /**
     * The toString representation, eagerly constructed and cached to avoid repeated string
     * building.
     */
    private String stringRepresentation;

    /**
     * Constructs a new instance connection info object. The constructor will attempt to retrieve
     * the instance's host name and domain name through the operating system's lookup mechanisms.
     *
     * @param inetAddress the network address the instance's task manager binds its sockets to
     * @param dataPort the port instance's task manager expects to receive transfer envelopes on
     * @param hostNameSupplier the supplier for obtaining fully-qualified domain name and pure
     *     hostname of the task manager
     */
    @VisibleForTesting
    public TaskManagerLocation(
            ResourceID resourceID,
            InetAddress inetAddress,
            int dataPort,
            HostNameSupplier hostNameSupplier) {
        // -1 indicates a local instance connection info
        checkArgument(dataPort > 0 || dataPort == -1, "dataPort must be > 0, or -1 (local)");

        this.resourceID = checkNotNull(resourceID);
        this.inetAddress = checkNotNull(inetAddress);
        this.dataPort = dataPort;
        this.hostNameSupplier = checkNotNull(hostNameSupplier);
    }

    /**
     * Constructs a new instance connection info object. The constructor will attempt to retrieve
     * the instance's host name and domain name through the operating system's lookup mechanisms.
     *
     * @param inetAddress the network address the instance's task manager binds its sockets to
     * @param dataPort the port instance's task manager expects to receive transfer envelopes on
     */
    @VisibleForTesting
    public TaskManagerLocation(ResourceID resourceID, InetAddress inetAddress, int dataPort) {
        this(resourceID, inetAddress, dataPort, new DefaultHostNameSupplier(inetAddress));
    }

    public static TaskManagerLocation fromUnresolvedLocation(
            final UnresolvedTaskManagerLocation unresolvedLocation,
            final ResolutionMode resolutionMode)
            throws UnknownHostException {

        InetAddress inetAddress = InetAddress.getByName(unresolvedLocation.getExternalAddress());

        switch (resolutionMode) {
            case RETRIEVE_HOST_NAME:
                return new TaskManagerLocation(
                        unresolvedLocation.getResourceID(),
                        inetAddress,
                        unresolvedLocation.getDataPort(),
                        new DefaultHostNameSupplier(inetAddress));
            case USE_IP_ONLY:
                return new TaskManagerLocation(
                        unresolvedLocation.getResourceID(),
                        inetAddress,
                        unresolvedLocation.getDataPort(),
                        new IpOnlyHostNameSupplier(inetAddress));
            default:
                throw new UnsupportedOperationException("Unsupported resolution mode provided.");
        }
    }

    // ------------------------------------------------------------------------
    //  Getters
    // ------------------------------------------------------------------------

    /**
     * Gets the ID of the resource in which the TaskManager is started. The format of this depends
     * on how the TaskManager is started:
     *
     * <ul>
     *   <li>If the TaskManager is started via YARN, this is the YARN container ID.
     *   <li>If the TaskManager is started via Mesos, this is the Mesos container ID.
     *   <li>If the TaskManager is started in standalone mode, or via a MiniCluster, this is a
     *       random ID.
     *   <li>Other deployment modes can set the resource ID in other ways.
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
     * Returns the fully-qualified domain name of the TaskManager provided by {@link
     * #hostNameSupplier}.
     *
     * @return The fully-qualified domain name of the TaskManager.
     */
    public String getFQDNHostname() {
        return hostNameSupplier.getFqdnHostName();
    }

    /**
     * Gets the hostname of the TaskManager from {@link #hostNameSupplier}.
     *
     * @return The hostname of the TaskManager.
     */
    public String getHostname() {
        return hostNameSupplier.getHostName();
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

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
            LOG.warn(
                    "Unable to determine the canonical hostname. Input split assignment (such as "
                            + "for HDFS files) may be non-local when the canonical hostname is missing.");
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
            LOG.warn(
                    "No hostname could be resolved for the IP address {}, using IP address as host name. "
                            + "Local input split assignment (such as for HDFS files) may be impacted.",
                    inetAddress.getHostAddress());
        } else {
            hostName = NetUtils.getHostnameFromFQDN(fqdnHostName);
        }

        return hostName;
    }

    @Override
    public String toString() {
        if (stringRepresentation == null) {
            this.stringRepresentation =
                    String.format("%s @ %s (dataPort=%d)", resourceID, getFQDNHostname(), dataPort);
        }
        return stringRepresentation;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj != null && obj.getClass() == getClass()) {
            TaskManagerLocation that = (TaskManagerLocation) obj;
            return this.resourceID.equals(that.resourceID)
                    && this.inetAddress.equals(that.inetAddress)
                    && this.dataPort == that.dataPort;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return resourceID.hashCode() + 17 * inetAddress.hashCode() + 129 * dataPort;
    }

    @Override
    public int compareTo(@Nonnull TaskManagerLocation o) {
        // decide based on resource ID first
        int resourceIdCmp =
                this.resourceID.getResourceIdString().compareTo(o.resourceID.getResourceIdString());
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

    // --------------------------------------------------------------------------------------------
    // Hostname Resolution Suppliers
    // --------------------------------------------------------------------------------------------

    public interface HostNameSupplier extends Serializable {
        String getHostName();

        String getFqdnHostName();
    }

    /**
     * This Supplier class could retrieve the FQDN host name of the given InetAddress on demand,
     * extract the pure host name and cache the results for later use.
     */
    @VisibleForTesting
    public static class DefaultHostNameSupplier implements HostNameSupplier {
        private final InetAddress inetAddress;
        private String hostName;
        private String fqdnHostName;

        public DefaultHostNameSupplier(InetAddress inetAddress) {
            this.inetAddress = inetAddress;
        }

        /**
         * Gets the hostname of the TaskManager. The hostname derives from the fully qualified
         * domain name (FQDN, see {@link #getFQDNHostname()}):
         *
         * <ul>
         *   <li>If the FQDN is the textual IP address, then the hostname is also the IP address
         *   <li>If the FQDN has only one segment (such as "localhost", or "host17"), then this is
         *       used as the hostname.
         *   <li>If the FQDN has multiple segments (such as "worker3.subgroup.company.net"), then
         *       the first segment (here "worker3") will be used as the hostname.
         * </ul>
         *
         * @return The hostname of the TaskManager.
         */
        @Override
        public String getHostName() {
            if (hostName == null) {
                hostName = TaskManagerLocation.getHostName(inetAddress);
            }
            return hostName;
        }

        /**
         * Returns the fully-qualified domain name the TaskManager. If the name could not be
         * determined, the return value will be a textual representation of the TaskManager's IP
         * address.
         *
         * @return The fully-qualified domain name of the TaskManager.
         */
        @Override
        public String getFqdnHostName() {
            if (fqdnHostName == null) {
                fqdnHostName = TaskManagerLocation.getFqdnHostName(inetAddress);
            }
            return fqdnHostName;
        }
    }

    /**
     * This Supplier class returns the IP address of the given InetAddress directly, therefore no
     * reverse DNS lookup is required.
     */
    @VisibleForTesting
    public static class IpOnlyHostNameSupplier implements HostNameSupplier {
        private final InetAddress inetAddress;

        public IpOnlyHostNameSupplier(InetAddress inetAddress) {
            this.inetAddress = inetAddress;
        }

        /**
         * Returns the textual representation of the TaskManager's IP address as host name.
         *
         * @return The textual representation of the TaskManager's IP address.
         */
        @Override
        public String getHostName() {
            return inetAddress.getHostAddress();
        }

        /**
         * Returns the textual representation of the TaskManager's IP address as FQDN host name.
         *
         * @return The textual representation of the TaskManager's IP address.
         */
        @Override
        public String getFqdnHostName() {
            return inetAddress.getHostAddress();
        }
    }

    /** The DNS resolution mode for TaskManager's IP address. */
    public enum ResolutionMode {
        RETRIEVE_HOST_NAME,
        USE_IP_ONLY
    }
}
