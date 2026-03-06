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

package org.apache.flink.runtime.resourcemanager.health;

import org.apache.flink.runtime.clusterframework.types.ResourceID;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Represents the health status of a node.
 *
 * <p>This class holds information about a node's quarantine status including the resource ID,
 * hostname, reason for quarantine, quarantine timestamp, and expiration timestamp.
 */
public class NodeHealthStatus {

    private final ResourceID resourceID;
    private final String hostname;
    private final String reason;
    private final long quarantineTimestamp;
    private final long expirationTimestamp;

    /**
     * Creates a new NodeHealthStatus.
     *
     * @param resourceID the resource ID of the node
     * @param hostname the hostname of the node
     * @param reason the reason for quarantining
     * @param quarantineTimestamp the timestamp when the node was quarantined
     * @param expirationTimestamp the timestamp when the quarantine expires
     */
    public NodeHealthStatus(
            ResourceID resourceID,
            String hostname,
            String reason,
            long quarantineTimestamp,
            long expirationTimestamp) {
        this.resourceID = Objects.requireNonNull(resourceID, "resourceID must not be null");
        this.hostname = Objects.requireNonNull(hostname, "hostname must not be null");
        this.reason = Objects.requireNonNull(reason, "reason must not be null");
        this.quarantineTimestamp = quarantineTimestamp;
        this.expirationTimestamp = expirationTimestamp;
    }

    /**
     * Returns the resource ID of the node.
     *
     * @return the resource ID
     */
    public ResourceID getResourceID() {
        return resourceID;
    }

    /**
     * Returns the hostname of the node.
     *
     * @return the hostname
     */
    public String getHostname() {
        return hostname;
    }

    /**
     * Returns the reason for quarantining.
     *
     * @return the reason
     */
    public String getReason() {
        return reason;
    }

    /**
     * Returns the timestamp when the node was quarantined.
     *
     * @return the quarantine timestamp
     */
    public long getQuarantineTimestamp() {
        return quarantineTimestamp;
    }

    /**
     * Returns the timestamp when the quarantine expires.
     *
     * @return the expiration timestamp
     */
    public long getExpirationTimestamp() {
        return expirationTimestamp;
    }

    /**
     * Checks if the quarantine has expired.
     *
     * @param now the current timestamp in milliseconds
     * @return true if the quarantine has expired, false otherwise
     */
    public boolean isExpired(long now) {
        return now >= expirationTimestamp;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NodeHealthStatus that = (NodeHealthStatus) o;
        return quarantineTimestamp == that.quarantineTimestamp
                && expirationTimestamp == that.expirationTimestamp
                && Objects.equals(resourceID, that.resourceID)
                && Objects.equals(hostname, that.hostname)
                && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceID, hostname, reason, quarantineTimestamp, expirationTimestamp);
    }

    @Override
    public String toString() {
        return "NodeHealthStatus{"
                + "resourceID="
                + resourceID
                + ", hostname='"
                + hostname
                + '\''
                + ", reason='"
                + reason
                + '\''
                + ", quarantineTimestamp="
                + quarantineTimestamp
                + ", expirationTimestamp="
                + expirationTimestamp
                + '}';
    }
}
