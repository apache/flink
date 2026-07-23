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

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link NodeHealthManager} interface and its implementations. */
public class NodeHealthManagerTest {

    private static final ResourceID RESOURCE_ID_1 = new ResourceID("resource-1");
    private static final ResourceID RESOURCE_ID_2 = new ResourceID("resource-2");
    private static final ResourceID RESOURCE_ID_3 = new ResourceID("resource-3");
    private static final String HOSTNAME_1 = "host-1";
    private static final String HOSTNAME_2 = "host-2";
    private static final String REASON = "test reason";
    private static final Duration DURATION = Duration.ofMinutes(30);

    @Test
    void testNoOpNodeHealthManagerAlwaysHealthy() {
        final NodeHealthManager manager = new NoOpNodeHealthManager();

        // All nodes should be reported as healthy by NoOpNodeHealthManager
        assertThat(manager.isHealthy(RESOURCE_ID_1)).isTrue();
        assertThat(manager.isHealthy(RESOURCE_ID_2)).isTrue();
        assertThat(manager.isHealthy(RESOURCE_ID_3)).isTrue();
    }

    @Test
    void testDefaultNodeHealthManagerInitialState() {
        final DefaultNodeHealthManager manager = new DefaultNodeHealthManager();

        // All nodes should be healthy by default
        assertThat(manager.isHealthy(RESOURCE_ID_1)).isTrue();
        assertThat(manager.isHealthy(RESOURCE_ID_2)).isTrue();
        assertThat(manager.isHealthy(RESOURCE_ID_3)).isTrue();
    }

    @Test
    void testDefaultNodeHealthManagerMarkQuarantined() {
        final DefaultNodeHealthManager manager = new DefaultNodeHealthManager();

        // Mark a node as quarantined
        manager.markQuarantined(RESOURCE_ID_1, HOSTNAME_1, REASON, DURATION);

        // The marked node should be unhealthy
        assertThat(manager.isHealthy(RESOURCE_ID_1)).isFalse();
        // Other nodes should remain healthy
        assertThat(manager.isHealthy(RESOURCE_ID_2)).isTrue();
    }

    @Test
    void testDefaultNodeHealthManagerRemoveQuarantine() {
        final DefaultNodeHealthManager manager = new DefaultNodeHealthManager();

        // Mark a node as quarantined
        manager.markQuarantined(RESOURCE_ID_1, HOSTNAME_1, REASON, DURATION);
        assertThat(manager.isHealthy(RESOURCE_ID_1)).isFalse();

        // Remove quarantine from the node
        manager.removeQuarantine(RESOURCE_ID_1);
        assertThat(manager.isHealthy(RESOURCE_ID_1)).isTrue();
    }

    @Test
    void testDefaultNodeHealthManagerMultipleNodes() {
        final DefaultNodeHealthManager manager = new DefaultNodeHealthManager();

        // Mark multiple nodes as quarantined
        manager.markQuarantined(RESOURCE_ID_1, HOSTNAME_1, REASON, DURATION);
        manager.markQuarantined(RESOURCE_ID_2, HOSTNAME_2, REASON, DURATION);

        assertThat(manager.isHealthy(RESOURCE_ID_1)).isFalse();
        assertThat(manager.isHealthy(RESOURCE_ID_2)).isFalse();
        assertThat(manager.isHealthy(RESOURCE_ID_3)).isTrue();
    }

    @Test
    void testDefaultNodeHealthManagerListAll() {
        final DefaultNodeHealthManager manager = new DefaultNodeHealthManager();

        // Initially, no nodes should be quarantined
        Collection<NodeHealthStatus> statuses = manager.listAll();
        assertThat(statuses).isEmpty();

        // Mark a node as quarantined
        manager.markQuarantined(RESOURCE_ID_1, HOSTNAME_1, REASON, DURATION);

        // Should have one quarantined node
        statuses = manager.listAll();
        assertThat(statuses).hasSize(1);
        NodeHealthStatus status = statuses.iterator().next();
        assertThat(status.getResourceID()).isEqualTo(RESOURCE_ID_1);
        assertThat(status.getHostname()).isEqualTo(HOSTNAME_1);
        assertThat(status.getReason()).isEqualTo(REASON);

        // Mark another node as quarantined
        manager.markQuarantined(RESOURCE_ID_2, HOSTNAME_2, REASON, DURATION);

        // Should have two quarantined nodes
        statuses = manager.listAll();
        assertThat(statuses).hasSize(2);

        // Remove quarantine from one node
        manager.removeQuarantine(RESOURCE_ID_1);

        // Should have one quarantined node left
        statuses = manager.listAll();
        assertThat(statuses).hasSize(1);
        status = statuses.iterator().next();
        assertThat(status.getResourceID()).isEqualTo(RESOURCE_ID_2);
    }

    @Test
    void testDefaultNodeHealthManagerRemoveQuarantineFromNeverQuarantinedNode() {
        final DefaultNodeHealthManager manager = new DefaultNodeHealthManager();

        // Remove quarantine from a node that was never quarantined
        // This should be a no-op, node should remain healthy
        manager.removeQuarantine(RESOURCE_ID_1);
        assertThat(manager.isHealthy(RESOURCE_ID_1)).isTrue();
    }

    @Test
    void testDefaultNodeHealthManagerCleanupExpired() {
        final DefaultNodeHealthManager manager = new DefaultNodeHealthManager();

        // Mark nodes as quarantined with very short durations
        manager.markQuarantined(RESOURCE_ID_1, HOSTNAME_1, REASON, Duration.ofMillis(100));
        manager.markQuarantined(RESOURCE_ID_2, HOSTNAME_2, REASON, Duration.ofHours(1));

        assertThat(manager.isHealthy(RESOURCE_ID_1)).isFalse();
        assertThat(manager.isHealthy(RESOURCE_ID_2)).isFalse();

        // Wait for first quarantine to expire
        try {
            Thread.sleep(150);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Cleanup expired quarantines
        manager.cleanupExpired();

        // First node should be healthy again, second should still be quarantined
        assertThat(manager.isHealthy(RESOURCE_ID_1)).isTrue();
        assertThat(manager.isHealthy(RESOURCE_ID_2)).isFalse();

        Collection<NodeHealthStatus> statuses = manager.listAll();
        assertThat(statuses).hasSize(1);
        assertThat(statuses.iterator().next().getResourceID()).isEqualTo(RESOURCE_ID_2);
    }

    @Test
    void testNoOpNodeHealthManagerListAllAndCleanup() {
        final NodeHealthManager manager = new NoOpNodeHealthManager();

        // NoOpNodeHealthManager should always return empty list
        assertThat(manager.listAll()).isEmpty();

        // Cleanup should be a no-op
        manager.cleanupExpired();
        assertThat(manager.listAll()).isEmpty();

        // Mark and remove should be no-ops
        manager.markQuarantined(RESOURCE_ID_1, HOSTNAME_1, REASON, DURATION);
        assertThat(manager.isHealthy(RESOURCE_ID_1)).isTrue();
        assertThat(manager.listAll()).isEmpty();

        manager.removeQuarantine(RESOURCE_ID_1);
        assertThat(manager.isHealthy(RESOURCE_ID_1)).isTrue();
    }

    @Test
    void testNodeHealthStatus() {
        final long quarantineTimestamp = System.currentTimeMillis();
        final long expirationTimestamp = quarantineTimestamp + Duration.ofHours(1).toMillis();

        final NodeHealthStatus status1 =
                new NodeHealthStatus(
                        RESOURCE_ID_1,
                        HOSTNAME_1,
                        REASON,
                        quarantineTimestamp,
                        expirationTimestamp);
        final NodeHealthStatus status2 =
                new NodeHealthStatus(
                        RESOURCE_ID_2,
                        HOSTNAME_2,
                        REASON,
                        quarantineTimestamp,
                        expirationTimestamp);

        assertThat(status1.getResourceID()).isEqualTo(RESOURCE_ID_1);
        assertThat(status1.getHostname()).isEqualTo(HOSTNAME_1);
        assertThat(status1.getReason()).isEqualTo(REASON);
        assertThat(status1.getQuarantineTimestamp()).isEqualTo(quarantineTimestamp);
        assertThat(status1.getExpirationTimestamp()).isEqualTo(expirationTimestamp);

        assertThat(status2.getResourceID()).isEqualTo(RESOURCE_ID_2);
        assertThat(status2.getHostname()).isEqualTo(HOSTNAME_2);
    }

    @Test
    void testNodeHealthStatusIsExpired() {
        final long quarantineTimestamp =
                System.currentTimeMillis() - Duration.ofHours(2).toMillis();
        final long expirationTimestamp =
                System.currentTimeMillis() - Duration.ofHours(1).toMillis();

        final NodeHealthStatus status =
                new NodeHealthStatus(
                        RESOURCE_ID_1,
                        HOSTNAME_1,
                        REASON,
                        quarantineTimestamp,
                        expirationTimestamp);

        // The quarantine should be expired
        assertThat(status.isExpired(System.currentTimeMillis())).isTrue();
        // The quarantine should not be expired at the quarantine timestamp
        assertThat(status.isExpired(quarantineTimestamp)).isFalse();
    }

    @Test
    void testNodeHealthStatusEqualsAndHashCode() {
        final long quarantineTimestamp = System.currentTimeMillis();
        final long expirationTimestamp = quarantineTimestamp + Duration.ofHours(1).toMillis();

        final NodeHealthStatus status1a =
                new NodeHealthStatus(
                        RESOURCE_ID_1,
                        HOSTNAME_1,
                        REASON,
                        quarantineTimestamp,
                        expirationTimestamp);
        final NodeHealthStatus status1b =
                new NodeHealthStatus(
                        RESOURCE_ID_1,
                        HOSTNAME_1,
                        REASON,
                        quarantineTimestamp,
                        expirationTimestamp);
        final NodeHealthStatus status2 =
                new NodeHealthStatus(
                        RESOURCE_ID_2,
                        HOSTNAME_2,
                        REASON,
                        quarantineTimestamp,
                        expirationTimestamp);

        assertThat(status1a).isEqualTo(status1b);
        assertThat(status1a).isNotEqualTo(status2);
        assertThat(status1a.hashCode()).isEqualTo(status1b.hashCode());
    }

    @Test
    void testNodeHealthStatusToString() {
        final long quarantineTimestamp = System.currentTimeMillis();
        final long expirationTimestamp = quarantineTimestamp + Duration.ofHours(1).toMillis();

        final NodeHealthStatus status =
                new NodeHealthStatus(
                        RESOURCE_ID_1,
                        HOSTNAME_1,
                        REASON,
                        quarantineTimestamp,
                        expirationTimestamp);

        final String toString = status.toString();
        assertThat(toString).contains(RESOURCE_ID_1.toString());
        assertThat(toString).contains(HOSTNAME_1);
        assertThat(toString).contains(REASON);
    }
}
