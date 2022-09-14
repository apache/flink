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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperTestEnvironment;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.KeeperException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the {@link ZooKeeperCheckpointIDCounter}. The tests are inherited from the test
 * base class {@link CheckpointIDCounterTestBase}.
 */
class ZooKeeperCheckpointIDCounterITCase extends CheckpointIDCounterTestBase {

    private static ZooKeeperTestEnvironment zookeeper;

    @BeforeEach
    void setup() {
        zookeeper = new ZooKeeperTestEnvironment(1);
    }

    @AfterEach
    void tearDown() throws Exception {
        cleanAndStopZooKeeperIfRunning();
    }

    private void cleanAndStopZooKeeperIfRunning() throws Exception {
        if (zookeeper.getClient().isStarted()) {
            zookeeper.deleteAll();
            zookeeper.shutdown();
        }
    }

    /** Tests that counter node is removed from ZooKeeper after shutdown. */
    @Test
    public void testShutdownRemovesState() throws Exception {
        ZooKeeperCheckpointIDCounter counter = createCheckpointIdCounter();
        counter.start();

        CuratorFramework client = zookeeper.getClient();
        assertThat(client.checkExists().forPath(counter.getPath())).isNotNull();

        counter.shutdown(JobStatus.FINISHED).join();
        assertThat(client.checkExists().forPath(counter.getPath())).isNull();
    }

    @Test
    public void testIdempotentShutdown() throws Exception {
        ZooKeeperCheckpointIDCounter counter = createCheckpointIdCounter();
        counter.start();

        CuratorFramework client = zookeeper.getClient();
        counter.shutdown(JobStatus.FINISHED).join();

        // shutdown shouldn't fail due to missing path
        counter.shutdown(JobStatus.FINISHED).join();
        assertThat(client.checkExists().forPath(counter.getPath())).isNull();
    }

    @Test
    public void testShutdownWithFailureDueToMissingConnection() throws Exception {
        ZooKeeperCheckpointIDCounter counter = createCheckpointIdCounter();
        counter.start();

        cleanAndStopZooKeeperIfRunning();

        assertThatThrownBy(() -> counter.shutdown(JobStatus.FINISHED).get())
                .as("The shutdown should fail because of the client connection being dropped.")
                .isInstanceOf(ExecutionException.class)
                .hasCauseExactlyInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testShutdownWithFailureDueToExistingChildNodes() throws Exception {
        final ZooKeeperCheckpointIDCounter counter = createCheckpointIdCounter();
        counter.start();

        final CuratorFramework client =
                ZooKeeperUtils.useNamespaceAndEnsurePath(zookeeper.getClient(), "/");
        final String counterNodePath = ZooKeeperUtils.generateZookeeperPath(counter.getPath());
        final String childNodePath =
                ZooKeeperUtils.generateZookeeperPath(
                        counterNodePath, "unexpected-child-node-causing-a-failure");
        client.create().forPath(childNodePath);

        final String namespacedCounterNodePath =
                ZooKeeperUtils.generateZookeeperPath(client.getNamespace(), counterNodePath);
        final Throwable expectedRootCause =
                KeeperException.create(KeeperException.Code.NOTEMPTY, namespacedCounterNodePath);
        assertThatThrownBy(() -> counter.shutdown(JobStatus.FINISHED).get())
                .as(
                        "The shutdown should fail because of a child node being present and the shutdown not performing an explicit recursive deletion.")
                .isInstanceOf(ExecutionException.class)
                .extracting(FlinkAssertions::chainOfCauses, FlinkAssertions.STREAM_THROWABLE)
                .anySatisfy(
                        cause ->
                                assertThat(cause)
                                        .isInstanceOf(expectedRootCause.getClass())
                                        .hasMessage(expectedRootCause.getMessage()));

        client.delete().forPath(childNodePath);
        counter.shutdown(JobStatus.FINISHED).join();

        assertThat(client.checkExists().forPath(counterNodePath))
                .as(
                        "A retry of the shutdown should have worked now after the root cause was resolved.")
                .isNull();
    }

    /** Tests that counter node is NOT removed from ZooKeeper after suspend. */
    @Test
    public void testSuspendKeepsState() throws Exception {
        ZooKeeperCheckpointIDCounter counter = createCheckpointIdCounter();
        counter.start();

        CuratorFramework client = zookeeper.getClient();
        assertThat(client.checkExists().forPath(counter.getPath())).isNotNull();

        counter.shutdown(JobStatus.SUSPENDED).join();
        assertThat(client.checkExists().forPath(counter.getPath())).isNotNull();
    }

    @Override
    protected ZooKeeperCheckpointIDCounter createCheckpointIdCounter() throws Exception {
        return new ZooKeeperCheckpointIDCounter(
                ZooKeeperUtils.useNamespaceAndEnsurePath(zookeeper.getClient(), "/"),
                new DefaultLastStateConnectionStateListener());
    }
}
