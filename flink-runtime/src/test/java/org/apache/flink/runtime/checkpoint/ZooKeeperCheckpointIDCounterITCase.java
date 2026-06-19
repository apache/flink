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
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.KeeperException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.ExecutionException;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the {@link ZooKeeperCheckpointIDCounter}. The tests are inherited from the test
 * base class {@link CheckpointIDCounterTestBase}.
 */
class ZooKeeperCheckpointIDCounterITCase extends CheckpointIDCounterTestBase {

    private final ZooKeeperExtension zooKeeperExtension = new ZooKeeperExtension();

    @RegisterExtension
    final EachCallbackWrapper<ZooKeeperExtension> zooKeeperResource =
            new EachCallbackWrapper<>(zooKeeperExtension);

    @RegisterExtension
    final TestingFatalErrorHandlerExtension testingFatalErrorHandlerResource =
            new TestingFatalErrorHandlerExtension();

    private CuratorFramework getZooKeeperClient() {
        return zooKeeperExtension.getZooKeeperClient(
                testingFatalErrorHandlerResource.getTestingFatalErrorHandler());
    }

    /** Tests that counter node is removed from ZooKeeper after shutdown. */
    @Test
    void testShutdownRemovesState() throws Exception {
        ZooKeeperCheckpointIDCounter counter = createCheckpointIdCounter();
        counter.start();

        CuratorFramework client = getZooKeeperClient();
        assertThat(client.checkExists().forPath(counter.getPath())).isNotNull();

        counter.shutdown(JobStatus.FINISHED).join();
        assertThat(client.checkExists().forPath(counter.getPath())).isNull();
    }

    @Test
    void testIdempotentShutdown() throws Exception {
        ZooKeeperCheckpointIDCounter counter = createCheckpointIdCounter();
        counter.start();

        CuratorFramework client = getZooKeeperClient();
        counter.shutdown(JobStatus.FINISHED).join();

        // shutdown shouldn't fail due to missing path
        counter.shutdown(JobStatus.FINISHED).join();
        assertThat(client.checkExists().forPath(counter.getPath())).isNull();
    }

    @Test
    void testShutdownWithFailureDueToMissingConnection() throws Exception {
        ZooKeeperCheckpointIDCounter counter = createCheckpointIdCounter();
        counter.start();

        zooKeeperExtension.close();

        assertThatFuture(counter.shutdown(JobStatus.FINISHED))
                .as("The shutdown should fail because of the client connection being dropped.")
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void testShutdownWithFailureDueToExistingChildNodes() throws Exception {
        final ZooKeeperCheckpointIDCounter counter = createCheckpointIdCounter();
        counter.start();

        final CuratorFramework client =
                ZooKeeperUtils.useNamespaceAndEnsurePath(getZooKeeperClient(), "/");
        final String counterNodePath = ZooKeeperUtils.generateZookeeperPath(counter.getPath());
        final String childNodePath =
                ZooKeeperUtils.generateZookeeperPath(
                        counterNodePath, "unexpected-child-node-causing-a-failure");
        client.create().forPath(childNodePath);

        final String namespacedCounterNodePath =
                ZooKeeperUtils.generateZookeeperPath(client.getNamespace(), counterNodePath);
        final Throwable expectedRootCause =
                KeeperException.create(KeeperException.Code.NOTEMPTY, namespacedCounterNodePath);
        assertThatFuture(counter.shutdown(JobStatus.FINISHED))
                .as(
                        "The shutdown should fail because of a child node being present and the shutdown not performing an explicit recursive deletion.")
                .eventuallyFailsWith(ExecutionException.class)
                .havingCause()
                .withCause(expectedRootCause);

        client.delete().forPath(childNodePath);
        counter.shutdown(JobStatus.FINISHED).join();

        assertThat(client.checkExists().forPath(counterNodePath))
                .as(
                        "A retry of the shutdown should have worked now after the root cause was resolved.")
                .isNull();
    }

    /** Tests that counter node is NOT removed from ZooKeeper after suspend. */
    @Test
    void testSuspendKeepsState() throws Exception {
        ZooKeeperCheckpointIDCounter counter = createCheckpointIdCounter();
        counter.start();

        CuratorFramework client = getZooKeeperClient();
        assertThat(client.checkExists().forPath(counter.getPath())).isNotNull();

        counter.shutdown(JobStatus.SUSPENDED).join();
        assertThat(client.checkExists().forPath(counter.getPath())).isNotNull();
    }

    @Override
    protected ZooKeeperCheckpointIDCounter createCheckpointIdCounter() throws Exception {
        return new ZooKeeperCheckpointIDCounter(
                ZooKeeperUtils.useNamespaceAndEnsurePath(getZooKeeperClient(), "/"),
                new DefaultLastStateConnectionStateListener());
    }
}
