/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.highavailability.zookeeper.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.rest.util.NoOpFatalErrorHandler;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for the {@link ZooKeeperUtils}. */
class ZooKeeperUtilsWriteLeaderInformationTest {
    private final ZooKeeperExtension zooKeeperExtension = new ZooKeeperExtension();

    @RegisterExtension
    private final EachCallbackWrapper<ZooKeeperExtension> eachWrapper =
            new EachCallbackWrapper<>(zooKeeperExtension);

    @Test
    public void testWriteAndReadLeaderInformation() throws Exception {
        runWriteAndReadLeaderInformationTest(LeaderInformation.known(UUID.randomUUID(), "barfoo"));
    }

    @Test
    public void testWriteAndReadEmptyLeaderInformation() throws Exception {
        runWriteAndReadLeaderInformationTest(LeaderInformation.empty());
    }

    private void runWriteAndReadLeaderInformationTest(LeaderInformation leaderInformation)
            throws Exception {
        final CuratorFrameworkWithUnhandledErrorListener curatorFramework = startCuratorFramework();

        final String path = "/foobar";

        try {
            ZooKeeperUtils.writeLeaderInformationToZooKeeper(
                    leaderInformation, curatorFramework.asCuratorFramework(), () -> true, path);

            final LeaderInformation readLeaderInformation =
                    ZooKeeperUtils.readLeaderInformation(
                            curatorFramework.asCuratorFramework().getData().forPath(path));

            assertThat(readLeaderInformation).isEqualTo(leaderInformation);
        } finally {
            curatorFramework.close();
        }
    }

    @Nonnull
    private CuratorFrameworkWithUnhandledErrorListener startCuratorFramework() {
        final Configuration configuration = new Configuration();
        configuration.set(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperExtension.getConnectString());
        final CuratorFrameworkWithUnhandledErrorListener curatorFramework =
                ZooKeeperUtils.startCuratorFramework(configuration, NoOpFatalErrorHandler.INSTANCE);
        return curatorFramework;
    }

    @Test
    public void testDeleteZNode() throws Exception {
        final CuratorFrameworkWithUnhandledErrorListener curatorFramework = startCuratorFramework();

        try {
            final String path = "/foobar";
            curatorFramework.asCuratorFramework().create().forPath(path, new byte[4]);
            curatorFramework.asCuratorFramework().create().forPath(path + "/bar", new byte[4]);
            ZooKeeperUtils.deleteZNode(curatorFramework.asCuratorFramework(), path);

            assertThat(curatorFramework.asCuratorFramework().getChildren().forPath("/")).isEmpty();
        } finally {
            curatorFramework.close();
        }
    }
}
