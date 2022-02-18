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

package org.apache.flink.connector.pulsar.testutils.runtime.mock;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.MoreExecutors;

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl.ENCODING_SCHEME;
import static org.apache.zookeeper.CreateMode.PERSISTENT;

/** A ZooKeeperClientFactory implementation which returns mocked zookeeper instead of normal zk. */
public class MockZooKeeperClientFactory implements ZooKeeperClientFactory {

    private final MockZooKeeper zooKeeper;

    public MockZooKeeperClientFactory() {
        this.zooKeeper = MockZooKeeper.newInstance(MoreExecutors.newDirectExecutorService());
        List<ACL> dummyAclList = new ArrayList<>(0);

        try {
            ZkUtils.createFullPathOptimistic(
                    zooKeeper,
                    "/ledgers/available/192.168.1.1:" + 5000,
                    "".getBytes(ENCODING_SCHEME),
                    dummyAclList,
                    PERSISTENT);

            zooKeeper.create(
                    "/ledgers/LAYOUT",
                    "1\nflat:1".getBytes(ENCODING_SCHEME),
                    dummyAclList,
                    PERSISTENT);
        } catch (KeeperException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public CompletableFuture<ZooKeeper> create(
            String serverList, SessionType sessionType, int zkSessionTimeoutMillis) {
        return CompletableFuture.completedFuture(zooKeeper);
    }

    MockZooKeeper getZooKeeper() {
        return zooKeeper;
    }
}
