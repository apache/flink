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
import org.apache.flink.runtime.zookeeper.ZooKeeperTestEnvironment;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for the {@link ZooKeeperCheckpointIDCounter}. The tests are inherited from the test
 * base class {@link CheckpointIDCounterTestBase}.
 */
public class ZooKeeperCheckpointIDCounterITCase extends CheckpointIDCounterTestBase {

    private static final ZooKeeperTestEnvironment ZooKeeper = new ZooKeeperTestEnvironment(1);

    @AfterClass
    public static void tearDown() throws Exception {
        ZooKeeper.shutdown();
    }

    @Before
    public void cleanUp() throws Exception {
        ZooKeeper.deleteAll();
    }

    /** Tests that counter node is removed from ZooKeeper after shutdown. */
    @Test
    public void testShutdownRemovesState() throws Exception {
        ZooKeeperCheckpointIDCounter counter = createCheckpointIdCounter();
        counter.start();

        CuratorFramework client = ZooKeeper.getClient();
        assertNotNull(client.checkExists().forPath(counter.getPath()));

        counter.shutdown(JobStatus.FINISHED);
        assertNull(client.checkExists().forPath(counter.getPath()));
    }

    /** Tests that counter node is NOT removed from ZooKeeper after suspend. */
    @Test
    public void testSuspendKeepsState() throws Exception {
        ZooKeeperCheckpointIDCounter counter = createCheckpointIdCounter();
        counter.start();

        CuratorFramework client = ZooKeeper.getClient();
        assertNotNull(client.checkExists().forPath(counter.getPath()));

        counter.shutdown(JobStatus.SUSPENDED);
        assertNotNull(client.checkExists().forPath(counter.getPath()));
    }

    @Override
    protected ZooKeeperCheckpointIDCounter createCheckpointIdCounter() throws Exception {
        return new ZooKeeperCheckpointIDCounter(
                ZooKeeper.getClient(), new DefaultLastStateConnectionStateListener());
    }
}
