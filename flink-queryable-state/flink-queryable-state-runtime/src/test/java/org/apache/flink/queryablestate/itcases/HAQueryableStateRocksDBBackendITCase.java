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

package org.apache.flink.queryablestate.itcases;

import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.CheckpointStorageUtils;
import org.apache.flink.streaming.util.StateBackendUtils;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

/**
 * Several integration tests for queryable state using the {@link
 * org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend}.
 */
class HAQueryableStateRocksDBBackendITCase extends AbstractQueryableStateTestBase {

    // NUM_TMS * NUM_SLOTS_PER_TM must match the parallelism of the pipelines so that
    // we always use all TaskManagers so that the JM oracle is always properly re-registered
    private static final int NUM_TMS = 2;
    private static final int NUM_SLOTS_PER_TM = 2;

    private static final int QS_PROXY_PORT_RANGE_START = 9074;
    private static final int QS_SERVER_PORT_RANGE_START = 9079;

    @TempDir
    @Order(1)
    static Path tmpStateBackendDir;

    @TempDir
    @Order(2)
    static Path tmpHaStoragePath;

    @RegisterExtension
    @Order(3)
    static final AllCallbackWrapper<ZooKeeperExtension> ZK_RESOURCE =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    @RegisterExtension
    @Order(4)
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    () ->
                            new MiniClusterResourceConfiguration.Builder()
                                    .setConfiguration(getConfig())
                                    .setNumberTaskManagers(NUM_TMS)
                                    .setNumberSlotsPerTaskManager(NUM_SLOTS_PER_TM)
                                    .build());

    @Override
    protected StreamExecutionEnvironment createEnv() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StateBackendUtils.configureRocksDBStateBackend(env);
        CheckpointStorageUtils.configureFileSystemCheckpointStorage(
                env, tmpHaStoragePath.toUri().toString());
        return env;
    }

    @BeforeAll
    static void setup(@InjectClusterClient RestClusterClient<?> injectedClusterClient)
            throws Exception {
        client = new QueryableStateClient("localhost", QS_PROXY_PORT_RANGE_START);

        clusterClient = injectedClusterClient;
    }

    @AfterAll
    static void tearDown() throws Exception {
        client.shutdownAndWait();
    }

    private static Configuration getConfig() {

        Configuration config = new Configuration();
        config.set(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("4m"));
        config.set(TaskManagerOptions.MINI_CLUSTER_NUM_TASK_MANAGERS, NUM_TMS);
        config.set(TaskManagerOptions.NUM_TASK_SLOTS, NUM_SLOTS_PER_TM);
        config.set(QueryableStateOptions.CLIENT_NETWORK_THREADS, 2);
        config.set(QueryableStateOptions.PROXY_NETWORK_THREADS, 2);
        config.set(QueryableStateOptions.SERVER_NETWORK_THREADS, 2);
        config.set(
                QueryableStateOptions.PROXY_PORT_RANGE,
                QS_PROXY_PORT_RANGE_START + "-" + (QS_PROXY_PORT_RANGE_START + NUM_TMS));
        config.set(
                QueryableStateOptions.SERVER_PORT_RANGE,
                QS_SERVER_PORT_RANGE_START + "-" + (QS_SERVER_PORT_RANGE_START + NUM_TMS));
        config.set(WebOptions.SUBMIT_ENABLE, false);

        config.set(HighAvailabilityOptions.HA_STORAGE_PATH, tmpHaStoragePath.toString());

        config.set(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM,
                ZK_RESOURCE.getCustomExtension().getConnectString());
        config.set(HighAvailabilityOptions.HA_MODE, "zookeeper");

        return config;
    }
}
