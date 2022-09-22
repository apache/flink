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

package org.apache.flink.connector.pulsar.table;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntime;
import org.apache.flink.connector.testframe.environment.MiniClusterTestEnvironment;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base class for Pulsar table integration test. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class PulsarTableTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarTableTestBase.class);

    @TestEnv MiniClusterTestEnvironment flink = new MiniClusterTestEnvironment();

    // Defines pulsar running environment
    @TestExternalSystem
    protected PulsarTestEnvironment pulsar = new PulsarTestEnvironment(runtime());

    @TestSemantics
    protected CheckpointingMode[] semantics =
            new CheckpointingMode[] {CheckpointingMode.EXACTLY_ONCE};

    protected StreamExecutionEnvironment env;

    protected StreamTableEnvironment tableEnv;

    protected PulsarRuntime runtime() {
        return PulsarRuntime.container();
    }

    private static final int DEFAULT_PARALLELISM = 2;

    @BeforeAll
    public void beforeAll() {
        pulsar.startUp();
        // run env
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig()
                .getConfiguration()
                .setString("table.dynamic-table-options.enabled", "true");
    }

    public void createTestTopic(String topic, int numPartitions) {
        pulsar.operator().createTopic(topic, numPartitions);
    }

    @AfterAll
    public void afterAll() {
        pulsar.tearDown();
    }
}
