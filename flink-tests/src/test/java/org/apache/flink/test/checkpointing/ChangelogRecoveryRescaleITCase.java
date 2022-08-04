/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.Test;

/**
 * This verifies that rescale works correctly for Changelog state backend with materialized state /
 * non-materialized state.
 */
public class ChangelogRecoveryRescaleITCase extends ChangelogRecoverySwitchEnvTestBase {

    public ChangelogRecoveryRescaleITCase(AbstractStateBackend delegatedStateBackend) {
        super(delegatedStateBackend);
    }

    @Test
    public void testRescaleOut() throws Exception {
        testSwitchEnv(getEnv(NUM_SLOTS / 2), getEnv(NUM_SLOTS));
    }

    @Test
    public void testRescaleIn() throws Exception {
        testSwitchEnv(getEnv(NUM_SLOTS), getEnv(NUM_SLOTS / 2));
    }

    private StreamExecutionEnvironment getEnv(int parallelism) {
        StreamExecutionEnvironment env = getEnv(delegatedStateBackend, 50, 0, 20, 0);
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setParallelism(parallelism);
        return env;
    }
}
