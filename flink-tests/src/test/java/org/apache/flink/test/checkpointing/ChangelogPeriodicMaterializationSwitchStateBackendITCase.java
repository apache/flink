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
 * This verifies that switching state backend works correctly for Changelog state backend with
 * materialized state / non-materialized state.
 */
public class ChangelogPeriodicMaterializationSwitchStateBackendITCase
        extends ChangelogPeriodicMaterializationSwitchEnvTestBase {

    public ChangelogPeriodicMaterializationSwitchStateBackendITCase(
            AbstractStateBackend delegatedStateBackend) {
        super(delegatedStateBackend);
    }

    @Test
    public void testSwitchFromEnablingToDisabling() throws Exception {
        testSwitchEnv(getEnv(true), getEnv(false));
    }

    @Test
    public void testSwitchFromEnablingToDisablingWithRescalingOut() throws Exception {
        testSwitchEnv(getEnv(true, NUM_SLOTS / 2), getEnv(false, NUM_SLOTS));
    }

    @Test
    public void testSwitchFromEnablingToDisablingWithRescalingIn() throws Exception {
        testSwitchEnv(getEnv(true, NUM_SLOTS), getEnv(false, NUM_SLOTS / 2));
    }

    private StreamExecutionEnvironment getEnv(boolean enableChangelog) {
        return getEnv(enableChangelog, NUM_SLOTS);
    }

    private StreamExecutionEnvironment getEnv(boolean enableChangelog, int parallelism) {
        StreamExecutionEnvironment env = getEnv(delegatedStateBackend, 100, 0, 500, 0);
        env.enableChangelogStateBackend(enableChangelog);
        env.setParallelism(parallelism);
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        return env;
    }
}
