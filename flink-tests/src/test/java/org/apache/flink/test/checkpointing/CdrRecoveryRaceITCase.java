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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Exercises the checkpointing-during-recovery restore-loop path for NON-source operators: a bounded
 * BATCH operator (behind a blocking exchange) whose input is already fully available drains it to
 * {@code END_OF_INPUT} during the recovery window. Without the fix in {@code
 * StreamTask.recoverChannels} this suspends the restore mailbox loop before recovery completes,
 * tripping "Mailbox loop interrupted before recovery was finished" in {@code
 * StreamTask.restoreInternal}. CDR is forced on by the surrounding test setup.
 *
 * <p>INCOMPLETE / NOT A RELIABLE REGRESSION GUARD. The race window on a fresh deployment with no
 * recovered channel state is very small, so this test does NOT deterministically reproduce the bug
 * on its own: during development it reproduced reliably only with a temporary artificial delay
 * inserted into the recovery chain (after partitions are requested, before completion) to widen the
 * window -- that delay is intentionally NOT part of the product code. As committed this is a fast
 * BATCH + CDR smoke test of the no-recovered-state recovery path, not a guaranteed reproducer; a
 * future iteration should make it deterministic (e.g. a test-only hook to widen the window, or an
 * unaligned-checkpoint restore that carries in-flight channel state) before relying on it.
 */
// TODO FLINK-38544: make this deterministically reproduce the non-source recovery race.
class CdrRecoveryRaceITCase {

    private static final int PARALLELISM = 8;
    private static final int ITERATIONS = 30;

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(new Configuration())
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .build());

    @Test
    void testBoundedOperatorFinishesDuringRecovery() throws Exception {
        for (int i = 0; i < ITERATIONS; i++) {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(PARALLELISM);
            // BATCH mode: blocking exchanges make a downstream operator's entire input available
            // before it deploys, so during its restore loop it drains straight to END_OF_INPUT --
            // the condition that trips the non-source recovery race (mirrors TPC-DS).
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
            env.enableCheckpointing(50, CheckpointingMode.EXACTLY_ONCE);

            DataStream<Long> source = env.fromSequence(1, 64);
            source.rebalance()
                    .map((MapFunction<Long, Long>) value -> value + 1)
                    .name("non-source-map")
                    .rebalance()
                    .map((MapFunction<Long, Long>) value -> value * 2)
                    .name("non-source-map-2")
                    .sinkTo(new DiscardingSink<>());

            env.execute("cdr-recovery-race-" + i);
        }
    }
}
