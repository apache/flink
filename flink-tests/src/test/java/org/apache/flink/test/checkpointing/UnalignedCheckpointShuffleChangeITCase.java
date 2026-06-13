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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for changing distribution patterns (POINTWISE / ALL_TO_ALL) and parallelism
 * between runs when restoring from an unaligned checkpoint.
 *
 * <p>Each parameterized case is a 9-tuple: (desc, oldUpPar, oldDownPar, oldPattern, newUpPar,
 * newDownPar, newPattern, recoverOutputOnDownstream, checkpointDuringRecovery). Run 1 emits a
 * bounded monotonic sequence {@code [0, TOTAL_COUNT)} XORed with a header, takes an in-flight
 * unaligned checkpoint, and is cancelled. Run 2 restores with the new topology and runs to
 * completion. The verifying sink asserts: (a) no header corruption (mis-routed buffers), (b) no
 * per-subtask duplicates after restore, and (c) every value in {@code [0, TOTAL_COUNT)} arrives at
 * the sink.
 *
 * <p>The test is parameterized over three recovery configurations:
 *
 * <ul>
 *   <li>Neither {@code UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM} nor {@code
 *       CHECKPOINTING_DURING_RECOVERY_ENABLED} (both false)
 *   <li>Only {@code UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM} enabled (true, false)
 *   <li>Both enabled (true, true)
 * </ul>
 */
@ExtendWith(ParameterizedTestExtension.class)
class UnalignedCheckpointShuffleChangeITCase extends UnalignedCheckpointTestBase {

    private static final int TOTAL_COUNT = 20_000;

    private static final ConcurrentSkipListSet<Long> SEEN_GLOBAL = new ConcurrentSkipListSet<>();
    private static final AtomicLong POST_RESTORE_DUPLICATES = new AtomicLong();
    private static final AtomicLong CORRUPTIONS = new AtomicLong();

    enum EdgePattern {
        POINTWISE,
        ALL_TO_ALL
    }

    @Parameter(0)
    public String desc;

    @Parameter(1)
    public int oldUpPar;

    @Parameter(2)
    public int oldDownPar;

    @Parameter(3)
    public EdgePattern oldPattern;

    @Parameter(4)
    public int newUpPar;

    @Parameter(5)
    public int newDownPar;

    @Parameter(6)
    public EdgePattern newPattern;

    @Parameter(7)
    public boolean recoverOutputOnDownstream;

    @Parameter(8)
    public boolean checkpointDuringRecovery;

    @Parameters(name = "{0}")
    public static Collection<Object[]> parameters() {
        EdgePattern a2a = EdgePattern.ALL_TO_ALL;
        EdgePattern pw = EdgePattern.POINTWISE;

        return Arrays.asList(
                new Object[][] {
                    // ---- A2A -> A2A ----
                    {"A2A->A2A same", 3, 3, a2a, 3, 3, a2a},
                    {"A2A->A2A up-up", 3, 3, a2a, 4, 3, a2a},
                    {"A2A->A2A up-up wide", 3, 3, a2a, 11, 3, a2a},
                    {"A2A->A2A up-down", 4, 3, a2a, 3, 3, a2a},
                    {"A2A->A2A up-down wide", 11, 3, a2a, 3, 3, a2a},
                    {"A2A->A2A down-up", 3, 3, a2a, 3, 4, a2a},
                    {"A2A->A2A down-up wide", 3, 3, a2a, 3, 11, a2a},
                    {"A2A->A2A down-down", 3, 4, a2a, 3, 3, a2a},
                    {"A2A->A2A down-down wide", 3, 11, a2a, 3, 3, a2a},
                    // ---- A2A -> PW ----
                    {"A2A->PW same", 3, 3, a2a, 3, 3, pw},
                    {"A2A->PW up-up", 3, 3, a2a, 4, 3, pw},
                    {"A2A->PW up-up wide", 3, 3, a2a, 11, 3, pw},
                    {"A2A->PW up-down", 4, 3, a2a, 3, 3, pw},
                    {"A2A->PW up-down wide", 11, 3, a2a, 3, 3, pw},
                    {"A2A->PW down-up", 3, 3, a2a, 3, 4, pw},
                    {"A2A->PW down-up wide", 3, 3, a2a, 3, 11, pw},
                    {"A2A->PW down-down", 3, 4, a2a, 3, 3, pw},
                    {"A2A->PW down-down wide", 3, 11, a2a, 3, 3, pw},
                    // ---- PW -> A2A ----
                    {"PW->A2A same", 3, 3, pw, 3, 3, a2a},
                    {"PW->A2A up-up", 3, 3, pw, 4, 3, a2a},
                    {"PW->A2A up-up wide", 3, 3, pw, 11, 3, a2a},
                    {"PW->A2A up-down", 4, 3, pw, 3, 3, a2a},
                    {"PW->A2A up-down wide", 11, 3, pw, 3, 3, a2a},
                    {"PW->A2A down-up", 3, 3, pw, 3, 4, a2a},
                    {"PW->A2A down-up wide", 3, 3, pw, 3, 11, a2a},
                    {"PW->A2A down-down", 3, 4, pw, 3, 3, a2a},
                    {"PW->A2A down-down wide", 3, 11, pw, 3, 3, a2a},
                    // ---- PW -> PW ----
                    {"PW->PW same", 3, 3, pw, 3, 3, pw},
                    {"PW->PW up-up", 3, 3, pw, 4, 3, pw},
                    {"PW->PW up-up wide", 3, 3, pw, 11, 3, pw},
                    {"PW->PW up-down", 4, 3, pw, 3, 3, pw},
                    {"PW->PW up-down wide", 11, 3, pw, 3, 3, pw},
                    {"PW->PW down-up", 3, 3, pw, 3, 4, pw},
                    {"PW->PW down-up wide", 3, 3, pw, 3, 11, pw},
                    {"PW->PW down-down", 3, 4, pw, 3, 3, pw},
                    {"PW->PW down-down wide", 3, 11, pw, 3, 3, pw},
                    {"PW->PW both-up", 3, 3, pw, 4, 4, pw},
                    {"PW->PW both-down", 4, 4, pw, 3, 3, pw},
                });
    }

    @BeforeEach
    void setup() {
        SEEN_GLOBAL.clear();
        POST_RESTORE_DUPLICATES.set(0);
        CORRUPTIONS.set(0);
    }

    @TestTemplate
    void testShuffleChangeWithUnalignedCheckpoint(TestInfo testInfo) throws Exception {
        // Phase 1: run with old topology, generate checkpoint, cancel
        UnalignedSettings phase1Settings =
                createSettings(oldPattern, oldUpPar, oldDownPar)
                        .setCheckpointGenerationMode(
                                CheckpointGenerationMode.WAIT_FOR_CHECKPOINT_AND_CANCEL);
        String checkpointPath = super.execute(phase1Settings, testInfo);
        assertThat(checkpointPath)
                .as("Phase 1 must generate a checkpoint for restore test to be valid.")
                .isNotNull();

        // Phase 2: restore with new topology and run to completion
        UnalignedSettings phase2Settings =
                createSettings(newPattern, newUpPar, newDownPar)
                        .setRestoreCheckpoint(checkpointPath)
                        .setExpectedFinalJobStatus(JobStatus.FINISHED);
        super.execute(phase2Settings, testInfo);

        assertThat(CORRUPTIONS.get())
                .as("records with bad header (mis-routed or wrongly-decoded buffers)")
                .isZero();
        assertThat(POST_RESTORE_DUPLICATES.get())
                .as("post-restore duplicates (replayed buffer delivered twice to a sink subtask)")
                .isZero();
        assertThat(SEEN_GLOBAL)
                .as("union of all sink output must cover [0, %d)", TOTAL_COUNT)
                .hasSize(TOTAL_COUNT);
        assertThat(SEEN_GLOBAL.first()).isZero();
        assertThat(SEEN_GLOBAL.last()).isEqualTo((long) (TOTAL_COUNT - 1));
    }

    @Override
    protected void checkCounters(JobExecutionResult result) {}

    private UnalignedSettings createSettings(EdgePattern pattern, int upPar, int downPar) {
        UnalignedSettings settings =
                new UnalignedSettings(new ShuffleChangeDagCreator(pattern, upPar, downPar));
        settings.setParallelism(Math.max(upPar, downPar));
        settings.setChannelTypes(ChannelType.LOCAL);
        return settings;
    }

    // -------------------------------------------------------------------------
    //  DAG creation
    // -------------------------------------------------------------------------

    private static class ShuffleChangeDagCreator implements DagCreator {
        private final EdgePattern pattern;
        private final int upPar;
        private final int downPar;

        ShuffleChangeDagCreator(EdgePattern pattern, int upPar, int downPar) {
            this.pattern = pattern;
            this.upPar = upPar;
            this.downPar = downPar;
        }

        @Override
        public void create(
                StreamExecutionEnvironment env,
                int minCheckpoints,
                boolean slotSharing,
                int expectedFailuresUntilSourceFinishes,
                long sourceSleepMs) {
            env.disableOperatorChaining();

            DataGeneratorSource<Long> source =
                    new DataGeneratorSource<>(
                            UnalignedCheckpointTestBase::withHeader,
                            TOTAL_COUNT,
                            RateLimiterStrategy.perSecond(5000),
                            Types.LONG);

            DataStream<Long> sourceStream =
                    env.fromSource(source, WatermarkStrategy.noWatermarks(), "source")
                            .uid("source")
                            .setParallelism(upPar);

            DataStream<Long> shuffled =
                    (pattern == EdgePattern.POINTWISE)
                            ? sourceStream.rescale()
                            : sourceStream.rebalance();

            shuffled.map(
                            x -> {
                                Thread.sleep(1);
                                return x;
                            })
                    .returns(Types.LONG)
                    .name("map")
                    .uid("map")
                    .setParallelism(downPar)
                    .addSink(new VerifyingSink())
                    .uid("sink")
                    .name("sink")
                    .setParallelism(downPar);
        }
    }

    // -------------------------------------------------------------------------
    //  Verification sink
    // -------------------------------------------------------------------------

    private static class VerifyingSink extends RichSinkFunction<Long>
            implements CheckpointedFunction {
        private transient ListState<Long> seenState;
        private transient HashSet<Long> seen;

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            seenState =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("seen", Types.LONG));
            seen = new HashSet<>();
            for (Long v : seenState.get()) {
                seen.add(v);
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            seenState.update(new ArrayList<>(seen));
        }

        @Override
        public void invoke(Long value, Context context) {
            try {
                long base = withoutHeader(value);
                if (!seen.add(base)) {
                    POST_RESTORE_DUPLICATES.incrementAndGet();
                }
                SEEN_GLOBAL.add(base);
            } catch (IllegalArgumentException e) {
                CORRUPTIONS.incrementAndGet();
            }
        }
    }
}
