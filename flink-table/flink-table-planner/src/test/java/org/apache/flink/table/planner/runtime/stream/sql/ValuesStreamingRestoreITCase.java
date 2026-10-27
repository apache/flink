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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * VALUES waits for one more checkpoint after emitting its rows before it exits (FLIP-147). That
 * checkpoint is triggered while the task is still running, so it doesn't mark VALUES as finished. A
 * job restored from it must not read the VALUES rows again.
 *
 * <p>Both tests restore after VALUES emitted all its rows. A restore halfway through the rows is
 * only tested by {@code ValuesCodeGeneratorTest#testRestoreEmitsOnlyTheRemainingRows}.
 */
class ValuesStreamingRestoreITCase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    @TempDir private Path tmpDir;

    private final List<JobClient> jobs = new ArrayList<>();

    @BeforeEach
    void before() {
        TestValuesTableFactory.clearAllData();
    }

    @AfterEach
    void after() throws Exception {
        // The datagen source never ends, so a job that failed half way would otherwise leak.
        for (JobClient job : jobs) {
            if (!job.getJobStatus().get().isGloballyTerminalState()) {
                cancel(job);
            }
        }
        TestValuesTableFactory.clearAllData();
    }

    /** Checkpoint N, the one VALUES waits for, doesn't mark it as finished. */
    @Test
    void testRestoreFromCheckpointTakenBeforeValuesExits(
            @InjectClusterClient RestClusterClient<?> client,
            @InjectMiniCluster MiniCluster miniCluster)
            throws Exception {
        assertNoDuplicatesAfterRestore(client, miniCluster, false);
    }

    /**
     * Checkpoint N+1 is triggered after VALUES exits, so it marks VALUES as finished. The legacy
     * source passes this case, too.
     */
    @Test
    void testRestoreFromCheckpointTakenAfterValuesExits(
            @InjectClusterClient RestClusterClient<?> client,
            @InjectMiniCluster MiniCluster miniCluster)
            throws Exception {
        assertNoDuplicatesAfterRestore(client, miniCluster, true);
    }

    private void assertNoDuplicatesAfterRestore(
            RestClusterClient<?> client, MiniCluster miniCluster, boolean afterValuesExits)
            throws Exception {
        JobClient job = startJob(null);
        waitUntilCondition(() -> rowsOf("[123]").size() == 3);
        assertThat(valuesState(client, job)).isEqualTo(ExecutionState.RUNNING);

        // The first checkpoint after VALUES emitted its rows. VALUES exits once it completes. The
        // periodic checkpoints are an hour apart, so VALUES can't exit before this one is taken.
        long checkpointId = client.triggerCheckpoint(job.getJobID(), CheckpointType.FULL).get();
        waitUntilCondition(() -> valuesState(client, job) == ExecutionState.FINISHED);
        if (afterValuesExits) {
            checkpointId = client.triggerCheckpoint(job.getJobID(), CheckpointType.FULL).get();
        }
        cancel(job);

        JobClient restored =
                startJob(
                        tmpDir.resolve(job.getJobID().toString())
                                .resolve("chk-" + checkpointId)
                                .toUri()
                                .toString());
        // VALUES waits for one more checkpoint before it exits.
        waitForAllTaskRunning(miniCluster, restored.getJobID(), true);
        client.triggerCheckpoint(restored.getJobID(), CheckpointType.FULL).get();
        waitUntilCondition(() -> valuesState(client, restored) == ExecutionState.FINISHED);
        cancel(restored);

        // A count of 2 means the restored job read the VALUES rows again.
        assertThat(rowsOf("[123]")).containsExactlyInAnyOrder("+I[1, 1]", "+I[2, 1]", "+I[3, 1]");
    }

    private JobClient startJob(String checkpoint) {
        Configuration conf = new Configuration();
        conf.set(CoreOptions.DEFAULT_PARALLELISM, 2);
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        // Checkpoints are triggered by the test only. The scheduler may trigger its first
        // checkpoint after the min pause instead of the interval, so both are long.
        conf.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofHours(1));
        conf.set(CheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, Duration.ofHours(1));
        conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, tmpDir.toUri().toString());
        conf.set(
                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        if (checkpoint != null) {
            conf.set(StateRecoveryOptions.SAVEPOINT_PATH, checkpoint);
        }
        TableEnvironment env =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().withConfiguration(conf).build());
        env.executeSql(
                "CREATE TABLE gen (id INT) WITH ('connector' = 'datagen', 'rows-per-second' = '5')");
        env.executeSql(
                "CREATE TABLE snk (id INT, cnt BIGINT, PRIMARY KEY (id) NOT ENFORCED)"
                        + " WITH ('connector' = 'values', 'sink-insert-only' = 'false')");
        JobClient job =
                env.executeSql(
                                "INSERT INTO snk SELECT id, COUNT(*) FROM ("
                                        + " SELECT id FROM (VALUES (1), (2), (3)) AS v(id)"
                                        + " UNION ALL SELECT 1000 AS id FROM gen"
                                        + ") GROUP BY id")
                        .getJobClient()
                        .orElseThrow();
        jobs.add(job);
        return job;
    }

    /** The state of the VALUES vertex, or null before the job has its vertices. */
    private static ExecutionState valuesState(RestClusterClient<?> client, JobClient job)
            throws Exception {
        return client.getJobDetails(job.getJobID()).get().getJobVertexInfos().stream()
                .filter(vertex -> vertex.getName().startsWith("Source: Values"))
                .findFirst()
                .map(JobDetailsInfo.JobVertexDetailsInfo::getExecutionState)
                .orElse(null);
    }

    /** Raw changelog rows of the sink whose key matches the given pattern. */
    private static List<String> rowsOf(String keyPattern) {
        return TestValuesTableFactory.getRawResultsAsStrings("snk").stream()
                .filter(row -> row.matches("[+-][IUD]\\[" + keyPattern + ", .*"))
                .collect(Collectors.toList());
    }

    private static void cancel(JobClient job) throws Exception {
        job.cancel().get();
        waitUntilCondition(() -> job.getJobStatus().get().isGloballyTerminalState());
    }
}
