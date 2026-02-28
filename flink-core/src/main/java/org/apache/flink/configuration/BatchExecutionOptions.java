/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.CoreOptions.DEFAULT_PARALLELISM;
import static org.apache.flink.configuration.JobManagerOptions.SCHEDULER;
import static org.apache.flink.configuration.description.TextElement.code;

/** Configuration options for the batch job execution. */
@PublicEvolving
public class BatchExecutionOptions {

    @Documentation.Section({Documentation.Sections.EXPERT_SCHEDULING})
    public static final ConfigOption<Boolean> ADAPTIVE_AUTO_PARALLELISM_ENABLED =
            key("execution.batch.adaptive.auto-parallelism.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "If true, Flink will automatically decide the parallelism of operators in batch jobs.");

    @Documentation.Section({Documentation.Sections.EXPERT_SCHEDULING})
    public static final ConfigOption<Integer> ADAPTIVE_AUTO_PARALLELISM_MIN_PARALLELISM =
            key("execution.batch.adaptive.auto-parallelism.min-parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDeprecatedKeys("jobmanager.adaptive-batch-scheduler.min-parallelism")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The lower bound of allowed parallelism to set adaptively if %s has been set to %s",
                                            code(SCHEDULER.key()),
                                            code(
                                                    JobManagerOptions.SchedulerType.AdaptiveBatch
                                                            .name()))
                                    .build());

    @Documentation.Section({Documentation.Sections.EXPERT_SCHEDULING})
    public static final ConfigOption<Integer> ADAPTIVE_AUTO_PARALLELISM_MAX_PARALLELISM =
            key("execution.batch.adaptive.auto-parallelism.max-parallelism")
                    .intType()
                    .defaultValue(128)
                    .withDeprecatedKeys("jobmanager.adaptive-batch-scheduler.max-parallelism")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The upper bound of allowed parallelism to set adaptively if %s has been set to %s",
                                            code(SCHEDULER.key()),
                                            code(
                                                    JobManagerOptions.SchedulerType.AdaptiveBatch
                                                            .name()))
                                    .build());

    @Documentation.Section({Documentation.Sections.EXPERT_SCHEDULING})
    public static final ConfigOption<MemorySize>
            ADAPTIVE_AUTO_PARALLELISM_AVG_DATA_VOLUME_PER_TASK =
                    key("execution.batch.adaptive.auto-parallelism.avg-data-volume-per-task")
                            .memoryType()
                            .defaultValue(MemorySize.ofMebiBytes(16))
                            .withDeprecatedKeys(
                                    "jobmanager.adaptive-batch-scheduler.avg-data-volume-per-task")
                            .withDescription(
                                    Description.builder()
                                            .text(
                                                    "The average size of data volume to expect each task instance to process if %s has been set to %s. "
                                                            + "Note that when data skew occurs or the decided parallelism reaches the %s (due to too much data), "
                                                            + "the data actually processed by some tasks may far exceed this value.",
                                                    code(SCHEDULER.key()),
                                                    code(
                                                            JobManagerOptions.SchedulerType
                                                                    .AdaptiveBatch.name()),
                                                    code(
                                                            ADAPTIVE_AUTO_PARALLELISM_MAX_PARALLELISM
                                                                    .key()))
                                            .build());

    @Documentation.Section({Documentation.Sections.EXPERT_SCHEDULING})
    public static final ConfigOption<Integer> ADAPTIVE_AUTO_PARALLELISM_DEFAULT_SOURCE_PARALLELISM =
            key("execution.batch.adaptive.auto-parallelism.default-source-parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDeprecatedKeys(
                            "jobmanager.adaptive-batch-scheduler.default-source-parallelism")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The default parallelism of source vertices or the upper bound of source parallelism "
                                                    + "to set adaptively if %s has been set to %s. Note that %s will be used if this configuration is not configured. "
                                                    + "If %s is not set either, then the default parallelism set via %s will be used instead.",
                                            code(SCHEDULER.key()),
                                            code(
                                                    JobManagerOptions.SchedulerType.AdaptiveBatch
                                                            .name()),
                                            code(ADAPTIVE_AUTO_PARALLELISM_MAX_PARALLELISM.key()),
                                            code(ADAPTIVE_AUTO_PARALLELISM_MAX_PARALLELISM.key()),
                                            code(DEFAULT_PARALLELISM.key()))
                                    .build());

    @Documentation.Section({Documentation.Sections.EXPERT_SCHEDULING})
    public static final ConfigOption<Boolean> SPECULATIVE_ENABLED =
            key("execution.batch.speculative.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDeprecatedKeys("jobmanager.adaptive-batch-scheduler.speculative.enabled")
                    .withDescription("Controls whether to enable speculative execution.");

    @Documentation.Section({Documentation.Sections.EXPERT_SCHEDULING})
    public static final ConfigOption<Integer> SPECULATIVE_MAX_CONCURRENT_EXECUTIONS =
            key("execution.batch.speculative.max-concurrent-executions")
                    .intType()
                    .defaultValue(2)
                    .withDeprecatedKeys(
                            "jobmanager.adaptive-batch-scheduler.speculative.max-concurrent-executions")
                    .withDescription(
                            "Controls the maximum number of execution attempts of each operator "
                                    + "that can execute concurrently, including the original one "
                                    + "and speculative ones.");

    @Documentation.Section({Documentation.Sections.EXPERT_SCHEDULING})
    public static final ConfigOption<Duration> BLOCK_SLOW_NODE_DURATION =
            key("execution.batch.speculative.block-slow-node-duration")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDeprecatedKeys(
                            "jobmanager.adaptive-batch-scheduler.speculative.block-slow-node-duration")
                    .withDescription(
                            "Controls how long an detected slow node should be blocked for.");

    @Documentation.Section({Documentation.Sections.EXPERT_SCHEDULING})
    public static final ConfigOption<Boolean> JOB_RECOVERY_ENABLED =
            key("execution.batch.job-recovery.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "A flag to enable or disable the job recovery. If enabled, batch jobs "
                                    + "can resume with previously generated intermediate results "
                                    + "after job master restarts due to failures, thereby preserving the progress.");

    @Documentation.Section({Documentation.Sections.EXPERT_SCHEDULING})
    public static final ConfigOption<Duration> JOB_RECOVERY_PREVIOUS_WORKER_RECOVERY_TIMEOUT =
            key("execution.batch.job-recovery.previous-worker.recovery.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "The timeout for a new job master to wait for the previous worker to reconnect."
                                    + "A reconnected worker will transmit the details of its produced intermediate "
                                    + "results to the new job master, enabling the job master to reuse these results.");

    @Documentation.Section({Documentation.Sections.EXPERT_SCHEDULING})
    public static final ConfigOption<Duration> JOB_RECOVERY_SNAPSHOT_MIN_PAUSE =
            key("execution.batch.job-recovery.snapshot.min-pause")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(3))
                    .withDescription(
                            "The minimal pause between snapshots taken by operator coordinator or other components. "
                                    + "It is used to avoid performance degradation due to excessive snapshot frequency.");

    private BatchExecutionOptions() {
        throw new UnsupportedOperationException("This class should never be instantiated.");
    }
}
