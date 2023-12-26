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

package org.apache.flink.streaming.api.environment;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;
import org.apache.flink.streaming.api.CheckpointingMode;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.LinkElement.link;

/**
 * Execution {@link ConfigOption} for configuring checkpointing related parameters.
 *
 * @see CheckpointConfig
 */
@PublicEvolving
public class ExecutionCheckpointingOptions {
    public static final ConfigOption<CheckpointingMode> CHECKPOINTING_MODE =
            ConfigOptions.key("execution.checkpointing.mode")
                    .enumType(CheckpointingMode.class)
                    .defaultValue(CheckpointingMode.EXACTLY_ONCE)
                    .withDescription("The checkpointing mode (exactly-once vs. at-least-once).");

    public static final ConfigOption<Duration> CHECKPOINTING_TIMEOUT =
            ConfigOptions.key("execution.checkpointing.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(10))
                    .withDescription(
                            "The maximum time that a checkpoint may take before being discarded.");

    public static final ConfigOption<Integer> MAX_CONCURRENT_CHECKPOINTS =
            ConfigOptions.key("execution.checkpointing.max-concurrent-checkpoints")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The maximum number of checkpoint attempts that may be in progress at the same time. If "
                                    + "this value is n, then no checkpoints will be triggered while n checkpoint attempts are currently in "
                                    + "flight. For the next checkpoint to be triggered, one checkpoint attempt would need to finish or "
                                    + "expire.");

    public static final ConfigOption<Duration> MIN_PAUSE_BETWEEN_CHECKPOINTS =
            ConfigOptions.key("execution.checkpointing.min-pause")
                    .durationType()
                    .defaultValue(Duration.ZERO)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The minimal pause between checkpointing attempts. This setting defines how soon the"
                                                    + "checkpoint coordinator may trigger another checkpoint after it becomes possible to trigger"
                                                    + "another checkpoint with respect to the maximum number of concurrent checkpoints"
                                                    + "(see %s).",
                                            TextElement.code(MAX_CONCURRENT_CHECKPOINTS.key()))
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "If the maximum number of concurrent checkpoints is set to one, this setting makes effectively "
                                                    + "sure that a minimum amount of time passes where no checkpoint is in progress at all.")
                                    .build());

    public static final ConfigOption<Integer> TOLERABLE_FAILURE_NUMBER =
            ConfigOptions.key("execution.checkpointing.tolerable-failed-checkpoints")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The tolerable checkpoint consecutive failure number. If set to 0, that means "
                                    + "we do not tolerance any checkpoint failure. This only applies to the following failure reasons: IOException on the "
                                    + "Job Manager, failures in the async phase on the Task Managers and checkpoint expiration due to a timeout. Failures "
                                    + "originating from the sync phase on the Task Managers are always forcing failover of an affected task. Other types of "
                                    + "checkpoint failures (such as checkpoint being subsumed) are being ignored.");

    public static final ConfigOption<CheckpointConfig.ExternalizedCheckpointCleanup>
            EXTERNALIZED_CHECKPOINT =
                    ConfigOptions.key("execution.checkpointing.externalized-checkpoint-retention")
                            .enumType(CheckpointConfig.ExternalizedCheckpointCleanup.class)
                            .defaultValue(
                                    CheckpointConfig.ExternalizedCheckpointCleanup
                                            .NO_EXTERNALIZED_CHECKPOINTS)
                            .withDescription(
                                    Description.builder()
                                            .text(
                                                    "Externalized checkpoints write their meta data out to persistent storage and are not "
                                                            + "automatically cleaned up when the owning job fails or is suspended (terminating with job "
                                                            + "status %s or %s). In this case, you have to manually clean up the checkpoint state, both the "
                                                            + "meta data and actual program state.",
                                                    TextElement.code("JobStatus#FAILED"),
                                                    TextElement.code("JobStatus#SUSPENDED"))
                                            .linebreak()
                                            .linebreak()
                                            .text(
                                                    "The mode defines how an externalized checkpoint should be cleaned up on job cancellation. If "
                                                            + "you choose to retain externalized checkpoints on cancellation you have to handle checkpoint "
                                                            + "clean up manually when you cancel the job as well (terminating with job status %s).",
                                                    TextElement.code("JobStatus#CANCELED"))
                                            .linebreak()
                                            .linebreak()
                                            .text(
                                                    "The target directory for externalized checkpoints is configured via %s.",
                                                    TextElement.code(
                                                            CheckpointingOptions
                                                                    .CHECKPOINTS_DIRECTORY
                                                                    .key()))
                                            .build());

    public static final ConfigOption<Duration> CHECKPOINTING_INTERVAL_DURING_BACKLOG =
            ConfigOptions.key("execution.checkpointing.interval-during-backlog")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If it is not null and any source reports isProcessingBacklog=true, "
                                                    + "it is the interval in which checkpoints are periodically scheduled.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "Checkpoint triggering may be delayed by the settings %s and %s.",
                                            TextElement.code(MAX_CONCURRENT_CHECKPOINTS.key()),
                                            TextElement.code(MIN_PAUSE_BETWEEN_CHECKPOINTS.key()))
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "Note: if it is not null, the value must either be 0, "
                                                    + "which means the checkpoint is disabled during backlog, "
                                                    + "or be larger than or equal to execution.checkpointing.interval.")
                                    .build());

    public static final ConfigOption<Duration> CHECKPOINTING_INTERVAL =
            ConfigOptions.key("execution.checkpointing.interval")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Gets the interval in which checkpoints are periodically scheduled.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "This setting defines the base interval. Checkpoint triggering may be delayed by the settings "
                                                    + "%s, %s and %s",
                                            TextElement.code(MAX_CONCURRENT_CHECKPOINTS.key()),
                                            TextElement.code(MIN_PAUSE_BETWEEN_CHECKPOINTS.key()),
                                            TextElement.code(
                                                    CHECKPOINTING_INTERVAL_DURING_BACKLOG.key()))
                                    .build());

    public static final ConfigOption<Boolean> ENABLE_UNALIGNED =
            ConfigOptions.key("execution.checkpointing.unaligned.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDeprecatedKeys("execution.checkpointing.unaligned")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Enables unaligned checkpoints, which greatly reduce checkpointing times under backpressure.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "Unaligned checkpoints contain data stored in buffers as part of the checkpoint state, which "
                                                    + "allows checkpoint barriers to overtake these buffers. Thus, the checkpoint duration becomes "
                                                    + "independent of the current throughput as checkpoint barriers are effectively not embedded into "
                                                    + "the stream of data anymore.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "Unaligned checkpoints can only be enabled if %s is %s and if %s is 1",
                                            TextElement.code(CHECKPOINTING_MODE.key()),
                                            TextElement.code(
                                                    CheckpointingMode.EXACTLY_ONCE.toString()),
                                            TextElement.code(MAX_CONCURRENT_CHECKPOINTS.key()))
                                    .build());

    public static final ConfigOption<Duration> ALIGNED_CHECKPOINT_TIMEOUT =
            ConfigOptions.key("execution.checkpointing.aligned-checkpoint-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(0L))
                    .withDeprecatedKeys("execution.checkpointing.alignment-timeout")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Only relevant if %s is enabled.",
                                            TextElement.code(ENABLE_UNALIGNED.key()))
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "If timeout is 0, checkpoints will always start unaligned.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "If timeout has a positive value, checkpoints will start aligned. "
                                                    + "If during checkpointing, checkpoint start delay exceeds this timeout, alignment "
                                                    + "will timeout and checkpoint barrier will start working as unaligned checkpoint.")
                                    .build());

    /** @deprecated Use {@link #ALIGNED_CHECKPOINT_TIMEOUT} instead. */
    @Deprecated
    public static final ConfigOption<Duration> ALIGNMENT_TIMEOUT =
            ConfigOptions.key("execution.checkpointing.alignment-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(0L))
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Deprecated. %s should be used instead. Only relevant if %s is enabled.",
                                            TextElement.code(ALIGNED_CHECKPOINT_TIMEOUT.key()),
                                            TextElement.code(ENABLE_UNALIGNED.key()))
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "If timeout is 0, checkpoints will always start unaligned.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "If timeout has a positive value, checkpoints will start aligned. "
                                                    + "If during checkpointing, checkpoint start delay exceeds this timeout, alignment "
                                                    + "will timeout and checkpoint barrier will start working as unaligned checkpoint.")
                                    .build());

    public static final ConfigOption<Boolean> FORCE_UNALIGNED =
            ConfigOptions.key("execution.checkpointing.unaligned.forced")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Forces unaligned checkpoints, particularly allowing them for iterative jobs.")
                                    .build());

    public static final ConfigOption<Long> CHECKPOINT_ID_OF_IGNORED_IN_FLIGHT_DATA =
            ConfigOptions.key("execution.checkpointing.recover-without-channel-state.checkpoint-id")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Checkpoint id for which in-flight data should be ignored in case of the recovery from this checkpoint.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "It is better to keep this value empty until "
                                                    + "there is explicit needs to restore from "
                                                    + "the specific checkpoint without in-flight data.")
                                    .linebreak()
                                    .build());

    public static final ConfigOption<Boolean> ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH =
            ConfigOptions.key("execution.checkpointing.checkpoints-after-tasks-finish.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Feature toggle for enabling checkpointing even if some of tasks"
                                                    + " have finished. Before you enable it, please take a look at %s ",
                                            link(
                                                    "{{.Site.BaseURL}}{{.Site.LanguagePrefix}}/docs/dev/datastream/fault-tolerance/checkpointing/#checkpointing-with-parts-of-the-graph-finished-beta",
                                                    "the important considerations"))
                                    .build());

    /**
     * Access to this option is officially only supported via {@link
     * CheckpointConfig#setForceCheckpointing(boolean)}, but there is no good reason behind this.
     *
     * @deprecated This will be removed once iterations properly participate in checkpointing.
     */
    @Internal @Deprecated @Documentation.ExcludeFromDocumentation
    public static final ConfigOption<Boolean> FORCE_CHECKPOINTING =
            key("execution.checkpointing.force")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Flag to force checkpointing in iterative jobs.");

    /**
     * Access to this option is officially only supported via {@link
     * CheckpointConfig#enableApproximateLocalRecovery(boolean)}, but there is no good reason behind
     * this.
     */
    @Internal @Documentation.ExcludeFromDocumentation
    public static final ConfigOption<Boolean> APPROXIMATE_LOCAL_RECOVERY =
            key("execution.checkpointing.approximate-local-recovery")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Flag to enable approximate local recovery.");

    public static final ConfigOption<Integer> UNALIGNED_MAX_SUBTASKS_PER_CHANNEL_STATE_FILE =
            key("execution.checkpointing.unaligned.max-subtasks-per-channel-state-file")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "Defines the maximum number of subtasks that share the same channel state file. "
                                    + "It can reduce the number of small files when enable unaligned checkpoint. "
                                    + "Each subtask will create a new channel state file when this is configured to 1.");
}
