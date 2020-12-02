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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;
import org.apache.flink.streaming.api.CheckpointingMode;

import java.time.Duration;

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
		.withDescription("The maximum time that a checkpoint may take before being discarded.");

	public static final ConfigOption<Integer> MAX_CONCURRENT_CHECKPOINTS =
		ConfigOptions.key("execution.checkpointing.max-concurrent-checkpoints")
			.intType()
			.defaultValue(1)
			.withDescription("The maximum number of checkpoint attempts that may be in progress at the same time. If " +
				"this value is n, then no checkpoints will be triggered while n checkpoint attempts are currently in " +
				"flight. For the next checkpoint to be triggered, one checkpoint attempt would need to finish or " +
				"expire.");

	public static final ConfigOption<Duration> MIN_PAUSE_BETWEEN_CHECKPOINTS =
		ConfigOptions.key("execution.checkpointing.min-pause")
			.durationType()
			.defaultValue(Duration.ZERO)
			.withDescription(Description.builder()
				.text("The minimal pause between checkpointing attempts. This setting defines how soon the" +
					"checkpoint coordinator may trigger another checkpoint after it becomes possible to trigger" +
					"another checkpoint with respect to the maximum number of concurrent checkpoints" +
					"(see %s).", TextElement.code(MAX_CONCURRENT_CHECKPOINTS.key()))
				.linebreak()
				.linebreak()
				.text("If the maximum number of concurrent checkpoints is set to one, this setting makes effectively " +
					"sure that a minimum amount of time passes where no checkpoint is in progress at all.")
				.build());

	public static final ConfigOption<Boolean> PREFER_CHECKPOINT_FOR_RECOVERY =
		ConfigOptions.key("execution.checkpointing.prefer-checkpoint-for-recovery")
			.booleanType()
			.defaultValue(false)
			.withDescription("If enabled, a job recovery should fallback to checkpoint when there is a more recent " +
				"savepoint.");

	public static final ConfigOption<Integer> TOLERABLE_FAILURE_NUMBER =
		ConfigOptions.key("execution.checkpointing.tolerable-failed-checkpoints")
			.intType()
			.noDefaultValue()
			.withDescription("The tolerable checkpoint failure number. If set to 0, that means" +
				"we do not tolerance any checkpoint failure.");

	public static final ConfigOption<CheckpointConfig.ExternalizedCheckpointCleanup> EXTERNALIZED_CHECKPOINT =
		ConfigOptions.key("execution.checkpointing.externalized-checkpoint-retention")
			.enumType(CheckpointConfig.ExternalizedCheckpointCleanup.class)
			.noDefaultValue()
			.withDescription(Description.builder()
				.text(
					"Externalized checkpoints write their meta data out to persistent storage and are not " +
						"automatically cleaned up when the owning job fails or is suspended (terminating with job " +
						"status %s or %s. In this case, you have to manually clean up the checkpoint state, both the " +
						"meta data and actual program state.",
					TextElement.code("JobStatus#FAILED"),
					TextElement.code("JobStatus#SUSPENDED"))
				.linebreak()
				.linebreak()
				.text(
					"The mode defines how an externalized checkpoint should be cleaned up on job cancellation. If " +
						"you choose to retain externalized checkpoints on cancellation you have to handle checkpoint " +
						"clean up manually when you cancel the job as well (terminating with job status %s).",
					TextElement.code("JobStatus#CANCELED"))
				.linebreak()
				.linebreak()
				.text(
					"The target directory for externalized checkpoints is configured via %s.",
					TextElement.code(CheckpointingOptions.CHECKPOINTS_DIRECTORY.key())).build());

	public static final ConfigOption<Duration> CHECKPOINTING_INTERVAL =
		ConfigOptions.key("execution.checkpointing.interval")
			.durationType()
			.noDefaultValue()
			.withDescription(Description.builder()
				.text("Gets the interval in which checkpoints are periodically scheduled.")
				.linebreak()
				.linebreak()
				.text("This setting defines the base interval. Checkpoint triggering may be delayed by the settings " +
						"%s and %s",
					TextElement.code(MAX_CONCURRENT_CHECKPOINTS.key()),
					TextElement.code(MIN_PAUSE_BETWEEN_CHECKPOINTS.key()))
				.build());

	public static final ConfigOption<Boolean> ENABLE_UNALIGNED =
		ConfigOptions.key("execution.checkpointing.unaligned")
			.booleanType()
			.defaultValue(false)
			.withDescription(Description.builder()
				.text("Enables unaligned checkpoints, which greatly reduce checkpointing times under backpressure.")
				.linebreak()
				.linebreak()
				.text("Unaligned checkpoints contain data stored in buffers as part of the checkpoint state, which " +
					"allows checkpoint barriers to overtake these buffers. Thus, the checkpoint duration becomes " +
					"independent of the current throughput as checkpoint barriers are effectively not embedded into " +
					"the stream of data anymore.")
				.linebreak()
				.linebreak()
				.text("Unaligned checkpoints can only be enabled if %s is %s and if %s is 1",
					TextElement.code(CHECKPOINTING_MODE.key()),
					TextElement.code(CheckpointingMode.EXACTLY_ONCE.toString()),
					TextElement.code(MAX_CONCURRENT_CHECKPOINTS.key()))
				.build());

	public static final ConfigOption<Duration> ALIGNMENT_TIMEOUT =
		ConfigOptions.key("execution.checkpointing.alignment-timeout")
			.durationType()
			.defaultValue(Duration.ofSeconds(0L))
			.withDescription(Description.builder()
				.text("Only relevant if %s is enabled.", TextElement.code(ENABLE_UNALIGNED.key()))
				.linebreak()
				.linebreak()
				.text("If timeout is 0, checkpoints will always start unaligned.")
				.linebreak()
				.linebreak()
				.text("If timeout has a positive value, checkpoints will start aligned. " +
					"If during checkpointing, checkpoint start delay exceeds this timeout, alignment " +
					"will timeout and checkpoint barrier will start working as unaligned checkpoint.")
				.build());

	@Documentation.ExcludeFromDocumentation("Do not advertise this option until rescaling of unaligned checkpoint is completed.")
	public static final ConfigOption<Boolean> FORCE_UNALIGNED =
		ConfigOptions.key("execution.checkpointing.unaligned.forced")
			.booleanType()
			.defaultValue(true)
			.withDescription(Description.builder()
				.text("Forces unaligned checkpoints, particularly allowing them for iterative jobs.")
				.build());
}
