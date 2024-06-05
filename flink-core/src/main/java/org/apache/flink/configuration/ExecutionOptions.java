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

package org.apache.flink.configuration;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.description.Description;

import java.time.Duration;

import static org.apache.flink.configuration.description.TextElement.text;

/** {@link ConfigOption}s specific for a single execution of a user program. */
@PublicEvolving
public class ExecutionOptions {
    /** A special marker value for disabling buffer timeout. */
    public static final long DISABLED_NETWORK_BUFFER_TIMEOUT = -1L;

    /** A special marker value for flushing network buffers after each record. */
    public static final long FLUSH_AFTER_EVERY_RECORD = 0L;

    public static final ConfigOption<RuntimeExecutionMode> RUNTIME_MODE =
            ConfigOptions.key("execution.runtime-mode")
                    .enumType(RuntimeExecutionMode.class)
                    .defaultValue(RuntimeExecutionMode.STREAMING)
                    .withDescription(
                            "Runtime execution mode of DataStream programs. Among other things, "
                                    + "this controls task scheduling, network shuffle behavior, and time semantics.");

    public static final ConfigOption<BatchShuffleMode> BATCH_SHUFFLE_MODE =
            ConfigOptions.key("execution.batch-shuffle-mode")
                    .enumType(BatchShuffleMode.class)
                    .defaultValue(BatchShuffleMode.ALL_EXCHANGES_BLOCKING)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Defines how data is exchanged between tasks in batch '%s' if the shuffling "
                                                    + "behavior has not been set explicitly for an individual exchange.",
                                            text(RUNTIME_MODE.key()))
                                    .linebreak()
                                    .text(
                                            "With pipelined exchanges, upstream and downstream tasks run simultaneously. "
                                                    + "In order to achieve lower latency, a result record is immediately "
                                                    + "sent to and processed by the downstream task. Thus, the receiver "
                                                    + "back-pressures the sender. The streaming mode always uses this "
                                                    + "exchange.")
                                    .linebreak()
                                    .text(
                                            "With blocking exchanges, upstream and downstream tasks run in stages. "
                                                    + "Records are persisted to some storage between stages. Downstream "
                                                    + "tasks then fetch these records after the upstream tasks finished. "
                                                    + "Such an exchange reduces the resources required to execute the "
                                                    + "job as it does not need to run upstream and downstream "
                                                    + "tasks simultaneously.")
                                    .linebreak()
                                    .text(
                                            "With hybrid exchanges (experimental), downstream tasks can run anytime as "
                                                    + "long as upstream tasks start running. When given sufficient "
                                                    + "resources, it can reduce the overall job execution time by running "
                                                    + "tasks simultaneously. Otherwise, it also allows jobs to be executed "
                                                    + "with very little resources. It adapts to custom preferences between "
                                                    + "persisting less data and restarting less tasks on failures, by "
                                                    + "providing different spilling strategies.")
                                    .build());

    /**
     * Should be moved to {@code ExecutionCheckpointingOptions} along with {@code
     * ExecutionConfig#useSnapshotCompression}, which should be put into {@code CheckpointConfig}.
     */
    public static final ConfigOption<Boolean> SNAPSHOT_COMPRESSION =
            ConfigOptions.key("execution.checkpointing.snapshot-compression")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Tells if we should use compression for the state snapshot data or not");

    public static final ConfigOption<Boolean> BUFFER_TIMEOUT_ENABLED =
            ConfigOptions.key("execution.buffer-timeout.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If disabled, the config execution.buffer-timeout.interval will not take effect and the flushing will be triggered only when the output "
                                                    + "buffer is full thus maximizing throughput")
                                    .build());

    public static final ConfigOption<Duration> BUFFER_TIMEOUT =
            ConfigOptions.key("execution.buffer-timeout.interval")
                    .durationType()
                    .defaultValue(Duration.ofMillis(100))
                    .withDeprecatedKeys("execution.buffer-timeout")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The maximum time frequency (milliseconds) for the flushing of the output buffers. By default "
                                                    + "the output buffers flush frequently to provide low latency and to aid smooth developer "
                                                    + "experience. Setting the parameter can result in three logical modes:")
                                    .list(
                                            text(
                                                    "A positive value triggers flushing periodically by that interval"),
                                            text(
                                                    FLUSH_AFTER_EVERY_RECORD
                                                            + " triggers flushing after every record thus minimizing latency"),
                                            text(
                                                    "If the config "
                                                            + BUFFER_TIMEOUT_ENABLED.key()
                                                            + " is false,"
                                                            + " trigger flushing only when the output buffer is full thus maximizing "
                                                            + "throughput"))
                                    .build());

    public static final ConfigOption<MemorySize> SORT_PARTITION_MEMORY =
            ConfigOptions.key("execution.sort-partition.memory")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription(
                            "Sets the managed memory size for sort partition operator in NonKeyedPartitionWindowedStream."
                                    + "The memory size is only a weight hint. Thus, it will affect the operator's memory weight within a "
                                    + "task, but the actual memory used depends on the running environment.");

    public static final ConfigOption<MemorySize> SORT_KEYED_PARTITION_MEMORY =
            ConfigOptions.key("execution.sort-keyed-partition.memory")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription(
                            "Sets the managed memory size for sort partition operator on KeyedPartitionWindowedStream."
                                    + "The memory size is only a weight hint. Thus, it will affect the operator's memory weight within a "
                                    + "task, but the actual memory used depends on the running environment.");

    @Documentation.ExcludeFromDocumentation(
            "This is an expert option, that we do not want to expose in the documentation")
    public static final ConfigOption<Boolean> SORT_INPUTS =
            ConfigOptions.key("execution.sorted-inputs.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "A flag to enable or disable sorting inputs of keyed operators. "
                                    + "NOTE: It takes effect only in the BATCH runtime mode.");

    @Documentation.ExcludeFromDocumentation(
            "This is an expert option, that we do not want to expose in the documentation")
    public static final ConfigOption<MemorySize> SORTED_INPUTS_MEMORY =
            ConfigOptions.key("execution.sorted-inputs.memory")
                    .memoryType()
                    // in sync with other weights from Table API and DataStream API
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription(
                            "Sets the managed memory size for sorting inputs of keyed operators in "
                                    + "BATCH runtime mode. The memory size is only a weight hint. "
                                    + "Thus, it will affect the operator's memory weight within a "
                                    + "task, but the actual memory used depends on the running "
                                    + "environment.");

    @Documentation.ExcludeFromDocumentation(
            "This is an expert option, that we do not want to expose in the documentation")
    public static final ConfigOption<Boolean> USE_BATCH_STATE_BACKEND =
            ConfigOptions.key("execution.batch-state-backend.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "A flag to enable or disable batch runtime specific state backend and timer service for keyed"
                                    + " operators. NOTE: It takes effect only in the BATCH runtime mode and requires sorted inputs"
                                    + SORT_INPUTS.key()
                                    + " to be enabled.");

    // ------------------------- Async State Execution --------------------------

    /**
     * The max limit of in-flight records number in async state execution, 'in-flight' refers to the
     * records that have entered the operator but have not yet been processed and emitted to the
     * downstream. If the in-flight records number exceeds the limit, the newly records entering
     * will be blocked until the in-flight records number drops below the limit.
     */
    @Experimental
    @Documentation.ExcludeFromDocumentation(
            "This is an experimental option, internal use only for now.")
    public static final ConfigOption<Integer> ASYNC_INFLIGHT_RECORDS_LIMIT =
            ConfigOptions.key("execution.async-state.in-flight-records-limit")
                    .intType()
                    .defaultValue(6000)
                    .withDescription(
                            "The max limit of in-flight records number in async state execution, 'in-flight' refers"
                                    + " to the records that have entered the operator but have not yet been processed and"
                                    + " emitted to the downstream. If the in-flight records number exceeds the limit,"
                                    + " the newly records entering will be blocked until the in-flight records number drops below the limit.");

    /**
     * The size of buffer under async state execution. Async state execution provides a buffer
     * mechanism to reduce state access. When the number of state requests in the buffer exceeds the
     * batch size, a batched state execution would be triggered. Larger batch sizes will bring
     * higher end-to-end latency, this option works with {@link #ASYNC_STATE_BUFFER_TIMEOUT} to
     * control the frequency of triggering.
     */
    @Experimental
    @Documentation.ExcludeFromDocumentation(
            "This is an experimental option, internal use only for now.")
    public static final ConfigOption<Integer> ASYNC_STATE_BUFFER_SIZE =
            ConfigOptions.key("execution.async-state.buffer-size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The size of buffer under async state execution. Async state execution provides a buffer mechanism to reduce state access."
                                    + " When the number of state requests in the active buffer exceeds the batch size,"
                                    + " a batched state execution would be triggered. Larger batch sizes will bring higher end-to-end latency,"
                                    + " this option works with 'execution.async-state.buffer-timeout' to control the frequency of triggering.");

    /**
     * The timeout of buffer triggering in milliseconds. If the buffer has not reached the {@link
     * #ASYNC_STATE_BUFFER_SIZE} within 'buffer-timeout' milliseconds, a trigger will perform
     * actively.
     */
    @Experimental
    @Documentation.ExcludeFromDocumentation(
            "This is an experimental option, internal use only for now.")
    public static final ConfigOption<Long> ASYNC_STATE_BUFFER_TIMEOUT =
            ConfigOptions.key("execution.async-state.buffer-timeout")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription(
                            "The timeout of buffer triggering in milliseconds. If the buffer has not reached the"
                                    + " 'execution.async-state.buffer-size' within 'buffer-timeout' milliseconds,"
                                    + " a trigger will perform actively.");
}
