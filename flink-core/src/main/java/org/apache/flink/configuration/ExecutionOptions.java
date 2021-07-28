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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.ShuffleMode;
import org.apache.flink.configuration.description.Description;

import java.time.Duration;

import static org.apache.flink.configuration.description.TextElement.text;

/** {@link ConfigOption}s specific for a single execution of a user program. */
@PublicEvolving
public class ExecutionOptions {

    public static final ConfigOption<RuntimeExecutionMode> RUNTIME_MODE =
            ConfigOptions.key("execution.runtime-mode")
                    .enumType(RuntimeExecutionMode.class)
                    .defaultValue(RuntimeExecutionMode.STREAMING)
                    .withDescription(
                            "Runtime execution mode of DataStream programs. Among other things, "
                                    + "this controls task scheduling, network shuffle behavior, and time semantics.");

    public static final ConfigOption<ShuffleMode> SHUFFLE_MODE =
            ConfigOptions.key("execution.shuffle-mode")
                    .enumType(ShuffleMode.class)
                    .defaultValue(ShuffleMode.AUTOMATIC)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Mode that defines how data is exchanged between tasks if the shuffling "
                                                    + "behavior has not been set explicitly for an individual exchange. "
                                                    + "The shuffle mode depends on the configured '%s' and is only "
                                                    + "relevant for batch executions on bounded streams.",
                                            text(RUNTIME_MODE.key()))
                                    .linebreak()
                                    .text(
                                            "In streaming mode, upstream and downstream tasks run simultaneously to achieve low latency. "
                                                    + "An exchange is always pipelined (i.e. a result record is immediately sent to and "
                                                    + "processed by the downstream task). Thus, the receiver back-pressures the sender.")
                                    .linebreak()
                                    .text(
                                            "In batch mode, upstream and downstream tasks can run in stages. Blocking exchanges persist "
                                                    + "records to some storage. Downstream tasks then fetch these records after the "
                                                    + "upstream tasks finished. Such an exchange reduces the resources required to "
                                                    + "execute the job as it does not need to run upstream and downstream tasks "
                                                    + "simultaneously.")
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

    public static final ConfigOption<Duration> BUFFER_TIMEOUT =
            ConfigOptions.key("execution.buffer-timeout")
                    .durationType()
                    .defaultValue(Duration.ofMillis(100))
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
                                                    "0 triggers flushing after every record thus minimizing latency"),
                                            text(
                                                    "-1 ms triggers flushing only when the output buffer is full thus maximizing "
                                                            + "throughput"))
                                    .build());

    @Documentation.ExcludeFromDocumentation(
            "This is an expert option, that we do not want to expose in" + " the documentation")
    public static final ConfigOption<Boolean> SORT_INPUTS =
            ConfigOptions.key("execution.sorted-inputs.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "A flag to enable or disable sorting inputs of keyed operators. "
                                    + "NOTE: It takes effect only in the BATCH runtime mode.");

    @Documentation.ExcludeFromDocumentation(
            "This is an expert option, that we do not want to expose in" + " the documentation")
    public static final ConfigOption<Boolean> USE_BATCH_STATE_BACKEND =
            ConfigOptions.key("execution.batch-state-backend.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "A flag to enable or disable batch runtime specific state backend and timer service for keyed"
                                    + " operators. NOTE: It takes effect only in the BATCH runtime mode and requires sorted inputs"
                                    + SORT_INPUTS.key()
                                    + " to be enabled.");
}
