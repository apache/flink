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

package org.apache.flink.changelog.fs;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

import java.time.Duration;

import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT;

/** {@link ConfigOptions} for {@link FsStateChangelogStorage}. */
@Experimental
@Documentation.ExcludeFromDocumentation("ChangelogBackend is under development")
public class FsStateChangelogOptions {

    public static final ConfigOption<String> BASE_PATH =
            ConfigOptions.key("dstl.dfs.base-path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Base path to store changelog files.");

    public static final ConfigOption<Boolean> COMPRESSION_ENABLED =
            ConfigOptions.key("dstl.dfs.compression.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable compression when serializing changelog.");

    public static final ConfigOption<MemorySize> PREEMPTIVE_PERSIST_THRESHOLD =
            ConfigOptions.key("dstl.dfs.preemptive-persist-threshold")
                    .memoryType()
                    .defaultValue(MemorySize.parse("5Mb"))
                    .withDescription(
                            "Size threshold for state changes of a single operator "
                                    + "beyond which they are persisted pre-emptively without waiting for a checkpoint. "
                                    + " Improves checkpointing time by allowing quasi-continuous uploading of state changes "
                                    + "(as opposed to uploading all accumulated changes on checkpoint).");

    public static final ConfigOption<Duration> PERSIST_DELAY =
            ConfigOptions.key("dstl.dfs.batch.persist-delay")
                    .durationType()
                    .defaultValue(Duration.ofMillis(10))
                    .withDescription(
                            "Delay before persisting changelog after receiving persist request (on checkpoint). "
                                    + "Minimizes the number of files and requests "
                                    + "if multiple operators (backends) or sub-tasks are using the same store. "
                                    + "Correspondingly increases checkpoint time (async phase).");

    public static final ConfigOption<MemorySize> PERSIST_SIZE_THRESHOLD =
            ConfigOptions.key("dstl.dfs.batch.persist-size-threshold")
                    .memoryType()
                    .defaultValue(MemorySize.parse("10Mb"))
                    .withDescription(
                            "Size threshold for state changes that were requested to be persisted but are waiting for "
                                    + PERSIST_DELAY.key()
                                    + " (from all operators). "
                                    + ". Once reached, accumulated changes are persisted immediately. "
                                    + "This is different from "
                                    + PREEMPTIVE_PERSIST_THRESHOLD.key()
                                    + " as it happens AFTER the checkpoint and potentially for state changes of multiple operators.");

    public static final ConfigOption<MemorySize> UPLOAD_BUFFER_SIZE =
            ConfigOptions.key("dstl.dfs.upload.buffer-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("1Mb"))
                    .withDescription("Buffer size used when uploading change sets");

    public static final ConfigOption<Integer> NUM_UPLOAD_THREADS =
            ConfigOptions.key("dstl.dfs.upload.num-threads")
                    .intType()
                    .defaultValue(5)
                    .withDescription("Number of threads to use for upload.");

    public static final ConfigOption<MemorySize> IN_FLIGHT_DATA_LIMIT =
            ConfigOptions.key("dstl.dfs.upload.max-in-flight")
                    .memoryType()
                    .defaultValue(MemorySize.parse("100Mb"))
                    .withDescription(
                            "Max amount of data allowed to be in-flight. "
                                    + "Upon reaching this limit the task will fail");

    public static final ConfigOption<String> RETRY_POLICY =
            ConfigOptions.key("dstl.dfs.upload.retry-policy")
                    .stringType()
                    .defaultValue("fixed")
                    .withDescription(
                            "Retry policy for the failed uploads (in particular, timed out). Valid values: none, fixed.");
    public static final ConfigOption<Duration> UPLOAD_TIMEOUT =
            ConfigOptions.key("dstl.dfs.upload.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription(
                            "Time threshold beyond which an upload is considered timed out. "
                                    + "If a new attempt is made but this upload succeeds earlier then this upload result will be used. "
                                    + "May improve upload times if tail latencies of upload requests are significantly high. "
                                    + "Only takes effect if "
                                    + RETRY_POLICY.key()
                                    + " is fixed. "
                                    + "Please note that timeout * max_attempts should be less than "
                                    + CHECKPOINTING_TIMEOUT.key());
    public static final ConfigOption<Integer> RETRY_MAX_ATTEMPTS =
            ConfigOptions.key("dstl.dfs.upload.max-attempts")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Maximum number of attempts (including the initial one) to peform a particular upload. "
                                    + "Only takes effect if "
                                    + RETRY_POLICY.key()
                                    + " is fixed.");
    public static final ConfigOption<Duration> RETRY_DELAY_AFTER_FAILURE =
            ConfigOptions.key("dstl.dfs.upload.next-attempt-delay")
                    .durationType()
                    .defaultValue(Duration.ofMillis(500))
                    .withDescription(
                            "Delay before the next attempt (if the failure was not caused by a timeout).");
}
