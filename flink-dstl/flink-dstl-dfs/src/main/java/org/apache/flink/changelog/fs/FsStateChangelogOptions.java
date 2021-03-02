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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

/** {@link ConfigOptions} for {@link FsStateChangelogStorage}. */
@Experimental
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
    public static final ConfigOption<MemorySize> UPLOAD_BUFFER_SIZE =
            ConfigOptions.key("dstl.dfs.upload.buffer-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("1Mb"))
                    .withDescription("Buffer size used when uploading change sets");
}
