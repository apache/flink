/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.rocksdb.sstmerge;

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

import java.time.Duration;

/** Configuration options for manual compaction for the RocksDB backend. */
public class RocksDBManualCompactionOptions {

    @Documentation.Section(Documentation.Sections.EXPERT_ROCKSDB)
    public static final ConfigOption<Duration> MIN_INTERVAL =
            ConfigOptions.key("state.backend.rocksdb.manual-compaction.min-interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(0))
                    .withDescription(
                            "The minimum interval between manual compactions. Zero disables manual compactions");

    @Documentation.Section(Documentation.Sections.EXPERT_ROCKSDB)
    public static final ConfigOption<Integer> MAX_PARALLEL_COMPACTIONS =
            ConfigOptions.key("state.backend.rocksdb.manual-compaction.max-parallel-compactions")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "The maximum number of manual compactions to start."
                                    + "Note that only one of them can run at a time as of v8.10.0; all the others will be waiting");

    @Documentation.Section(Documentation.Sections.EXPERT_ROCKSDB)
    public static final ConfigOption<MemorySize> MAX_FILE_SIZE_TO_COMPACT =
            ConfigOptions.key("state.backend.rocksdb.manual-compaction.max-file-size-to-compact")
                    .memoryType()
                    .defaultValue(MemorySize.parse("50k"))
                    .withDescription("The maximum size of individual input files");

    @Documentation.Section(Documentation.Sections.EXPERT_ROCKSDB)
    public static final ConfigOption<Integer> MIN_FILES_TO_COMPACT =
            ConfigOptions.key("state.backend.rocksdb.manual-compaction.min-files-to-compact")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "The minimum number of input files to compact together in a single compaction run");

    @Documentation.Section(Documentation.Sections.EXPERT_ROCKSDB)
    public static final ConfigOption<Integer> MAX_FILES_TO_COMPACT =
            ConfigOptions.key("state.backend.rocksdb.manual-compaction.max-files-to-compact")
                    .intType()
                    .defaultValue(30)
                    .withDescription(
                            "The maximum number of input files to compact together in a single compaction run");

    @Documentation.Section(Documentation.Sections.EXPERT_ROCKSDB)
    public static final ConfigOption<MemorySize> MAX_OUTPUT_FILE_SIZE =
            ConfigOptions.key("state.backend.rocksdb.manual-compaction.max-output-file-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64Mb"))
                    .withDescription("The maximum output file size");

    @Documentation.Section(Documentation.Sections.EXPERT_ROCKSDB)
    public static final ConfigOption<Integer> MAX_AUTO_COMPACTIONS =
            ConfigOptions.key("state.backend.rocksdb.manual-compaction.max-auto-compactions")
                    .intType()
                    .defaultValue(30)
                    .withDescription(
                            "The maximum number of automatic compactions running for manual compaction to start."
                                    + "If the actual number is higher, manual compaction won't be started to avoid delaying automatic ones.");
}
