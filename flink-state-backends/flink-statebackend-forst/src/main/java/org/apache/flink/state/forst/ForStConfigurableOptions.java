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

package org.apache.flink.state.forst;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.util.Preconditions;

import org.forstdb.CompactionStyle;
import org.forstdb.CompressionType;
import org.forstdb.InfoLogLevel;

import java.io.File;
import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.LinkElement.link;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.state.forst.ForStOptions.CACHE_DIRECTORY;
import static org.apache.flink.state.forst.ForStOptions.CACHE_RESERVED_SIZE;
import static org.apache.flink.state.forst.ForStOptions.CACHE_SIZE_BASE_LIMIT;
import static org.apache.flink.state.forst.ForStOptions.EXECUTOR_COORDINATOR_INLINE;
import static org.apache.flink.state.forst.ForStOptions.EXECUTOR_READ_IO_PARALLELISM;
import static org.apache.flink.state.forst.ForStOptions.EXECUTOR_WRITE_IO_INLINE;
import static org.apache.flink.state.forst.ForStOptions.EXECUTOR_WRITE_IO_PARALLELISM;
import static org.forstdb.CompactionStyle.FIFO;
import static org.forstdb.CompactionStyle.LEVEL;
import static org.forstdb.CompactionStyle.NONE;
import static org.forstdb.CompactionStyle.UNIVERSAL;
import static org.forstdb.CompressionType.LZ4_COMPRESSION;
import static org.forstdb.CompressionType.NO_COMPRESSION;
import static org.forstdb.CompressionType.SNAPPY_COMPRESSION;
import static org.forstdb.InfoLogLevel.INFO_LEVEL;

/**
 * This class contains the configuration options for the ForStStateBackend.
 *
 * <p>Currently, Options of ForSt would be configured by values here, and a user-defined {@link
 * ForStOptionsFactory} may override the configurations here.
 */
@Experimental
public class ForStConfigurableOptions implements Serializable {

    // --------------------------------------------------------------------------
    // Provided configurable DBOptions within Flink
    // --------------------------------------------------------------------------

    public static final ConfigOption<Integer> MAX_BACKGROUND_THREADS =
            key("state.backend.forst.thread.num")
                    .intType()
                    .defaultValue(2)
                    .withDescription(
                            "The maximum number of concurrent background flush and compaction jobs (per stateful operator). "
                                    + "The default value is '2'.");

    public static final ConfigOption<Integer> MAX_OPEN_FILES =
            key("state.backend.forst.files.open")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "The maximum number of open files (per stateful operator) that can be used by the DB, '-1' means no limit. "
                                    + "The default value is '-1'.");

    public static final ConfigOption<MemorySize> LOG_MAX_FILE_SIZE =
            key("state.backend.forst.log.max-file-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("25mb"))
                    .withDescription(
                            "The maximum size of ForSt's file used for information logging. "
                                    + "If the log files becomes larger than this, a new file will be created. "
                                    + "If 0, all logs will be written to one log file. "
                                    + "The default maximum file size is '25MB'. ");

    public static final ConfigOption<Integer> LOG_FILE_NUM =
            key("state.backend.forst.log.file-num")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "The maximum number of files ForSt should keep for information logging (Default setting: 4).");

    public static final ConfigOption<String> LOG_DIR =
            key("state.backend.forst.log.dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The directory for ForSt's information logging files. "
                                    + "If empty (Flink default setting), log files will be in the same directory as the Flink log. "
                                    + "If non-empty, this directory will be used and the data directory's absolute path will be used as the prefix of the log file name. "
                                    + "If setting this option as a non-existing location, e.g '/dev/null', ForSt will then create the log under its own database folder as before.");

    public static final ConfigOption<InfoLogLevel> LOG_LEVEL =
            key("state.backend.forst.log.level")
                    .enumType(InfoLogLevel.class)
                    .defaultValue(INFO_LEVEL)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The specified information logging level for ForSt. "
                                                    + "If unset, Flink will use %s.",
                                            code(INFO_LEVEL.name()))
                                    .linebreak()
                                    .text(
                                            "Note: ForSt info logs will not be written to the TaskManager logs and there "
                                                    + "is no rolling strategy, unless you configure %s, %s, and %s accordingly. "
                                                    + "Without a rolling strategy, long-running tasks may lead to uncontrolled "
                                                    + "disk space usage if configured with increased log levels!",
                                            code(LOG_DIR.key()),
                                            code(LOG_MAX_FILE_SIZE.key()),
                                            code(LOG_FILE_NUM.key()))
                                    .linebreak()
                                    .text(
                                            "There is no need to modify the ForSt log level, unless for troubleshooting ForSt.")
                                    .build());

    // --------------------------------------------------------------------------
    // Provided configurable ColumnFamilyOptions within Flink
    // --------------------------------------------------------------------------

    public static final ConfigOption<CompactionStyle> COMPACTION_STYLE =
            key("state.backend.forst.compaction.style")
                    .enumType(CompactionStyle.class)
                    .defaultValue(LEVEL)
                    .withDescription(
                            String.format(
                                    "The specified compaction style for DB. Candidate compaction style is %s, %s, %s or %s, "
                                            + "and Flink chooses '%s' as default style.",
                                    LEVEL.name(),
                                    FIFO.name(),
                                    UNIVERSAL.name(),
                                    NONE.name(),
                                    LEVEL.name()));

    public static final ConfigOption<Boolean> USE_DYNAMIC_LEVEL_SIZE =
            key("state.backend.forst.compaction.level.use-dynamic-size")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If true, ForSt will pick target size of each level dynamically. From an empty DB, ")
                                    .text(
                                            "ForSt would make last level the base level, which means merging L0 data into the last level, ")
                                    .text(
                                            "until it exceeds max_bytes_for_level_base. And then repeat this process for second last level and so on. ")
                                    .text("The default value is 'false'. ")
                                    .text(
                                            "For more information, please refer to %s",
                                            link(
                                                    "https://github.com/facebook/rocksdb/wiki/Leveled-Compaction#level_compaction_dynamic_level_bytes-is-true",
                                                    "RocksDB's doc."))
                                    .build());

    public static final ConfigOption<List<CompressionType>> COMPRESSION_PER_LEVEL =
            key("state.backend.forst.compression.per.level")
                    .enumType(CompressionType.class)
                    .asList()
                    .defaultValues(SNAPPY_COMPRESSION)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "A semicolon-separated list of Compression Type. Different levels can have different "
                                                    + "compression policies. In many cases, lower levels use fast compression algorithms,"
                                                    + " while higher levels with more data use slower but more effective compression algorithms. "
                                                    + "The N th element in the List corresponds to the compression type of the level N-1"
                                                    + "When %s is true, compression_per_level[0] still determines L0, but other "
                                                    + "elements are based on the base level and may not match the level seen in the info log",
                                            code(USE_DYNAMIC_LEVEL_SIZE.key()))
                                    .linebreak()
                                    .text(
                                            "Note: If the List size is smaller than the level number, the undefined lower level uses the last Compression Type in the List")
                                    .linebreak()
                                    .text(
                                            "Some commonly used compression algorithms for candidates include %s ,%s and %s",
                                            code(NO_COMPRESSION.name()),
                                            code(SNAPPY_COMPRESSION.name()),
                                            code(LZ4_COMPRESSION.name()))
                                    .linebreak()
                                    .text(
                                            "The default value is %s, which means that all data uses the Snappy compression algorithm.",
                                            code(SNAPPY_COMPRESSION.name()))
                                    .text(
                                            "Likewise, if set to %s , means that all data is not compressed, which will achieve faster speed but will bring some space amplification.",
                                            code(NO_COMPRESSION.name()))
                                    .text(
                                            "In addition, if we need to consider both spatial amplification and performance, we can also set it to '%s;%s;%s', which means that L0 and L1 data will not be compressed, and other data will be compressed using LZ4.",
                                            code(NO_COMPRESSION.name()),
                                            code(NO_COMPRESSION.name()),
                                            code(LZ4_COMPRESSION.name()))
                                    .build());

    public static final ConfigOption<MemorySize> TARGET_FILE_SIZE_BASE =
            key("state.backend.forst.compaction.level.target-file-size-base")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64mb"))
                    .withDescription(
                            "The target file size for compaction, which determines a level-1 file size. "
                                    + "The default value is '64MB'.");

    public static final ConfigOption<MemorySize> MAX_SIZE_LEVEL_BASE =
            key("state.backend.forst.compaction.level.max-size-level-base")
                    .memoryType()
                    .defaultValue(MemorySize.parse("256mb"))
                    .withDescription(
                            "The upper-bound of the total size of level base files in bytes. "
                                    + "The default value is '256MB'.");

    public static final ConfigOption<MemorySize> WRITE_BUFFER_SIZE =
            key("state.backend.forst.writebuffer.size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64mb"))
                    .withDescription(
                            "The amount of data built up in memory (backed by an unsorted log on disk) "
                                    + "before converting to a sorted on-disk files. The default writebuffer size is '64MB'.");

    public static final ConfigOption<Integer> MAX_WRITE_BUFFER_NUMBER =
            key("state.backend.forst.writebuffer.count")
                    .intType()
                    .defaultValue(2)
                    .withDescription(
                            "The maximum number of write buffers that are built up in memory. "
                                    + "The default value is '2'.");

    public static final ConfigOption<Integer> MIN_WRITE_BUFFER_NUMBER_TO_MERGE =
            key("state.backend.forst.writebuffer.number-to-merge")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The minimum number of write buffers that will be merged together before writing to storage. "
                                    + "The default value is '1'.");

    public static final ConfigOption<MemorySize> BLOCK_SIZE =
            key("state.backend.forst.block.blocksize")
                    .memoryType()
                    .defaultValue(MemorySize.parse("4kb"))
                    .withDescription(
                            "The approximate size (in bytes) of user data packed per block. "
                                    + "The default blocksize is '4KB'.");

    public static final ConfigOption<MemorySize> METADATA_BLOCK_SIZE =
            key("state.backend.forst.block.metadata-blocksize")
                    .memoryType()
                    .defaultValue(MemorySize.parse("4kb"))
                    .withDescription(
                            "Approximate size of partitioned metadata packed per block. "
                                    + "Currently applied to indexes block when partitioned index/filters option is enabled. "
                                    + "The default blocksize is '4KB'.");

    public static final ConfigOption<MemorySize> BLOCK_CACHE_SIZE =
            key("state.backend.forst.block.cache-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("8mb"))
                    .withDescription(
                            "The amount of the cache for data blocks in ForSt. "
                                    + "The default block-cache size is '8MB'.");

    public static final ConfigOption<MemorySize> WRITE_BATCH_SIZE =
            key("state.backend.forst.write-batch-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("2mb"))
                    .withDescription(
                            "The max size of the consumed memory for ForSt batch write, "
                                    + "will flush just based on item count if this config set to 0.");

    public static final ConfigOption<Boolean> USE_BLOOM_FILTER =
            key("state.backend.forst.use-bloom-filter")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, every newly created SST file will contain a Bloom filter. "
                                    + "It is disabled by default.");

    public static final ConfigOption<Double> BLOOM_FILTER_BITS_PER_KEY =
            key("state.backend.forst.bloom-filter.bits-per-key")
                    .doubleType()
                    .defaultValue(10.0)
                    .withDescription(
                            "Bits per key that bloom filter will use, this only take effect when bloom filter is used. "
                                    + "The default value is 10.0.");

    public static final ConfigOption<Boolean> BLOOM_FILTER_BLOCK_BASED_MODE =
            key("state.backend.forst.bloom-filter.block-based-mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, ForSt will use block-based filter instead of full filter, this only take effect when bloom filter is used. "
                                    + "The default value is 'false'.");

    public static final ConfigOption<Long> COMPACT_FILTER_QUERY_TIME_AFTER_NUM_ENTRIES =
            key("state.backend.forst.compaction.filter.query-time-after-num-entries")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription(
                            "Number of state entries to process by compaction filter before updating current timestamp. "
                                    + "Updating the timestamp more often can improve cleanup speed, "
                                    + "but it decreases compaction performance because it uses JNI calls from native code.The default value is '1000L'.");

    public static final ConfigOption<Duration> COMPACT_FILTER_PERIODIC_COMPACTION_TIME =
            key("state.backend.forst.compaction.filter.periodic-compaction-time")
                    .durationType()
                    .defaultValue(Duration.ofDays(30))
                    .withDescription(
                            "Periodic compaction could speed up expired state entries cleanup, especially for state"
                                    + " entries rarely accessed. Files older than this value will be picked up for compaction,"
                                    + " and re-written to the same level as they were before. It makes sure a file goes through"
                                    + " compaction filters periodically. 0 means turning off periodic compaction.The default value is '30days'.");

    static final ConfigOption<?>[] CANDIDATE_CONFIGS =
            new ConfigOption<?>[] {
                // cache
                CACHE_DIRECTORY,
                CACHE_SIZE_BASE_LIMIT,
                CACHE_RESERVED_SIZE,
                // configurable forst executor
                EXECUTOR_COORDINATOR_INLINE,
                EXECUTOR_WRITE_IO_INLINE,
                EXECUTOR_WRITE_IO_PARALLELISM,
                EXECUTOR_READ_IO_PARALLELISM,
                // configurable DBOptions
                MAX_BACKGROUND_THREADS,
                MAX_OPEN_FILES,
                LOG_LEVEL,
                LOG_MAX_FILE_SIZE,
                LOG_FILE_NUM,
                LOG_DIR,

                // configurable ColumnFamilyOptions
                COMPACTION_STYLE,
                COMPRESSION_PER_LEVEL,
                USE_DYNAMIC_LEVEL_SIZE,
                TARGET_FILE_SIZE_BASE,
                MAX_SIZE_LEVEL_BASE,
                WRITE_BUFFER_SIZE,
                MAX_WRITE_BUFFER_NUMBER,
                MIN_WRITE_BUFFER_NUMBER_TO_MERGE,
                BLOCK_SIZE,
                METADATA_BLOCK_SIZE,
                BLOCK_CACHE_SIZE,
                USE_BLOOM_FILTER,
                BLOOM_FILTER_BITS_PER_KEY,
                BLOOM_FILTER_BLOCK_BASED_MODE,
            };

    private static final Set<ConfigOption<?>> POSITIVE_INT_CONFIG_SET =
            new HashSet<>(
                    Arrays.asList(
                            MAX_BACKGROUND_THREADS,
                            LOG_FILE_NUM,
                            MAX_WRITE_BUFFER_NUMBER,
                            MIN_WRITE_BUFFER_NUMBER_TO_MERGE));

    private static final Set<ConfigOption<?>> SIZE_CONFIG_SET =
            new HashSet<>(
                    Arrays.asList(
                            TARGET_FILE_SIZE_BASE,
                            MAX_SIZE_LEVEL_BASE,
                            WRITE_BUFFER_SIZE,
                            BLOCK_SIZE,
                            METADATA_BLOCK_SIZE,
                            BLOCK_CACHE_SIZE));

    /**
     * Helper method to check whether the (key,value) is valid through given configuration and
     * returns the formatted value.
     *
     * @param option The configuration key which is configurable in {@link
     *     ForStConfigurableOptions}.
     * @param value The value within given configuration.
     */
    static void checkArgumentValid(ConfigOption<?> option, Object value) {
        final String key = option.key();

        if (POSITIVE_INT_CONFIG_SET.contains(option)) {
            Preconditions.checkArgument(
                    (Integer) value > 0,
                    "Configured value for key: " + key + " must be larger than 0.");
        } else if (SIZE_CONFIG_SET.contains(option)) {
            Preconditions.checkArgument(
                    ((MemorySize) value).getBytes() > 0,
                    "Configured size for key" + key + " must be larger than 0.");
        } else if (LOG_MAX_FILE_SIZE.equals(option)) {
            Preconditions.checkArgument(
                    ((MemorySize) value).getBytes() >= 0,
                    "Configured size for key " + key + " must be larger than or equal to 0.");
        } else if (LOG_DIR.equals(option)) {
            Preconditions.checkArgument(
                    new File((String) value).isAbsolute(),
                    "Configured path for key " + key + " is not absolute.");
        }
    }
}
