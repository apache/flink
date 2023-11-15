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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;

import static org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend.PriorityQueueStateType.ROCKSDB;
import static org.apache.flink.contrib.streaming.state.PredefinedOptions.DEFAULT;
import static org.apache.flink.contrib.streaming.state.PredefinedOptions.FLASH_SSD_OPTIMIZED;
import static org.apache.flink.contrib.streaming.state.PredefinedOptions.SPINNING_DISK_OPTIMIZED;
import static org.apache.flink.contrib.streaming.state.PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM;

/** Configuration options for the RocksDB backend. */
public class RocksDBOptions {

    /** The local directory (on the TaskManager) where RocksDB puts its files. */
    @Documentation.Section(Documentation.Sections.EXPERT_ROCKSDB)
    public static final ConfigOption<String> LOCAL_DIRECTORIES =
            ConfigOptions.key("state.backend.rocksdb.localdir")
                    .stringType()
                    .noDefaultValue()
                    .withDeprecatedKeys("state.backend.rocksdb.checkpointdir")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The local directory (on the TaskManager) where RocksDB puts its files. Per default, it will be <WORKING_DIR>/tmp. See %s for more details.",
                                            TextElement.code(
                                                    ClusterOptions
                                                            .TASK_MANAGER_PROCESS_WORKING_DIR_BASE
                                                            .key()))
                                    .build());

    /** Choice of timer service implementation. */
    @Documentation.Section(Documentation.Sections.STATE_BACKEND_ROCKSDB)
    public static final ConfigOption<EmbeddedRocksDBStateBackend.PriorityQueueStateType>
            TIMER_SERVICE_FACTORY =
                    ConfigOptions.key("state.backend.rocksdb.timer-service.factory")
                            .enumType(EmbeddedRocksDBStateBackend.PriorityQueueStateType.class)
                            .defaultValue(ROCKSDB)
                            .withDescription(
                                    "This determines the factory for timer service state implementation.");

    /** The cache size per key-group for ROCKSDB timer service factory implementation. */
    @Documentation.Section(Documentation.Sections.STATE_BACKEND_ROCKSDB)
    public static final ConfigOption<Integer> ROCKSDB_TIMER_SERVICE_FACTORY_CACHE_SIZE =
            ConfigOptions.key("state.backend.rocksdb.timer-service.cache-size")
                    .intType()
                    .defaultValue(128)
                    .withDescription(
                            String.format(
                                    "The cache size per keyGroup of rocksdb timer service factory. This option only has an effect "
                                            + "when '%s' is configured to '%s'. Increasing this value can improve the performance "
                                            + "of rocksdb timer service, but consumes more heap memory at the same time.",
                                    TIMER_SERVICE_FACTORY.key(), ROCKSDB.name()));

    /**
     * The number of threads used to transfer (download and upload) files in RocksDBStateBackend.
     */
    @Documentation.Section(Documentation.Sections.EXPERT_ROCKSDB)
    public static final ConfigOption<Integer> CHECKPOINT_TRANSFER_THREAD_NUM =
            ConfigOptions.key("state.backend.rocksdb.checkpoint.transfer.thread.num")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "The number of threads (per stateful operator) used to transfer (download and upload) files in RocksDBStateBackend.");

    /** The predefined settings for RocksDB DBOptions and ColumnFamilyOptions by Flink community. */
    @Documentation.Section(Documentation.Sections.EXPERT_ROCKSDB)
    public static final ConfigOption<String> PREDEFINED_OPTIONS =
            ConfigOptions.key("state.backend.rocksdb.predefined-options")
                    .stringType()
                    .defaultValue(DEFAULT.name())
                    .withDescription(
                            String.format(
                                    "The predefined settings for RocksDB DBOptions and ColumnFamilyOptions by Flink community. "
                                            + "Current supported candidate predefined-options are %s, %s, %s or %s. Note that user customized options and options "
                                            + "from the RocksDBOptionsFactory are applied on top of these predefined ones.",
                                    DEFAULT.name(),
                                    SPINNING_DISK_OPTIMIZED.name(),
                                    SPINNING_DISK_OPTIMIZED_HIGH_MEM.name(),
                                    FLASH_SSD_OPTIMIZED.name()));

    /** The options factory class for RocksDB to create DBOptions and ColumnFamilyOptions. */
    @Documentation.Section(Documentation.Sections.EXPERT_ROCKSDB)
    public static final ConfigOption<String> OPTIONS_FACTORY =
            ConfigOptions.key("state.backend.rocksdb.options-factory")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The options factory class for users to add customized options in DBOptions and ColumnFamilyOptions for RocksDB. "
                                    + "If set, the RocksDB state backend will load the class and apply configs to DBOptions and ColumnFamilyOptions "
                                    + "after loading ones from 'RocksDBConfigurableOptions' and pre-defined options.");

    @Documentation.Section(Documentation.Sections.STATE_BACKEND_ROCKSDB)
    public static final ConfigOption<Boolean> USE_MANAGED_MEMORY =
            ConfigOptions.key("state.backend.rocksdb.memory.managed")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "If set, the RocksDB state backend will automatically configure itself to use the "
                                    + "managed memory budget of the task slot, and divide the memory over write buffers, indexes, "
                                    + "block caches, etc. That way, the three major uses of memory of RocksDB will be capped.");

    @Documentation.Section(Documentation.Sections.STATE_BACKEND_ROCKSDB)
    public static final ConfigOption<MemorySize> FIX_PER_SLOT_MEMORY_SIZE =
            ConfigOptions.key("state.backend.rocksdb.memory.fixed-per-slot")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            String.format(
                                    "The fixed total amount of memory, shared among all RocksDB instances per slot. "
                                            + "This option overrides the '%s' option when configured.",
                                    USE_MANAGED_MEMORY.key()));

    @Documentation.Section(Documentation.Sections.STATE_BACKEND_ROCKSDB)
    public static final ConfigOption<MemorySize> FIX_PER_TM_MEMORY_SIZE =
            ConfigOptions.key("state.backend.rocksdb.memory.fixed-per-tm")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            String.format(
                                    "The fixed total amount of memory, shared among all RocksDB instances per Task Manager (cluster-level option). "
                                            + "This option only takes effect if neither '%s' nor '%s' are not configured. If none is configured "
                                            + "then each RocksDB column family state has its own memory caches (as controlled by the column "
                                            + "family options). "
                                            + "The relevant options for the shared resources (e.g. write-buffer-ratio) can be set on the same level (flink-conf.yaml)."
                                            + "Note, that this feature breaks resource isolation between the slots",
                                    USE_MANAGED_MEMORY.key(), FIX_PER_SLOT_MEMORY_SIZE.key()));

    @Documentation.Section(Documentation.Sections.STATE_BACKEND_ROCKSDB)
    public static final ConfigOption<Double> WRITE_BUFFER_RATIO =
            ConfigOptions.key("state.backend.rocksdb.memory.write-buffer-ratio")
                    .doubleType()
                    .defaultValue(0.5)
                    .withDescription(
                            String.format(
                                    "The maximum amount of memory that write buffers may take, as a fraction of the total shared memory. "
                                            + "This option only has an effect when '%s' or '%s' are configured.",
                                    USE_MANAGED_MEMORY.key(), FIX_PER_SLOT_MEMORY_SIZE.key()));

    @Documentation.Section(Documentation.Sections.STATE_BACKEND_ROCKSDB)
    public static final ConfigOption<Double> HIGH_PRIORITY_POOL_RATIO =
            ConfigOptions.key("state.backend.rocksdb.memory.high-prio-pool-ratio")
                    .doubleType()
                    .defaultValue(0.1)
                    .withDescription(
                            String.format(
                                    "The fraction of cache memory that is reserved for high-priority data like index, filter, and "
                                            + "compression dictionary blocks. This option only has an effect when '%s' or '%s' are configured.",
                                    USE_MANAGED_MEMORY.key(), FIX_PER_SLOT_MEMORY_SIZE.key()));

    @Documentation.Section(Documentation.Sections.STATE_BACKEND_ROCKSDB)
    public static final ConfigOption<Boolean> USE_PARTITIONED_INDEX_FILTERS =
            ConfigOptions.key("state.backend.rocksdb.memory.partitioned-index-filters")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            String.format(
                                    "With partitioning, the index/filter block of an SST file is partitioned into smaller blocks with "
                                            + "an additional top-level index on them. When reading an index/filter, only top-level index is loaded into memory. "
                                            + "The partitioned index/filter then uses the top-level index to load on demand into the block cache "
                                            + "the partitions that are required to perform the index/filter query. "
                                            + "This option only has an effect when '%s' or '%s' are configured.",
                                    USE_MANAGED_MEMORY.key(), FIX_PER_SLOT_MEMORY_SIZE.key()));
}
