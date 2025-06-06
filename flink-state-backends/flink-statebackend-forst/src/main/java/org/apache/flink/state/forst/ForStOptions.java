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
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;

import static org.apache.flink.configuration.description.TextElement.text;
import static org.apache.flink.state.forst.ForStStateBackend.CHECKPOINT_DIR_AS_PRIMARY_SHORTCUT;
import static org.apache.flink.state.forst.ForStStateBackend.LOCAL_DIR_AS_PRIMARY_SHORTCUT;
import static org.apache.flink.state.forst.ForStStateBackend.PriorityQueueStateType.ForStDB;

/** Configuration options for the ForStStateBackend. */
@Experimental
public class ForStOptions {

    /** The local directory (on the TaskManager) where ForSt puts some meta files. */
    @Documentation.Section(Documentation.Sections.EXPERT_FORST)
    public static final ConfigOption<String> LOCAL_DIRECTORIES =
            ConfigOptions.key("state.backend.forst.local-dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The local directory (on the TaskManager) where ForSt puts some metadata files. By default, it will be <WORKING_DIR>/tmp. See %s for more details.",
                                            TextElement.code(
                                                    ClusterOptions
                                                            .TASK_MANAGER_PROCESS_WORKING_DIR_BASE
                                                            .key()))
                                    .build());

    /** The remote directory where ForSt puts its SST files. */
    @Documentation.Section(Documentation.Sections.STATE_BACKEND_FORST)
    public static final ConfigOption<String> PRIMARY_DIRECTORY =
            ConfigOptions.key("state.backend.forst.primary-dir")
                    .stringType()
                    .defaultValue(CHECKPOINT_DIR_AS_PRIMARY_SHORTCUT)
                    .withDescription(
                            String.format(
                                    "The primary directory where ForSt puts its SST files. By default, it will be the same as the checkpoint directory. "
                                            + "Recognized shortcut name is '%s', which means that ForSt shares the directory with checkpoint, "
                                            + "and '%s', which means that ForSt will use the local directory of TaskManager.",
                                    CHECKPOINT_DIR_AS_PRIMARY_SHORTCUT,
                                    LOCAL_DIR_AS_PRIMARY_SHORTCUT));

    @Documentation.Section(Documentation.Sections.STATE_BACKEND_FORST)
    public static final ConfigOption<Boolean> SYNC_ENFORCE_LOCAL =
            ConfigOptions.key("state.backend.forst.sync.enforce-local")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to enforce local state for operators in synchronous mode when"
                                    + " enabling disaggregated state. This is useful in cases where "
                                    + "both synchronous operators and asynchronous operators are used "
                                    + "in the same job.");

    @Documentation.Section(Documentation.Sections.STATE_BACKEND_FORST)
    public static final ConfigOption<String> CACHE_DIRECTORY =
            ConfigOptions.key("state.backend.forst.cache.dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            String.format(
                                    "The directory where ForSt caches its SST files, fallback to the "
                                            + "subdirectory of '/cache' under the value of '%s' if not configured.",
                                    LOCAL_DIRECTORIES.key()));

    @Documentation.Section(Documentation.Sections.STATE_BACKEND_FORST)
    public static final ConfigOption<MemorySize> CACHE_SIZE_BASE_LIMIT =
            ConfigOptions.key("state.backend.forst.cache.size-based-limit")
                    .memoryType()
                    .defaultValue(MemorySize.ZERO)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "An upper-bound of the size that can be used for cache. User "
                                                    + "should specify at least one cache size limit to enable the cache, "
                                                    + "either this option or the '%s' option. "
                                                    + "They can be set simultaneously, and in this case, cache "
                                                    + "will grow if meet the requirements of both two options. "
                                                    + "The default value is '%s', meaning that this option is disabled. ",
                                            // can not ref the static member before definition.
                                            text("state.backend.forst.cache.reserve-size"),
                                            text(MemorySize.ZERO.toString()))
                                    .build());

    @Documentation.Section(Documentation.Sections.STATE_BACKEND_FORST)
    public static final ConfigOption<MemorySize> CACHE_RESERVED_SIZE =
            ConfigOptions.key("state.backend.forst.cache.reserve-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(256))
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The amount of reserved size on disk space, and remaining space can be "
                                                    + "leveraged by the cache. The cache will evict the oldest files when "
                                                    + "the reserved space on disk (the disk where cache directory is) is not "
                                                    + "enough. User should specify at least one cache size limit to enable the cache, "
                                                    + "either this option or the '%s' option. "
                                                    + "They can be set simultaneously, and in this case, "
                                                    + "cache will grow if meet the requirements of both two options. "
                                                    + "If the specified file system of the cache directory does not support "
                                                    + "reading the remaining space, the cache will not be able to reserve "
                                                    + "the specified space, hence this option will be ignored. "
                                                    + "The default value is '%s', meaning the disk will be reserved that much space, "
                                                    + "and the remaining of the disk can be used for cache. "
                                                    + "A configured value of '%s' means that this option is disabled.",
                                            text(CACHE_SIZE_BASE_LIMIT.key()),
                                            text(MemorySize.ofMebiBytes(256).toString()),
                                            text(MemorySize.ZERO.toString()))
                                    .build());

    @Documentation.Section(Documentation.Sections.EXPERT_FORST)
    public static final ConfigOption<Integer> CACHE_LRU_ACCESS_BEFORE_PROMOTION =
            ConfigOptions.key("state.backend.forst.cache.lru.access-before-promote")
                    .intType()
                    .defaultValue(6)
                    .withDescription(
                            "When the number of accesses to "
                                    + "a block in cold link reaches this value, the block will "
                                    + "be promoted to the head of the LRU list and become a hot link. "
                                    + "The evicted file in cache will be reloaded as well. "
                                    + "The default value is '5'.");

    @Documentation.Section(Documentation.Sections.EXPERT_FORST)
    public static final ConfigOption<Integer> CACHE_LRU_PROMOTION_LIMIT =
            ConfigOptions.key("state.backend.forst.cache.lru.promote-limit")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "When the number of eviction that a block in hot link "
                                    + "is moved to cold link reaches this value, the block will be blocked "
                                    + "from being promoted to the head of the LRU list. "
                                    + "The default value is '3'.");

    /** The options factory class for ForSt to create DBOptions and ColumnFamilyOptions. */
    @Documentation.Section(Documentation.Sections.EXPERT_FORST)
    public static final ConfigOption<String> OPTIONS_FACTORY =
            ConfigOptions.key("state.backend.forst.options-factory")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The options factory class for users to add customized options in DBOptions and ColumnFamilyOptions for ForSt. "
                                    + "If set, the ForSt state backend will load the class and apply configs to DBOptions and ColumnFamilyOptions "
                                    + "after loading ones from 'ForStConfigurableOptions' and pre-defined options.");

    @Documentation.Section(Documentation.Sections.EXPERT_FORST)
    public static final ConfigOption<Boolean> USE_MANAGED_MEMORY =
            ConfigOptions.key("state.backend.forst.memory.managed")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "If set true, the ForSt state backend will automatically configure itself to use the "
                                    + "managed memory budget of the task slot, and divide the memory over write buffers, indexes, "
                                    + "block caches, etc.");

    @Documentation.Section(Documentation.Sections.EXPERT_FORST)
    public static final ConfigOption<MemorySize> FIX_PER_SLOT_MEMORY_SIZE =
            ConfigOptions.key("state.backend.forst.memory.fixed-per-slot")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            String.format(
                                    "The fixed total amount of memory per slot, shared among all ForSt instances."
                                            + "This option overrides the '%s' option.",
                                    USE_MANAGED_MEMORY.key()));

    @Documentation.Section(Documentation.Sections.EXPERT_FORST)
    public static final ConfigOption<MemorySize> FIX_PER_TM_MEMORY_SIZE =
            ConfigOptions.key("state.backend.forst.memory.fixed-per-tm")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            String.format(
                                    "The fixed total amount of memory per Task Manager, shared among all ForSt instances. This is a cluster-level option. "
                                            + "This option only takes effect if '%s' is set to false and '%s' is not configured. If so, "
                                            + "then each ForSt column family state has its own memory caches (as controlled by the column "
                                            + "family options). "
                                            + "The relevant options for the shared resources (e.g. write-buffer-ratio) can be set on the same level (config.yaml). "
                                            + "Note that this feature breaks resource isolation between the slots.",
                                    USE_MANAGED_MEMORY.key(), FIX_PER_SLOT_MEMORY_SIZE.key()));

    public static final ConfigOption<Double> WRITE_BUFFER_RATIO =
            ConfigOptions.key("state.backend.forst.memory.write-buffer-ratio")
                    .doubleType()
                    .defaultValue(0.5)
                    .withDescription(
                            String.format(
                                    "The maximum amount of memory that write buffers may take, as a fraction of the total shared memory. "
                                            + "This option only has an effect when '%s' or '%s' are configured.",
                                    USE_MANAGED_MEMORY.key(), FIX_PER_SLOT_MEMORY_SIZE.key()));

    public static final ConfigOption<Double> HIGH_PRIORITY_POOL_RATIO =
            ConfigOptions.key("state.backend.forst.memory.high-prio-pool-ratio")
                    .doubleType()
                    .defaultValue(0.1)
                    .withDescription(
                            String.format(
                                    "The fraction of cache memory that is reserved for high-priority data like index, filter, and "
                                            + "compression dictionary blocks. This option only has an effect when '%s' or '%s' are configured.",
                                    USE_MANAGED_MEMORY.key(), FIX_PER_SLOT_MEMORY_SIZE.key()));

    @Documentation.Section(Documentation.Sections.STATE_BACKEND_FORST)
    public static final ConfigOption<Boolean> USE_PARTITIONED_INDEX_FILTERS =
            ConfigOptions.key("state.backend.forst.memory.partitioned-index-filters")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            String.format(
                                    "With partitioning, the index/filter block of an SST file is partitioned into smaller blocks with "
                                            + "an additional top-level index on them. When reading an index/filter, only top-level index is loaded into memory. "
                                            + "The partitioned index/filter then uses the top-level index to load on demand into the block cache "
                                            + "the partitions that are required to perform the index/filter query. "
                                            + "This option only has an effect when '%s' or '%s' are configured.",
                                    USE_MANAGED_MEMORY.key(), FIX_PER_SLOT_MEMORY_SIZE.key()));

    /** Choice of timer service implementation. */
    @Documentation.Section(Documentation.Sections.STATE_BACKEND_FORST)
    public static final ConfigOption<ForStStateBackend.PriorityQueueStateType>
            TIMER_SERVICE_FACTORY =
                    ConfigOptions.key("state.backend.forst.timer-service.factory")
                            .enumType(ForStStateBackend.PriorityQueueStateType.class)
                            .defaultValue(ForStDB)
                            .withDescription(
                                    "This determines the factory for timer service state implementation.");

    /** The cache size per key-group for ForSt timer service factory implementation. */
    @Documentation.Section(Documentation.Sections.STATE_BACKEND_FORST)
    public static final ConfigOption<Integer> FORST_TIMER_SERVICE_FACTORY_CACHE_SIZE =
            ConfigOptions.key("state.backend.forst.timer-service.cache-size")
                    .intType()
                    .defaultValue(128)
                    .withDescription(
                            String.format(
                                    "The cache size per keyGroup of ForSt timer service factory. This option only has an effect "
                                            + "when '%s' is configured to '%s'. Increasing this value can improve the performance "
                                            + "of ForSt timer service, but consumes more heap memory at the same time.",
                                    TIMER_SERVICE_FACTORY.key(), ForStDB.name()));

    @Documentation.Section(Documentation.Sections.EXPERT_FORST)
    public static final ConfigOption<Boolean> EXECUTOR_COORDINATOR_INLINE =
            ConfigOptions.key("state.backend.forst.executor.inline-coordinator")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to let the task thread be the coordinator thread responsible for distributing requests. "
                                    + "If set to 'true', the task thread will be responsible for distributing requests, "
                                    + "otherwise, a dedicated coordinator thread will be used. "
                                    + "The default value is 'false'.");

    @Documentation.Section(Documentation.Sections.EXPERT_FORST)
    public static final ConfigOption<Boolean> EXECUTOR_WRITE_IO_INLINE =
            ConfigOptions.key("state.backend.forst.executor.inline-write")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to let write requests be executed within the coordinator thread. "
                                    + "If set to 'true', write requests will be executed within the coordinator thread, "
                                    + "otherwise, a dedicated write thread will be used. "
                                    + "The default value is 'true'.");

    @Documentation.Section(Documentation.Sections.STATE_BACKEND_FORST)
    public static final ConfigOption<Integer> EXECUTOR_READ_IO_PARALLELISM =
            ConfigOptions.key("state.backend.forst.executor.read-io-parallelism")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "The number of threads used for read IO operations in the executor.");

    @Documentation.Section(Documentation.Sections.STATE_BACKEND_FORST)
    public static final ConfigOption<Integer> EXECUTOR_WRITE_IO_PARALLELISM =
            ConfigOptions.key("state.backend.forst.executor.write-io-parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The number of threads used for write IO operations in the executor."
                                    + " Only valid when '"
                                    + EXECUTOR_WRITE_IO_INLINE.key()
                                    + "' is false.");
}
