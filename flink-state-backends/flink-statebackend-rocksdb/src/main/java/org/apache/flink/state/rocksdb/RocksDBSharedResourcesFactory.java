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

package org.apache.flink.state.rocksdb;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.memory.SharedResources;
import org.apache.flink.util.function.LongFunctionWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import static org.apache.flink.state.rocksdb.RocksDBOptions.FIX_PER_TM_MEMORY_SIZE;

/**
 * A factory of {@link RocksDBSharedResources}. Encapsulates memory share scope (e.g. TM, Slot) and
 * lifecycle (managed/unmanaged).
 */
enum RocksDBSharedResourcesFactory {
    /** Memory allocated per Slot (shared across slot tasks), managed by Flink. */
    SLOT_SHARED_MANAGED(false, MemoryShareScope.SLOT) {
        @Override
        protected OpaqueMemoryResource<RocksDBSharedResources> createInternal(
                RocksDBMemoryConfiguration jobMemoryConfig,
                String resourceId,
                Environment env,
                double memoryFraction,
                LongFunctionWithException<RocksDBSharedResources, Exception> allocator)
                throws Exception {
            return env.getMemoryManager()
                    .getSharedMemoryResourceForManagedMemory(resourceId, allocator, memoryFraction);
        }
    },
    /** Memory allocated per Slot (shared across slot tasks), unmanaged. */
    SLOT_SHARED_UNMANAGED(false, MemoryShareScope.SLOT) {
        @Override
        protected OpaqueMemoryResource<RocksDBSharedResources> createInternal(
                RocksDBMemoryConfiguration jobMemoryConfig,
                String resourceId,
                Environment env,
                double memoryFraction,
                LongFunctionWithException<RocksDBSharedResources, Exception> allocator)
                throws Exception {
            return env.getMemoryManager()
                    .getExternalSharedMemoryResource(
                            resourceId,
                            allocator,
                            jobMemoryConfig.getFixedMemoryPerSlot().getBytes());
        }
    },
    /** Memory allocated per TM (shared across all tasks), unmanaged. */
    TM_SHARED_UNMANAGED(false, MemoryShareScope.TM) {
        @Override
        protected OpaqueMemoryResource<RocksDBSharedResources> createInternal(
                RocksDBMemoryConfiguration jobMemoryConfig,
                String resourceId,
                Environment env,
                double memoryFraction,
                LongFunctionWithException<RocksDBSharedResources, Exception> allocator)
                throws Exception {

            SharedResources sharedResources = env.getSharedResources();
            Object leaseHolder = new Object();
            SharedResources.ResourceAndSize<RocksDBSharedResources> resource =
                    sharedResources.getOrAllocateSharedResource(
                            resourceId, leaseHolder, allocator, getTmSharedMemorySize(env));
            ThrowingRunnable<Exception> disposer =
                    () -> sharedResources.release(resourceId, leaseHolder, unused -> {});

            return new OpaqueMemoryResource<>(resource.resourceHandle(), resource.size(), disposer);
        }
    };

    private final boolean managed;
    private final MemoryShareScope shareScope;

    RocksDBSharedResourcesFactory(boolean managed, MemoryShareScope shareScope) {
        this.managed = managed;
        this.shareScope = shareScope;
    }

    @Nullable
    public static RocksDBSharedResourcesFactory from(
            RocksDBMemoryConfiguration jobMemoryConfig, Environment env) {
        if (jobMemoryConfig.isUsingFixedMemoryPerSlot()) {
            return RocksDBSharedResourcesFactory.SLOT_SHARED_UNMANAGED;
        } else if (jobMemoryConfig.isUsingManagedMemory()) {
            return RocksDBSharedResourcesFactory.SLOT_SHARED_MANAGED;
        } else if (getTmSharedMemorySize(env) > 0) {
            return RocksDBSharedResourcesFactory.TM_SHARED_UNMANAGED;
        } else {
            // not shared and not managed - allocate per column family
            return null;
        }
    }

    public final OpaqueMemoryResource<RocksDBSharedResources> create(
            RocksDBMemoryConfiguration jobMemoryConfig,
            Environment env,
            double memoryFraction,
            Logger logger,
            RocksDBMemoryControllerUtils.RocksDBMemoryFactory rocksDBMemoryFactory)
            throws Exception {
        logger.info(
                "Getting shared memory for RocksDB: shareScope={}, managed={}",
                shareScope,
                managed);
        return createInternal(
                jobMemoryConfig,
                managed ? MANAGED_MEMORY_RESOURCE_ID : UNMANAGED_MEMORY_RESOURCE_ID,
                env,
                memoryFraction,
                createAllocator(
                        shareScope.getConfiguration(jobMemoryConfig, env), rocksDBMemoryFactory));
    }

    protected abstract OpaqueMemoryResource<RocksDBSharedResources> createInternal(
            RocksDBMemoryConfiguration jobMemoryConfig,
            String resourceId,
            Environment env,
            double memoryFraction,
            LongFunctionWithException<RocksDBSharedResources, Exception> allocator)
            throws Exception;

    private static long getTmSharedMemorySize(Environment env) {
        return env.getTaskManagerInfo()
                .getConfiguration()
                .getOptional(FIX_PER_TM_MEMORY_SIZE)
                .orElse(MemorySize.ZERO)
                .getBytes();
    }

    private static final String MANAGED_MEMORY_RESOURCE_ID = "state-rocks-managed-memory";

    private static final String UNMANAGED_MEMORY_RESOURCE_ID = "state-rocks-fixed-slot-memory";

    private static LongFunctionWithException<RocksDBSharedResources, Exception> createAllocator(
            RocksDBMemoryConfiguration config,
            RocksDBMemoryControllerUtils.RocksDBMemoryFactory rocksDBMemoryFactory) {
        return size ->
                RocksDBMemoryControllerUtils.allocateRocksDBSharedResources(
                        size,
                        config.getWriteBufferRatio(),
                        config.getHighPriorityPoolRatio(),
                        config.isUsingPartitionedIndexFilters(),
                        rocksDBMemoryFactory);
    }
}

enum MemoryShareScope {
    TM {
        @Override
        public RocksDBMemoryConfiguration getConfiguration(
                RocksDBMemoryConfiguration jobMemoryConfig, Environment env) {
            return RocksDBMemoryConfiguration.fromConfiguration(
                    env.getTaskManagerInfo().getConfiguration());
        }
    },
    SLOT {
        @Override
        public RocksDBMemoryConfiguration getConfiguration(
                RocksDBMemoryConfiguration jobMemoryConfig, Environment env) {
            return jobMemoryConfig;
        }
    };

    public abstract RocksDBMemoryConfiguration getConfiguration(
            RocksDBMemoryConfiguration jobMemoryConfig, Environment env);
}
