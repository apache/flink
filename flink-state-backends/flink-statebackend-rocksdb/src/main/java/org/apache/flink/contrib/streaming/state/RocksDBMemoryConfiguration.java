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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;

/** The settings regarding RocksDBs memory usage. */
public final class RocksDBMemoryConfiguration implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Flag whether to use the managed memory budget for RocksDB. Null is not set. */
    @Nullable private Boolean useManagedMemory;

    /** The total memory for all RocksDB instances at this slot. Null is not set. */
    @Nullable private MemorySize fixedMemoryPerSlot;

    /**
     * The maximum fraction of the total shared memory consumed by the write buffers. Null if not
     * set.
     */
    @Nullable private Double writeBufferRatio;

    /**
     * The high priority pool ratio in the shared cache, used for index & filter blocks. Null if not
     * set.
     */
    @Nullable private Double highPriorityPoolRatio;

    /** Flag whether to use partition index/filters. Null if not set. */
    @Nullable private Boolean usePartitionedIndexFilters;

    // ------------------------------------------------------------------------

    /**
     * Configures RocksDB to use the managed memory of a slot. See {@link
     * RocksDBOptions#USE_MANAGED_MEMORY} for details.
     */
    public void setUseManagedMemory(boolean useManagedMemory) {
        this.useManagedMemory = useManagedMemory;
    }

    /**
     * Configures RocksDB to use a fixed amount of memory shared between all instances (operators)
     * in a slot. See {@link RocksDBOptions#FIX_PER_SLOT_MEMORY_SIZE} for details.
     */
    public void setFixedMemoryPerSlot(MemorySize fixedMemoryPerSlot) {
        checkArgument(
                fixedMemoryPerSlot == null || fixedMemoryPerSlot.getBytes() > 0,
                "Total memory per slot must be > 0");

        this.fixedMemoryPerSlot = fixedMemoryPerSlot;
    }

    /**
     * Configures RocksDB to use a fixed amount of memory shared between all instances (operators)
     * in a slot. See {@link #setFixedMemoryPerSlot(MemorySize)} for details.
     */
    public void setFixedMemoryPerSlot(String totalMemoryPerSlotStr) {
        setFixedMemoryPerSlot(MemorySize.parse(totalMemoryPerSlotStr));
    }

    /**
     * Sets the fraction of the total memory to be used for write buffers. This only has an effect
     * is either {@link #setUseManagedMemory(boolean)} or {@link #setFixedMemoryPerSlot(MemorySize)}
     * are set.
     *
     * <p>See {@link RocksDBOptions#WRITE_BUFFER_RATIO} for details.
     */
    public void setWriteBufferRatio(double writeBufferRatio) {
        Preconditions.checkArgument(
                writeBufferRatio > 0 && writeBufferRatio < 1.0,
                "Write Buffer ratio %s must be in (0, 1)",
                writeBufferRatio);
        this.writeBufferRatio = writeBufferRatio;
    }

    /**
     * Sets the fraction of the total memory to be used for high priority blocks like indexes,
     * dictionaries, etc. This only has an effect is either {@link #setUseManagedMemory(boolean)} or
     * {@link #setFixedMemoryPerSlot(MemorySize)} are set.
     *
     * <p>See {@link RocksDBOptions#HIGH_PRIORITY_POOL_RATIO} for details.
     */
    public void setHighPriorityPoolRatio(double highPriorityPoolRatio) {
        Preconditions.checkArgument(
                highPriorityPoolRatio > 0 && highPriorityPoolRatio < 1.0,
                "High priority pool ratio %s must be in (0, 1)",
                highPriorityPoolRatio);
        this.highPriorityPoolRatio = highPriorityPoolRatio;
    }

    /**
     * Gets whether the state backend is configured to use the managed memory of a slot for RocksDB.
     * See {@link RocksDBOptions#USE_MANAGED_MEMORY} for details.
     */
    public boolean isUsingManagedMemory() {
        return useManagedMemory != null
                ? useManagedMemory
                : RocksDBOptions.USE_MANAGED_MEMORY.defaultValue();
    }

    /**
     * Gets whether the state backend is configured to use a fixed amount of memory shared between
     * all RocksDB instances (in all tasks and operators) of a slot. See {@link
     * RocksDBOptions#FIX_PER_SLOT_MEMORY_SIZE} for details.
     */
    public boolean isUsingFixedMemoryPerSlot() {
        return fixedMemoryPerSlot != null;
    }

    /**
     * Gets the fixed amount of memory to be shared between all RocksDB instances (in all tasks and
     * operators) of a slot. Null is not configured. See {@link RocksDBOptions#USE_MANAGED_MEMORY}
     * for details.
     */
    @Nullable
    public MemorySize getFixedMemoryPerSlot() {
        return fixedMemoryPerSlot;
    }

    /**
     * Gets the fraction of the total memory to be used for write buffers. This only has an effect
     * is either {@link #setUseManagedMemory(boolean)} or {@link #setFixedMemoryPerSlot(MemorySize)}
     * are set.
     *
     * <p>See {@link RocksDBOptions#WRITE_BUFFER_RATIO} for details.
     */
    public double getWriteBufferRatio() {
        return writeBufferRatio != null
                ? writeBufferRatio
                : RocksDBOptions.WRITE_BUFFER_RATIO.defaultValue();
    }

    /**
     * Gets the fraction of the total memory to be used for high priority blocks like indexes,
     * dictionaries, etc. This only has an effect is either {@link #setUseManagedMemory(boolean)} or
     * {@link #setFixedMemoryPerSlot(MemorySize)} are set.
     *
     * <p>See {@link RocksDBOptions#HIGH_PRIORITY_POOL_RATIO} for details.
     */
    public double getHighPriorityPoolRatio() {
        return highPriorityPoolRatio != null
                ? highPriorityPoolRatio
                : RocksDBOptions.HIGH_PRIORITY_POOL_RATIO.defaultValue();
    }

    /**
     * Gets whether the state backend is configured to use partitioned index/filters for RocksDB.
     *
     * <p>See {@link RocksDBOptions#USE_PARTITIONED_INDEX_FILTERS} for details.
     */
    public Boolean isUsingPartitionedIndexFilters() {
        return usePartitionedIndexFilters != null
                ? usePartitionedIndexFilters
                : RocksDBOptions.USE_PARTITIONED_INDEX_FILTERS.defaultValue();
    }

    // ------------------------------------------------------------------------

    /** Validates if the configured options are valid with respect to one another. */
    public void validate() {
        // As FLINK-15512 introduce a new mechanism to calculate the cache capacity,
        // the relationship of write_buffer_manager_capacity and cache_capacity has changed to:
        // write_buffer_manager_capacity / cache_capacity = 2 * writeBufferRatio / (3 -
        // writeBufferRatio)
        // we should ensure the sum of write buffer manager capacity and high priority pool less
        // than cache capacity.
        // TODO change the formula once FLINK-15532 resolved.
        if (writeBufferRatio != null
                && highPriorityPoolRatio != null
                && 2 * writeBufferRatio / (3 - writeBufferRatio) + highPriorityPoolRatio >= 1.0) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid configuration: writeBufferRatio %s with highPriPoolRatio %s",
                            writeBufferRatio, highPriorityPoolRatio));
        }
    }

    // ------------------------------------------------------------------------

    /**
     * Derives a RocksDBMemoryConfiguration from another object and a configuration. The values set
     * on the other object take precedence, and the values from the configuration are used if no
     * values are set on the other config object.
     */
    public static RocksDBMemoryConfiguration fromOtherAndConfiguration(
            RocksDBMemoryConfiguration other, ReadableConfig config) {

        final RocksDBMemoryConfiguration newConfig = new RocksDBMemoryConfiguration();

        newConfig.useManagedMemory =
                other.useManagedMemory != null
                        ? other.useManagedMemory
                        : config.get(RocksDBOptions.USE_MANAGED_MEMORY);

        newConfig.fixedMemoryPerSlot =
                other.fixedMemoryPerSlot != null
                        ? other.fixedMemoryPerSlot
                        : config.get(RocksDBOptions.FIX_PER_SLOT_MEMORY_SIZE);

        newConfig.writeBufferRatio =
                other.writeBufferRatio != null
                        ? other.writeBufferRatio
                        : config.get(RocksDBOptions.WRITE_BUFFER_RATIO);

        newConfig.highPriorityPoolRatio =
                other.highPriorityPoolRatio != null
                        ? other.highPriorityPoolRatio
                        : config.get(RocksDBOptions.HIGH_PRIORITY_POOL_RATIO);

        newConfig.usePartitionedIndexFilters =
                other.usePartitionedIndexFilters != null
                        ? other.usePartitionedIndexFilters
                        : config.get(RocksDBOptions.USE_PARTITIONED_INDEX_FILTERS);

        return newConfig;
    }

    public static RocksDBMemoryConfiguration fromConfiguration(Configuration configuration) {
        return fromOtherAndConfiguration(new RocksDBMemoryConfiguration(), configuration);
    }
}
