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

package org.apache.flink.runtime.operators.shipping;

import org.apache.flink.api.common.distributions.DataDistribution;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.util.MathUtils;

/**
 * The output emitter decides to which of the possibly multiple output channels a record is sent. It
 * implement routing based on hash-partitioning, broadcasting, round-robin, custom partition
 * functions, etc.
 *
 * @param <T> The type of the element handled by the emitter.
 */
public class OutputEmitter<T> implements ChannelSelector<SerializationDelegate<T>> {

    /** the shipping strategy used by this output emitter */
    private final ShipStrategyType strategy;

    /** counter to go over channels round robin */
    private int nextChannelToSendTo;

    /** the total number of output channels */
    private int numberOfChannels;

    /** the comparator for hashing / sorting */
    private final TypeComparator<T> comparator;

    private Object[][] partitionBoundaries; // the partition boundaries for range partitioning

    private DataDistribution
            distribution; // the data distribution to create the partition boundaries for range
    // partitioning

    private final Partitioner<Object> partitioner;

    private TypeComparator[] flatComparators;

    private Object[] keys;

    private Object[] extractedKeys;

    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    /**
     * Creates a new channel selector that uses the given strategy (broadcasting, partitioning, ...)
     * and uses the supplied task index perform a round robin distribution.
     *
     * @param strategy The distribution strategy to be used.
     */
    public OutputEmitter(ShipStrategyType strategy, int indexInSubtaskGroup) {
        this(strategy, indexInSubtaskGroup, null, null, null);
    }

    /**
     * Creates a new channel selector that uses the given strategy (broadcasting, partitioning, ...)
     * and uses the supplied comparator to hash / compare records for partitioning them
     * deterministically.
     *
     * @param strategy The distribution strategy to be used.
     * @param comparator The comparator used to hash / compare the records.
     */
    public OutputEmitter(ShipStrategyType strategy, TypeComparator<T> comparator) {
        this(strategy, 0, comparator, null, null);
    }

    @SuppressWarnings("unchecked")
    public OutputEmitter(
            ShipStrategyType strategy,
            int indexInSubtaskGroup,
            TypeComparator<T> comparator,
            Partitioner<?> partitioner,
            DataDistribution distribution) {
        if (strategy == null) {
            throw new NullPointerException();
        }

        this.strategy = strategy;
        this.nextChannelToSendTo = indexInSubtaskGroup;
        this.comparator = comparator;
        this.partitioner = (Partitioner<Object>) partitioner;
        this.distribution = distribution;

        switch (strategy) {
            case PARTITION_CUSTOM:
                extractedKeys = new Object[1];
            case FORWARD:
            case PARTITION_HASH:
            case PARTITION_RANDOM:
            case PARTITION_FORCED_REBALANCE:
                break;
            case PARTITION_RANGE:
                if (comparator != null) {
                    this.flatComparators = comparator.getFlatComparators();
                    this.keys = new Object[flatComparators.length];
                }
                break;
            case BROADCAST:
                break;
            default:
                throw new IllegalArgumentException(
                        "Invalid shipping strategy for OutputEmitter: " + strategy.name());
        }

        if (strategy == ShipStrategyType.PARTITION_CUSTOM && partitioner == null) {
            throw new NullPointerException(
                    "Partitioner must not be null when the ship strategy is set to custom partitioning.");
        }
    }

    // ------------------------------------------------------------------------
    // Channel Selection
    // ------------------------------------------------------------------------

    @Override
    public void setup(int numberOfChannels) {
        this.numberOfChannels = numberOfChannels;
    }

    @Override
    public final int selectChannel(SerializationDelegate<T> record) {
        switch (strategy) {
            case FORWARD:
                return forward();
            case PARTITION_RANDOM:
            case PARTITION_FORCED_REBALANCE:
                return robin(numberOfChannels);
            case PARTITION_HASH:
                return hashPartitionDefault(record.getInstance(), numberOfChannels);
            case PARTITION_CUSTOM:
                return customPartition(record.getInstance(), numberOfChannels);
            case PARTITION_RANGE:
                return rangePartition(record.getInstance(), numberOfChannels);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported distribution strategy: " + strategy.name());
        }
    }

    @Override
    public boolean isBroadcast() {
        if (strategy == ShipStrategyType.BROADCAST) {
            return true;
        } else {
            return false;
        }
    }

    // --------------------------------------------------------------------------------------------

    private int forward() {
        return 0;
    }

    private int robin(int numberOfChannels) {
        int nextChannel = nextChannelToSendTo;
        if (nextChannel >= numberOfChannels) {
            if (nextChannel == numberOfChannels) {
                nextChannel = 0;
            } else {
                nextChannel %= numberOfChannels;
            }
        }
        nextChannelToSendTo = nextChannel + 1;

        return nextChannel;
    }

    private int hashPartitionDefault(T record, int numberOfChannels) {
        int hash = this.comparator.hash(record);

        return MathUtils.murmurHash(hash) % numberOfChannels;
    }

    private int rangePartition(final T record, int numberOfChannels) {
        if (this.partitionBoundaries == null) {
            this.partitionBoundaries = new Object[numberOfChannels - 1][];
            for (int i = 0; i < numberOfChannels - 1; i++) {
                this.partitionBoundaries[i] =
                        this.distribution.getBucketBoundary(i, numberOfChannels);
            }
        }

        if (numberOfChannels == this.partitionBoundaries.length + 1) {
            final Object[][] boundaries = this.partitionBoundaries;

            // bin search the bucket
            int low = 0;
            int high = this.partitionBoundaries.length - 1;

            while (low <= high) {
                final int mid = (low + high) >>> 1;
                final int result = compareRecordAndBoundary(record, boundaries[mid]);

                if (result > 0) {
                    low = mid + 1;
                } else if (result < 0) {
                    high = mid - 1;
                } else {
                    return mid;
                }
            }
            // key not found, but the low index is the target bucket, since the boundaries are the
            // upper bound
            return low;
        } else {
            throw new IllegalStateException(
                    "The number of channels to partition among is inconsistent with the partitioners state.");
        }
    }

    private int customPartition(T record, int numberOfChannels) {
        if (extractedKeys == null) {
            extractedKeys = new Object[1];
        }

        try {
            if (comparator.extractKeys(record, extractedKeys, 0) == 1) {
                final Object key = extractedKeys[0];
                return partitioner.partition(key, numberOfChannels);
            } else {
                throw new RuntimeException(
                        "Inconsistency in the key comparator - comparator extracted more than one field.");
            }
        } catch (Throwable t) {
            throw new RuntimeException("Error while calling custom partitioner.", t);
        }
    }

    private final int compareRecordAndBoundary(T record, Object[] boundary) {
        this.comparator.extractKeys(record, keys, 0);

        if (flatComparators.length != keys.length || flatComparators.length > boundary.length) {
            throw new RuntimeException(
                    "Can not compare keys with boundary due to mismatched length.");
        }

        for (int i = 0; i < flatComparators.length; i++) {
            int result = flatComparators[i].compare(keys[i], boundary[i]);
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }
}
