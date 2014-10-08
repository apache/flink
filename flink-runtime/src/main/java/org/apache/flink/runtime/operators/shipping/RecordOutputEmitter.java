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
import org.apache.flink.types.Key;
import org.apache.flink.types.Record;

public class RecordOutputEmitter implements ChannelSelector<Record> {
	
	// ------------------------------------------------------------------------
	// Fields
	// ------------------------------------------------------------------------

	private static final byte[] DEFAULT_SALT = new byte[] { 17, 31, 47, 51, 83, 1 };
	
	private final ShipStrategyType strategy;			// the shipping strategy used by this output emitter
	
	private final TypeComparator<Record> comparator;	// the comparator for hashing / sorting
	
	private int[] channels;							// the reused array defining target channels
	
	private Key<?>[][] partitionBoundaries;		// the partition boundaries for range partitioning
	
	private final DataDistribution distribution; // the data distribution to create the partition boundaries for range partitioning
	
	private final Partitioner<Object> partitioner;
	
	private int nextChannelToSendTo;				// counter to go over channels round robin
	
	private Object[] extractedKeys;

	// ------------------------------------------------------------------------
	// Constructors
	// ------------------------------------------------------------------------

	/**
	 * Creates a new channel selector that uses the given strategy (broadcasting, partitioning, ...).
	 * 
	 * @param strategy The distribution strategy to be used.
	 */
	public RecordOutputEmitter(ShipStrategyType strategy) {
		this(strategy, null);
	}	
	
	/**
	 * Creates a new channel selector that uses the given strategy (broadcasting, partitioning, ...)
	 * and uses the supplied comparator to hash / compare records for partitioning them deterministically.
	 * 
	 * @param strategy The distribution strategy to be used.
	 * @param comparator The comparator used to hash / compare the records.
	 */
	public RecordOutputEmitter(ShipStrategyType strategy, TypeComparator<Record> comparator) {
		this(strategy, comparator, null, null);
	}

	/**
	 * Creates a new channel selector that uses the given strategy (broadcasting, partitioning, ...)
	 * and uses the supplied comparator to hash / compare records for partitioning them deterministically.
	 * 
	 * @param strategy The distribution strategy to be used.
	 * @param comparator The comparator used to hash / compare the records.
	 * @param distr The distribution pattern used in the case of a range partitioning.
	 */
	public RecordOutputEmitter(ShipStrategyType strategy, TypeComparator<Record> comparator, DataDistribution distr) {
		this(strategy, comparator, null, distr);
	}
	
	public RecordOutputEmitter(ShipStrategyType strategy, TypeComparator<Record> comparator, Partitioner<?> partitioner) {
		this(strategy, comparator, partitioner, null);
	}
		
	@SuppressWarnings("unchecked")
	public RecordOutputEmitter(ShipStrategyType strategy, TypeComparator<Record> comparator, Partitioner<?> partitioner, DataDistribution distr) {
		if (strategy == null) { 
			throw new NullPointerException();
		}
		
		this.strategy = strategy;
		this.comparator = comparator;
		this.distribution = distr;
		this.partitioner = (Partitioner<Object>) partitioner;
		
		switch (strategy) {
		case FORWARD:
		case PARTITION_FORCED_REBALANCE:
		case PARTITION_HASH:
		case PARTITION_RANGE:
		case PARTITION_RANDOM:
			this.channels = new int[1];
			break;
		case BROADCAST:
		case PARTITION_CUSTOM:
			break;
		default:
			throw new IllegalArgumentException("Invalid shipping strategy for OutputEmitter: " + strategy.name());
		}
		
		if (strategy == ShipStrategyType.PARTITION_RANGE && distr == null) {
			throw new NullPointerException("Data distribution must not be null when the ship strategy is range partitioning.");
		}
		if (strategy == ShipStrategyType.PARTITION_CUSTOM && partitioner == null) {
			throw new NullPointerException("Partitioner must not be null when the ship strategy is set to custom partitioning.");
		}
	}

	// ------------------------------------------------------------------------
	// Channel Selection
	// ------------------------------------------------------------------------

	@Override
	public final int[] selectChannels(Record record, int numberOfChannels) {
		switch (strategy) {
		case FORWARD:
		case PARTITION_RANDOM:
		case PARTITION_FORCED_REBALANCE:
			return robin(numberOfChannels);
		case PARTITION_HASH:
			return hashPartitionDefault(record, numberOfChannels);
		case PARTITION_CUSTOM:
			return customPartition(record, numberOfChannels);
		case BROADCAST:
			return broadcast(numberOfChannels);
		case PARTITION_RANGE:
			return rangePartition(record, numberOfChannels);
		default:
			throw new UnsupportedOperationException("Unsupported distribution strategy: " + strategy.name());
		}
	}
	
	// --------------------------------------------------------------------------------------------

	private final int[] robin(int numberOfChannels) {
		final int channel = this.nextChannelToSendTo;
		this.nextChannelToSendTo = channel > 0 ? channel - 1 : numberOfChannels - 1;
		this.channels[0] = channel;
		return this.channels;
	}

	private final int[] broadcast(int numberOfChannels) {
		if (this.channels == null || this.channels.length != numberOfChannels) {
			this.channels = new int[numberOfChannels];
			for (int i = 0; i < numberOfChannels; i++) {
				this.channels[i] = i;
			}
		}
		
		return this.channels;
	}

	private final int[] hashPartitionDefault(final Record record, int numberOfChannels) {
		int hash = this.comparator.hash(record);
		for (int i = 0; i < DEFAULT_SALT.length; i++) {
			hash ^= ((hash << 5) + DEFAULT_SALT[i] + (hash >> 2));
		}
		
		if(hash < 0) {
			if(hash == Integer.MIN_VALUE) {
				this.channels[0] = Integer.MAX_VALUE % numberOfChannels;
			} else {
				this.channels[0] = -hash % numberOfChannels;
			}
		} else {
			this.channels[0] = hash % numberOfChannels;
		}
		return this.channels;
	}
	
	private final int[] rangePartition(final Record record, int numberOfChannels) {
		if (this.partitionBoundaries == null) {
			this.partitionBoundaries = new Key[numberOfChannels - 1][];
			for (int i = 0; i < numberOfChannels - 1; i++) {
				this.partitionBoundaries[i] = this.distribution.getBucketBoundary(i, numberOfChannels);
			}
		}
		
		if (numberOfChannels == this.partitionBoundaries.length + 1) {
			final Key<?>[][] boundaries = this.partitionBoundaries;
			this.comparator.setReference(record);
			
			// bin search the bucket
			int low = 0;
			int high = this.partitionBoundaries.length - 1;
			
			while (low <= high) {
				final int mid = (low + high) >>> 1;
				final int result = this.comparator.compareAgainstReference(boundaries[mid]);
				
				if (result < 0) {
					low = mid + 1;
				} else if (result > 0) {
					high = mid - 1;
				} else {
					this.channels[0] = mid;
					return this.channels;
				}
			}
			this.channels[0] = low;	// key not found, but the low index is the target
									// bucket, since the boundaries are the upper bound
			return this.channels;
		} else {
			throw new IllegalStateException(
			"The number of channels to partition among is inconsistent with the partitioners state.");
		}
	}
	
	private final int[] customPartition(Record record, int numberOfChannels) {
		if (channels == null) {
			channels = new int[1];
			extractedKeys = new Object[1];
		}
		
		try {
			if (comparator.extractKeys(record, extractedKeys, 0) == 1) {
				final Object key = extractedKeys[0];
				channels[0] = partitioner.partition(key, numberOfChannels);
				return channels;
			}
			else {
				throw new RuntimeException("Inconsistency in the key comparator - comparator extracted more than one field.");
			}
		}
		catch (Throwable t) {
			throw new RuntimeException("Error while calling custom partitioner.", t);
		}
	}
}
