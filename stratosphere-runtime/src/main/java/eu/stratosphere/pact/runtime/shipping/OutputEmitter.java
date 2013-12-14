/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.shipping;

import eu.stratosphere.api.distributions.DataDistribution;
import eu.stratosphere.api.typeutils.TypeComparator;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.pact.runtime.plugable.SerializationDelegate;


public class OutputEmitter<T> implements ChannelSelector<SerializationDelegate<T>> {
	
	private static final byte[] DEFAULT_SALT = new byte[] { 17, 31, 47, 51, 83, 1 };
	
	private final ShipStrategyType strategy;		// the shipping strategy used by this output emitter
	
	private int[] channels;						// the reused array defining target channels
	
	private int nextChannelToSendTo = 0;		// counter to go over channels round robin
	
	private final TypeComparator<T> comparator;	// the comparator for hashing / sorting
	
//	private Key[][] partitionBoundaries;		// the partition boundaries for range partitioning
//	
//	private final DataDistribution distribution; // the data distribution to create the partition boundaries for range partitioning
	
	private final byte[] salt;					// the salt used to randomize the hash values

	// ------------------------------------------------------------------------
	// Constructors
	// ------------------------------------------------------------------------

	/**
	 * Creates a new channel selector that distributes data round robin.
	 */
	public OutputEmitter() {
		this(ShipStrategyType.NONE);
	}

	/**
	 * Creates a new channel selector that uses the given strategy (broadcasting, partitioning, ...).
	 * 
	 * @param strategy The distribution strategy to be used.
	 */
	public OutputEmitter(ShipStrategyType strategy) {
		this(strategy, null);
	}	
	
	/**
	 * Creates a new channel selector that uses the given strategy (broadcasting, partitioning, ...)
	 * and uses the supplied comparator to hash / compare records for partitioning them deterministically.
	 * 
	 * @param strategy The distribution strategy to be used.
	 * @param comparator The comparator used to hash / compare the records.
	 */
	public OutputEmitter(ShipStrategyType strategy, TypeComparator<T> comparator) {
		this(strategy, comparator, DEFAULT_SALT);
	}
	
	/**
	 * Creates a new channel selector that uses the given strategy (broadcasting, partitioning, ...)
	 * and uses the supplied comparator to hash / compare records for partitioning them deterministically.
	 * 
	 * @param strategy The distribution strategy to be used.
	 * @param comparator The comparator used to hash / compare the records.
	 * @param salt The salt to use to randomize the hashes.
	 */
	public OutputEmitter(ShipStrategyType strategy, TypeComparator<T> comparator, byte[] salt) {
		this(strategy, comparator, salt, null);
	}
	
	/**
	 * Creates a new channel selector that uses the given strategy (broadcasting, partitioning, ...)
	 * and uses the supplied comparator to hash / compare records for partitioning them deterministically.
	 * 
	 * @param strategy The distribution strategy to be used.
	 * @param comparator The comparator used to hash / compare the records.
	 * @param distr The distribution pattern used in the case of a range partitioning.
	 */
	public OutputEmitter(ShipStrategyType strategy, TypeComparator<T> comparator, DataDistribution distr) {
		this(strategy, comparator, DEFAULT_SALT, distr);
	}
	
	/**
	 * Creates a new channel selector that uses the given strategy (broadcasting, partitioning, ...)
	 * and uses the supplied comparator to hash / compare records for partitioning them deterministically.
	 * 
	 * @param strategy The distribution strategy to be used.
	 * @param comparator The comparator used to hash / compare the records.
	 * @param salt The salt to use to randomize the hashes.
	 * @param distr The distribution pattern used in the case of a range partitioning.
	 */
	public OutputEmitter(ShipStrategyType strategy, TypeComparator<T> comparator, byte[] salt, DataDistribution distr) {
		if (strategy == null || salt == null) { 
			throw new NullPointerException();
		}
		
		this.strategy = strategy;
		this.comparator = comparator;
		this.salt = salt;
//		this.distribution = distr;
		
		switch (strategy) {
		case FORWARD:
		case PARTITION_HASH:
		case PARTITION_LOCAL_HASH:
		case PARTITION_RANGE:
		case PARTITION_RANDOM:
		case BROADCAST:
			break;
		default:
			throw new IllegalArgumentException("Invalid shipping strategy for OutputEmitter: " + strategy.name());
		}
		
		if ((strategy == ShipStrategyType.PARTITION_RANGE) && distr == null)
			throw new NullPointerException("Data distribution must not be null when the ship strategy is range partitioning.");
	}

	// ------------------------------------------------------------------------
	// Channel Selection
	// ------------------------------------------------------------------------

	@Override
	public final int[] selectChannels(SerializationDelegate<T> record, int numberOfChannels) {
		switch (strategy) {
		case FORWARD:
		case PARTITION_RANDOM:
			return robin(numberOfChannels);
		case PARTITION_HASH:
		case PARTITION_LOCAL_HASH:
			return hashPartitionDefault(record.getInstance(), numberOfChannels);
		case PARTITION_RANGE:
			return rangePartition(record.getInstance(), numberOfChannels);
		case BROADCAST:
			return broadcast(numberOfChannels);
		default:
			throw new UnsupportedOperationException("Unsupported distribution strategy: " + strategy.name());
		}
	}
	
	// --------------------------------------------------------------------------------------------

	private final int[] robin(int numberOfChannels) {
		if (this.channels == null || this.channels.length != 1) {
			this.channels = new int[1];
		}
		
		int nextChannel = nextChannelToSendTo + 1;
		nextChannel = nextChannel < numberOfChannels ? nextChannel : 0;
		
		this.nextChannelToSendTo = nextChannel;
		this.channels[0] = nextChannel;
		return this.channels;
	}

	private final int[] broadcast(int numberOfChannels) {
		if (channels == null || channels.length != numberOfChannels) {
			channels = new int[numberOfChannels];
			for (int i = 0; i < numberOfChannels; i++)
				channels[i] = i;
		}

		return channels;
	}

	private final int[] hashPartitionDefault(T record, int numberOfChannels) {
		if (channels == null || channels.length != 1) {
			channels = new int[1];
		}
		
		int hash = this.comparator.hash(record);
		for (int i = 0; i < this.salt.length; i++) {
			hash ^= ((hash << 5) + this.salt[i] + (hash >> 2));
		}
	
		this.channels[0] = (hash < 0) ? -hash % numberOfChannels : hash % numberOfChannels;
		return this.channels;
	}
	
	private final int[] rangePartition(T record, int numberOfChannels) {
		throw new UnsupportedOperationException();
//		if (channels == null || channels.length != 1) {
//			channels = new int[1];
//		}
//		
//		if (this.partitionBoundaries == null) {
//			this.partitionBoundaries = new Key[numberOfChannels - 1][];
//			for (int i = 0; i < numberOfChannels - 1; i++) {
//				this.partitionBoundaries[i] = this.distribution.getBucketBoundary(i, numberOfChannels);
//			}
//		}
//		
//		if (numberOfChannels == this.partitionBoundaries.length + 1) {
//			final TypeComparator<T>[] boundaries = this.partitionBoundaries;
//			this.comparator.setReference(record);
//			
//			// bin search the bucket
//			int low = 0;
//			int high = this.partitionBoundaries.length - 1;
//			
//			while (low <= high) {
//				final int mid = (low + high) >>> 1;
//				final int result = this.comparator.compareToReference(boundaries[mid]);
//				
//				if (result < 0) {
//					low = mid + 1;
//				} else if (result > 0) {
//					high = mid - 1;
//				} else {
//					this.channels[0] = mid;
//					return this.channels;
//				}
//			}
//			this.channels[0] = low;	// key not found, but the low index is the target
//									// bucket, since the boundaries are the upper bound
//			return this.channels;
//		} else {
//			throw new IllegalStateException(
//			"The number of channels to partition among is inconsistent with the partitioners state.");
//		}
	}
}
