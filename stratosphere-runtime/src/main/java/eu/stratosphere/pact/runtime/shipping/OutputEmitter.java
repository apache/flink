/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.shipping;

import eu.stratosphere.api.common.distributions.DataDistribution;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.runtime.io.api.ChannelSelector;
import eu.stratosphere.pact.runtime.plugable.SerializationDelegate;

public class OutputEmitter<T> implements ChannelSelector<SerializationDelegate<T>> {
	
	private final ShipStrategyType strategy;		// the shipping strategy used by this output emitter
	
	private int[] channels;						// the reused array defining target channels
	
	private int nextChannelToSendTo = 0;		// counter to go over channels round robin
	
	private final TypeComparator<T> comparator;	// the comparator for hashing / sorting

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
		this(strategy, comparator, null);
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
		if (strategy == null) { 
			throw new NullPointerException();
		}
		
		this.strategy = strategy;
		this.comparator = comparator;
		
		switch (strategy) {
		case FORWARD:
		case PARTITION_HASH:
		case PARTITION_RANGE:
		case PARTITION_RANDOM:
		case BROADCAST:
			break;
		default:
			throw new IllegalArgumentException("Invalid shipping strategy for OutputEmitter: " + strategy.name());
		}
		
		if ((strategy == ShipStrategyType.PARTITION_RANGE) && distr == null) {
			throw new NullPointerException("Data distribution must not be null when the ship strategy is range partitioning.");
		}
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
			for (int i = 0; i < numberOfChannels; i++) {
				channels[i] = i;
			}
		}

		return channels;
	}

	private final int[] hashPartitionDefault(T record, int numberOfChannels) {
		if (channels == null || channels.length != 1) {
			channels = new int[1];
		}

		int hash = this.comparator.hash(record);

		hash = murmurHash(hash);

		if (hash >= 0) {
			this.channels[0] = hash % numberOfChannels;
		}
		else if (hash != Integer.MIN_VALUE) {
			this.channels[0] = -hash % numberOfChannels;
		}
		else {
			this.channels[0] = 0;
		}
	
		return this.channels;
	}

	private final int murmurHash(int k) {
		k *= 0xcc9e2d51;
		k = Integer.rotateLeft(k, 15);
		k *= 0x1b873593;
		
		k = Integer.rotateLeft(k, 13);
		k *= 0xe6546b64;

		k ^= 4;
		k ^= k >>> 16;
		k *= 0x85ebca6b;
		k ^= k >>> 13;
		k *= 0xc2b2ae35;
		k ^= k >>> 16;

		return k;
	}

	private final int[] rangePartition(T record, int numberOfChannels) {
		throw new UnsupportedOperationException();
	}
}
