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

import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.pact.common.contract.DataDistribution;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordComparator;


public class PactRecordOutputEmitter implements ChannelSelector<PactRecord>
{
	// ------------------------------------------------------------------------
	// Fields
	// ------------------------------------------------------------------------

	private static final byte[] DEFAULT_SALT = new byte[] { 17, 31, 47, 51, 83, 1 };
	
	private final ShipStrategyType strategy;			// the shipping strategy used by this output emitter
	
	private final PactRecordComparator comparator;	// the comparator for hashing / sorting
	
	private int[] channels;							// the reused array defining target channels
	
	private Key[][] partitionBoundaries;		// the partition boundaries for range partitioning
	
	private final DataDistribution<PactRecord> distribution; // the data distribution to create the partition boundaries for range partitioning
	
	private int nextChannelToSendTo;				// counter to go over channels round robin

	// ------------------------------------------------------------------------
	// Constructors
	// ------------------------------------------------------------------------

	/**
	 * Creates a new channel selector that uses the given strategy (broadcasting, partitioning, ...).
	 * 
	 * @param strategy The distribution strategy to be used.
	 */
	public PactRecordOutputEmitter(ShipStrategyType strategy)
	{
		this(strategy, null);
	}	
	
	/**
	 * Creates a new channel selector that uses the given strategy (broadcasting, partitioning, ...)
	 * and uses the supplied comparator to hash / compare records for partitioning them deterministically.
	 * 
	 * @param strategy The distribution strategy to be used.
	 * @param comparator The comparator used to hash / compare the records.
	 */
	public PactRecordOutputEmitter(ShipStrategyType strategy, PactRecordComparator comparator)
	{
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
	public PactRecordOutputEmitter(ShipStrategyType strategy, PactRecordComparator comparator, DataDistribution<PactRecord> distr)
	{
		if (strategy == null) { 
			throw new NullPointerException();
		}
		
		this.strategy = strategy;
		this.comparator = comparator;
		this.distribution = distr;
		
		switch (strategy) {
		case FORWARD:
		case PARTITION_HASH:
		case PARTITION_LOCAL_HASH:
		case PARTITION_RANGE:
		case PARTITION_RANDOM:
			this.channels = new int[1];
			break;
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

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.ChannelSelector#selectChannels(java.lang.Object, int)
	 */
	@Override
	public final int[] selectChannels(PactRecord record, int numberOfChannels)
	{
		switch (strategy) {
		case FORWARD:
		case PARTITION_RANDOM:
			return robin(numberOfChannels);
		case PARTITION_HASH:
		case PARTITION_LOCAL_HASH:
			return hashPartitionDefault(record, numberOfChannels);
		case PARTITION_RANGE:
			return rangePartition(record, numberOfChannels);
		case BROADCAST:
			return broadcast(numberOfChannels);
		default:
			throw new UnsupportedOperationException("Unsupported distribution strategy: " + strategy.name());
		}
	}
	
	// --------------------------------------------------------------------------------------------

	private final int[] robin(int numberOfChannels)
	{
		final int channel = this.nextChannelToSendTo;
		this.nextChannelToSendTo = channel > 0 ? channel - 1 : numberOfChannels - 1;
		this.channels[0] = channel;
		return this.channels;
	}

	private final int[] broadcast(int numberOfChannels)
	{
		if (this.channels == null || this.channels.length != numberOfChannels) {
			this.channels = new int[numberOfChannels];
			for (int i = 0; i < numberOfChannels; i++) {
				this.channels[i] = i;
			}
		}
		
		return this.channels;
	}

	private final int[] hashPartitionDefault(final PactRecord record, int numberOfChannels)
	{
		int hash = this.comparator.hash(record);
		for (int i = 0; i < DEFAULT_SALT.length; i++) {
			hash ^= ((hash << 5) + DEFAULT_SALT[i] + (hash >> 2));
		}
		this.channels[0] = (hash < 0) ? -hash % numberOfChannels : hash % numberOfChannels;
		return this.channels;
	}
	
	private final int[] rangePartition(final PactRecord record, int numberOfChannels)
	{
		if (this.partitionBoundaries == null) {
			this.partitionBoundaries = new Key[numberOfChannels - 1][];
			for (int i = 0; i < numberOfChannels - 1; i++) {
				final PactRecord boundary = this.distribution.getBucketBoundary(i, numberOfChannels);
				this.partitionBoundaries[i] = comparator.getKeysAsCopy(boundary);
			}
		}
		
		if (numberOfChannels == this.partitionBoundaries.length + 1) {
			final Key[][] boundaries = this.partitionBoundaries;
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
}
