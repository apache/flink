/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
import eu.stratosphere.pact.common.generic.types.TypeComparator;
import eu.stratosphere.pact.runtime.plugable.SerializationDelegate;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy.ShipStrategyType;

/**
 * @author Erik Nijkamp
 * @author Alexander Alexandrov
 * @author Stephan Ewen
 */
public class OutputEmitter<T> implements ChannelSelector<SerializationDelegate<T>>
{
	private static final byte[] DEFAULT_SALT = new byte[] { 17, 31, 47, 51, 83, 1 };
	
	private final ShipStrategyType strategy;		// the shipping strategy used by this output emitter
	
	private int[] channels;						// the reused array defining target channels
	
	private int nextChannelToSendTo = 0;		// counter to go over channels round robin
	
	private final TypeComparator<T> comparator;	// the comparator for hashing / sorting
	
	private final byte[] salt;					// the salt used to randomize the hash values

	// ------------------------------------------------------------------------
	// Constructors
	// ------------------------------------------------------------------------

	/**
	 * Creates a new channel selector that distributes data round robin.
	 */
	public OutputEmitter()
	{
		this(ShipStrategyType.NONE);
	}

	/**
	 * Creates a new channel selector that uses the given strategy (broadcasting, partitioning, ...).
	 * 
	 * @param strategy The distribution strategy to be used.
	 */
	public OutputEmitter(ShipStrategyType strategy)
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
	public OutputEmitter(ShipStrategyType strategy, TypeComparator<T> comparator)
	{
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
	public OutputEmitter(ShipStrategyType strategy, TypeComparator<T> comparator, byte[] salt)
	{
		if (strategy == null | salt == null) { 
			throw new NullPointerException();
		}
		this.strategy = strategy;
		this.salt = salt;
		this.comparator = comparator;
	}

	// ------------------------------------------------------------------------
	// Channel Selection
	// ------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.ChannelSelector#selectChannels(java.lang.Object, int)
	 */
	@Override
	public final int[] selectChannels(SerializationDelegate<T> record, int numberOfChannels)
	{
		switch (strategy) {
		case FORWARD:
			return robin(numberOfChannels);
		case PARTITION_HASH:
		case PARTITION_LOCAL_HASH:
			return hashPartitionDefault(record.getInstance(), numberOfChannels);
		case BROADCAST:
			return broadcast(numberOfChannels);
		default:
			throw new UnsupportedOperationException("Unsupported distribution strategy: " + strategy.name());
		}
	}
	
	// --------------------------------------------------------------------------------------------

	private final int[] robin(int numberOfChannels)
	{
		if (this.channels == null || this.channels.length != 1) {
			this.channels = new int[1];
		}
		
		int nextChannel = nextChannelToSendTo + 1;
		nextChannel = nextChannel < numberOfChannels ? nextChannel : 0;
		
		this.nextChannelToSendTo = nextChannel;
		this.channels[0] = nextChannel;
		return this.channels;
	}

	private final int[] broadcast(int numberOfChannels)
	{
		if (channels == null || channels.length != numberOfChannels) {
			channels = new int[numberOfChannels];
			for (int i = 0; i < numberOfChannels; i++)
				channels[i] = i;
		}

		return channels;
	}

	private final int[] hashPartitionDefault(T record, int numberOfChannels)
	{
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
}
