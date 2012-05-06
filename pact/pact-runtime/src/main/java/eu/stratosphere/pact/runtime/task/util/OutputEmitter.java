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

package eu.stratosphere.pact.runtime.task.util;

import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.pact.common.type.DeserializationException;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * @author Erik Nijkamp
 * @author Alexander Alexandrov
 * @author Stephan Ewen
 */
public class OutputEmitter implements ChannelSelector<PactRecord>
{
	/**
	 * Enumeration defining the different shipping types of the output, such as local forward, re-partitioning by hash,
	 * or re-partitioning by range.
	 */
	public enum ShipStrategy {
		FORWARD,
		PARTITION_HASH,
		PARTITION_LOCAL_HASH,
		PARTITION_RANGE,
		PARTITION_LOCAL_RANGE,
		BROADCAST,
		SFR,
		NONE
	}

	// ------------------------------------------------------------------------
	// Fields
	// ------------------------------------------------------------------------

	private static final byte[] DEFAULT_SALT = new byte[] { 17, 31, 47, 51, 83, 1 };
	
	private ShipStrategy strategy;				// the shipping strategy used by this output emitter
	
	private int[] channels;						// the reused array defining target channels
	
	private int nextChannelToSendTo = 0;		// counter to go over channels round robin
	
	private Class<? extends Key>[] keyClasses;
	
	private int[] keyPositions;
	
	private final byte[] salt;					// the salt used to randomize the hash values

	private PartitionFunction partitionFunction;

	// ------------------------------------------------------------------------
	// Constructors
	// ------------------------------------------------------------------------

	/**
	 * Creates a new channel selector that distributes data round robin.
	 */
	public OutputEmitter()
	{
		this(ShipStrategy.NONE);
	}

	/**
	 * Creates a new channel selector that uses the given strategy (broadcasting, partitioning, ...).
	 * 
	 * @param strategy
	 *        The distribution strategy to be used.
	 */
	public OutputEmitter(ShipStrategy strategy)
	{
		this.strategy = strategy;
		this.salt = DEFAULT_SALT;
	}	
		
	public OutputEmitter(ShipStrategy strategy, int[] keyPositions, Class<? extends Key>[] keyTypes)
	{
		this(strategy, DEFAULT_SALT, keyPositions, keyTypes);
	}
	
	public OutputEmitter(ShipStrategy strategy, byte[] salt , int[] keyPositions, Class<? extends Key>[] keyTypes)
	{
		if (strategy == null | salt == null | keyPositions == null | keyTypes == null) { 
			throw new NullPointerException();
		}
		this.strategy = strategy;
		this.salt = salt;
		this.keyPositions = keyPositions;
		this.keyClasses = keyTypes;
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
			return robin(numberOfChannels);
		case PARTITION_HASH:
		case PARTITION_LOCAL_HASH:
			return hashPartitionDefault(record, numberOfChannels);
		case PARTITION_RANGE:
			return partition_range(record, numberOfChannels);
		case BROADCAST:
			return broadcast(numberOfChannels);
		default:
			throw new UnsupportedOperationException("Unsupported distribution strategy: " + strategy.name());
		}
	}
	
	/**
	 * Set the partition function that is used for range partitioning
	 * @param func
	 */
	public void setPartitionFunction(PartitionFunction func) {
		this.partitionFunction = func;
	}

	private int[] partition_range(PactRecord record, int numberOfChannels) {
		try {
			partitionFunction.selectChannels(record, numberOfChannels, this.channels);
		} catch(NullPointerException npe) {
			throw new RuntimeException("Partition function for RangePartitioner not set!");
		}
		return this.channels;
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

	private final int[] hashPartitionDefault(PactRecord record, int numberOfChannels)
	{
		if (channels == null || channels.length != 1) {
			channels = new int[1];
		}
		
		int hash = 0;
		for (int i = 0; i < this.keyPositions.length; i++) {
			try {
				final Key k = record.getField(this.keyPositions[i], this.keyClasses[i]);
				hash ^= (1315423911 ^ ((1315423911 << 5) + k.hashCode() + (1315423911 >> 2)));
			} catch(IndexOutOfBoundsException ioobe) {
				throw new RuntimeException("Key field "+this.keyPositions[i]+" is of our bounds of record.", ioobe);
			} catch(NullPointerException npe) {
				throw new RuntimeException("Key field "+this.keyPositions[i]+" is null.", npe);
			} catch(DeserializationException de) {
				throw new RuntimeException("Key field "+this.keyPositions[i]+" of type '"+this.keyClasses[i].getName()+"' could not be deserialized.", de);
			}
		}
		
		for (int i = 0; i < salt.length; i++) {
			hash ^= ((hash << 5) + salt[i] + (hash >> 2));
		}
	
		this.channels[0] = (hash < 0) ? -hash % numberOfChannels : hash % numberOfChannels;
		return this.channels;
	}
}
