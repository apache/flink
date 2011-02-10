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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;

/**
 * 
 * 
 * @author Erik Nijkamp
 * @author Alexander Alexandrov
 * 
 * @param <K> The type of the key.
 * @param <V> The type of the value.
 */
public class OutputEmitter<K extends Key, V extends Value> implements ChannelSelector<KeyValuePair<K, V>>
{
	/**
	 * Enumeration defining the different shipping types of the output, such as local forward, re-partitioning by hash,
	 * or re-partitioning by range.
	 */
	public enum ShipStrategy {
		FORWARD, BROADCAST, PARTITION_HASH, PARTITION_RANGE, SFR, NONE
	}
	
	// ------------------------------------------------------------------------
	//                       Fields
	// ------------------------------------------------------------------------
	
	private final byte[] salt;					// the salt used to randomize the hash values
	
	private ShipStrategy strategy;				// the shipping strategy used by this output emitter
	
	private int[] channels;						// the reused array defining target channels
	
	private int nextChannelToSendTo = 0;		// counter to go over channels round robin 


	// ------------------------------------------------------------------------
	//                           Constructors
	// ------------------------------------------------------------------------

	/**
	 * Creates a new channel selector that distributes data round robin.
	 */
	public OutputEmitter() {
		this(ShipStrategy.NONE);
	}

	/**
	 * Creates a new channel selector that uses the given strategy (broadcasting, partitioning, ...).
	 * 
	 * @param strategy The distribution strategy to be used.
	 */
	public OutputEmitter(ShipStrategy strategy) {
		this(strategy, new byte[] { 17, 31, 47, 51, 83, 1 });
	}

	/**
	 * Creates a new channel selector that uses the given strategy (broadcasting, partitioning, ...), using the
	 * supplied salt to randomize hashes.
	 *  
	 * @param strategy The distribution strategy to be used.
	 * @param salt The salt used to randomize hash values.
	 */
	public OutputEmitter(ShipStrategy strategy, byte[] salt) {
		if (strategy != ShipStrategy.BROADCAST && strategy != ShipStrategy.PARTITION_HASH
				&& strategy != ShipStrategy.FORWARD)
		{
			throw new UnsupportedOperationException("Unsupported distribution strategy: " + strategy.name());
		}
		this.strategy = strategy;
		this.salt = salt;
	}
	
	
	// ------------------------------------------------------------------------
	//                          Channel Selection
	// ------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.ChannelSelector#selectChannels(java.lang.Object, int)
	 */
	@Override
	public int[] selectChannels(KeyValuePair<K, V> pair, int numberOfChannels) {
		switch (strategy) {
		case BROADCAST:
			return broadcast(numberOfChannels);
		case PARTITION_HASH:
			return partition(pair, numberOfChannels);
		case FORWARD:
			return robin(numberOfChannels);
		default:
			throw new UnsupportedOperationException();
		}
	}

	private int[] robin(int numberOfChannels) {
		nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels;
		return new int[] { nextChannelToSendTo };
	}

	private int[] broadcast(int numberOfChannels) {
		if (channels == null || channels.length != numberOfChannels) {
			channels = new int[numberOfChannels];
			for (int i = 0; i < numberOfChannels; i++)
				channels[i] = i;
		}
		
		return channels;
	}

	private int[] partition(KeyValuePair<K, V> pair, int numberOfChannels) {
		if (channels == null || channels.length != 1) {
			channels = new int[1];
		}
		channels[0] = getPartition(pair.getKey(), numberOfChannels);
		return channels;
	}

	private int getPartition(K key, int numberOfChannels) {
		int hash = 1315423911 ^ ((1315423911 << 5) + key.hashCode() + (1315423911 >> 2));

		for (int i = 0; i < salt.length; i++) {
			hash ^= ((hash << 5) + salt[i] + (hash >> 2));
		}

		return (hash < 0) ? -hash % numberOfChannels : hash % numberOfChannels;
	}
	
	// ------------------------------------------------------------------------
	//                            Serialization
	// ------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException {
		strategy = ShipStrategy.valueOf(in.readUTF());
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(strategy.name());
	}

}
