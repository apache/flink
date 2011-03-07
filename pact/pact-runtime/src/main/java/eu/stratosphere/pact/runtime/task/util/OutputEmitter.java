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
import java.util.Arrays;

import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 * @author Erik Nijkamp
 * @author Alexander Alexandrov
 * @param <K>
 * @param <V>
 */
public class OutputEmitter<K extends Key, V extends Value> implements ChannelSelector<KeyValuePair<K, V>> {
	private ShipStrategy strategy;

	private byte[] salt;
	
	private PactInteger[] histo = {
		new PactInteger(441),
		new PactInteger(1442),
		new PactInteger(2316),
		new PactInteger(3145),
		new PactInteger(4269),
		new PactInteger(5418),
		new PactInteger(6678),
		new PactInteger(7591),
		new PactInteger(8734)
	};

	private int nextChannelToSendTo = 0;

	public enum ShipStrategy {
		FORWARD, BROADCAST, PARTITION_HASH, PARTITION_RANGE, SFR, NONE
	}

	// TODO required by IOReadableWritable (en)
	public OutputEmitter() {
		this.salt = new byte[] { 17, 31, 47, 51, 83, 1 };
	}

	public OutputEmitter(ShipStrategy strategy) {
		this(strategy, new byte[] { 17, 31, 47, 51, 83, 1 });
	}

	public OutputEmitter(ShipStrategy strategy, byte[] salt) {
		this.strategy = strategy;
		this.salt = salt;
	}

	@Override
	public int[] selectChannels(KeyValuePair<K, V> pair, int numberOfChannels) {
		switch (strategy) {
		case BROADCAST:
			return broadcast(numberOfChannels);
		case PARTITION_HASH:
			return partition(pair, numberOfChannels);
		case PARTITION_RANGE:
			return range_partition(pair);
		default:
			return robin(numberOfChannels);
		}
	}

	private int[] robin(int numberOfChannels) {
		nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels;
		return new int[] { nextChannelToSendTo };
	}

	private int[] broadcast(int numberOfChannels) {
		int[] channels = new int[numberOfChannels];
		for (int i = 0; i < numberOfChannels; i++)
			channels[i] = i;
		return channels;
	}

	private int[] partition(KeyValuePair<K, V> pair, int numberOfChannels) {
		return new int[] { getPartition(pair.getKey(), numberOfChannels) };
	}
	
	private int[] range_partition(KeyValuePair<K, V> pair) {
		return new int[] {Arrays.binarySearch(histo, pair.getKey())};
	}

	private int getPartition(K key, int numberOfChannels) {
		int hash = 1315423911 ^ ((1315423911 << 5) + key.hashCode() + (1315423911 >> 2));

		for (int i = 0; i < salt.length; i++) {
			hash ^= ((hash << 5) + salt[i] + (hash >> 2));
		}

		return (hash < 0) ? -hash % numberOfChannels : hash % numberOfChannels;
	}

	@Override
	public void read(DataInput in) throws IOException {
		strategy = ShipStrategy.valueOf(in.readUTF());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(strategy.name());
	}

}
