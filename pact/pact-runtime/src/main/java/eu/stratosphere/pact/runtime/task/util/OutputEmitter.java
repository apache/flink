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
 * @author Erik Nijkamp
 * @author Alexander Alexandrov
 * @param <K>
 * @param <V>
 */
public class OutputEmitter<K extends Key, V extends Value> implements ChannelSelector<KeyValuePair<K, V>> {
	private ShipStrategy strategy;

	private byte[] salt;

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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void selectChannels(KeyValuePair<K, V> record, boolean[] channelFlags) {
		
		switch(strategy) {
		case BROADCAST:
			broadcast(channelFlags);
			break;
		case PARTITION_HASH:
			partition(record, channelFlags);
			break;
		default:
			robin(channelFlags);
			break;
		}
		
	}
	
	private void robin(boolean[] channelFlags) {
		
		nextChannelToSendTo = (nextChannelToSendTo + 1) % channelFlags.length;
		channelFlags[nextChannelToSendTo] = true;
	}

	private void broadcast(boolean[] channelFlags) {
		
		final int len = channelFlags.length;
		for(int i = 0; i < len; ++i) {
			channelFlags[i] = true;
		}
	}

	private void partition(KeyValuePair<K, V> pair, boolean[] channelFlags) {
		
		final int partition = getPartition(pair.getKey(), channelFlags.length);
		channelFlags[partition] = true;
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
