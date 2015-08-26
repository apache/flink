/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.partitioner;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * Partitioner that map each key on any two channels using power of two choices.
 *
 * @param <T>
 *            Type of the Tuple
 */
public class PartialPartitioner<T> extends StreamPartitioner<T> {
	private static final long serialVersionUID = 1L;

	private long[] targetTaskStats; // maintain past history of forwarded messages
	private HashFunction h1 = Hashing.murmur3_128(13);
	private HashFunction h2 = Hashing.murmur3_128(17);
	KeySelector<T, ?> keySelector;
	private int[] returnArray = new int[1];

	public PartialPartitioner(KeySelector<T, ?> keySelector, int numberOfOutputChannels) {
		super(PartitioningStrategy.PARTIAL);
		this.targetTaskStats = new long[numberOfOutputChannels];
		this.keySelector = keySelector;
	}

	@Override
	public int[] selectChannels(SerializationDelegate<StreamRecord<T>> record,
			int numberOfOutputChannels) {
		String str = record.getInstance().getKey(keySelector).toString(); // assume key is the first field
		int firstChoice = (int) ( Math.abs(h1.hashBytes(str.getBytes()).asLong()) % numberOfOutputChannels );
		int secondChoice = (int) ( Math.abs(h2.hashBytes(str.getBytes()).asLong()) % numberOfOutputChannels );
		int selected = targetTaskStats[firstChoice] > targetTaskStats[secondChoice] ? secondChoice : firstChoice;
		targetTaskStats[selected]++;
		
		returnArray[0] = selected;
		return returnArray;
	}
	
}
