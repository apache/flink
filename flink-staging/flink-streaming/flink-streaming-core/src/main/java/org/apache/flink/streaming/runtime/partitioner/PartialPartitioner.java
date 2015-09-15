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

package org.apache.flink.streaming.runtime.partitioner;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * Partial Key Grouping maps each message to two of the n possible channels
 * (round robin maps to n channels, field grouping maps to 1 channel). Among the
 * two possible channels it forwards the key to least loaded (each source
 * maintains the list of past messages) of two channels.
 * 
 * @param <T>
 *            Type of the Tuple
 */

public class PartialPartitioner<T> extends StreamPartitioner<T> {
	private static final long serialVersionUID = 1L;
	private long[] targetChannelStats;
	KeySelector<T, ?> keySelector;
	private int[] returnArray = new int[1];
	private boolean initializedStats;
	private HashFunction[] h ;
	private int workersPerKey = 2;
	private int currentPrime = 2;

	
	public PartialPartitioner(KeySelector<T, ?> keySelector) {
		this.initializedStats = false;
		this.keySelector = keySelector;
	}

	@Override
	public StreamPartitioner<T> copy() {
		return this;
	}

	private int getNextPrime(int x) {
		int num = x+1;
		while(!isPrime(num)) {
			num++;
		}
		return num;
	}

	private boolean isPrime(int num) {
		for(int i= 2; i<num;i++) {
			if (num%i == 0) {
				return false;
			}
		}
		return true;
	}

	@Override
	public int[] selectChannels(SerializationDelegate<StreamRecord<T>> record,
			int numChannels) {
		if (!initializedStats) {
			this.targetChannelStats = new long[numChannels];
			this.initializedStats = true;
			h = new HashFunction[2];
			for (int i =0 ; i <this.workersPerKey;i++) {
				currentPrime = getNextPrime(currentPrime);
				h[i] = Hashing.murmur3_128(currentPrime);
			}
		}
		Object key;
		int firstChoice;
		int secondChoice;
		try {
			key = keySelector.getKey(record.getInstance().getValue());
			
			firstChoice = (int) (Math.abs(h[0].hashBytes(serialize(key)).asLong()) % 
				numChannels);
			secondChoice = (int) (Math.abs(h[1].hashBytes(serialize(key)).asLong()) % 
					numChannels);

		} catch (Exception e) {
			throw new RuntimeException("Could not extract key from "
					+ record.getInstance().getValue(), e);
		}
		int selected = targetChannelStats[firstChoice] > targetChannelStats[secondChoice] ? secondChoice
				: firstChoice;
		targetChannelStats[selected]++;

		returnArray[0] = selected;
		return returnArray;
	}

	public int[] selectChannels(SerializationDelegate<StreamRecord<T>> record,
			int numChannels, int numWorkersPerKey) {
		
		if (!initializedStats) {
			this.targetChannelStats = new long[numChannels];
			this.initializedStats = true;
			
			if (numWorkersPerKey < 2 || numWorkersPerKey >= numChannels) {
				numWorkersPerKey = 2;
			}
			if ( numWorkersPerKey != numChannels ) {
				h = new HashFunction[numWorkersPerKey];
				for (int i =0 ; i <numWorkersPerKey;i++) {
					currentPrime = getNextPrime(currentPrime);
					h[i] = Hashing.murmur3_128(currentPrime);
				}
			}
		}
		int [] choices ;
		
		Object key;
		try {
			key = keySelector.getKey(record.getInstance().getValue());
			int counter = 0;
			choices = new int [numWorkersPerKey];
			if (numWorkersPerKey == numChannels) {
				while (counter < numWorkersPerKey) {
					choices[counter] = counter;
					counter++;
				}
			}else {
				while (counter < numWorkersPerKey) {
					choices[counter] = (int) (Math.abs(h[counter].hashBytes(serialize(key)).asLong()) % 
							numChannels);
					counter++;
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("Could not extract key from "
					+ record.getInstance().getValue(), e);
		}
		int selected = selectMinWorker(targetChannelStats, choices);
		targetChannelStats[selected]++;

		returnArray[0] = selected;
		return returnArray;
	}
	private int selectMinWorker(long[] loadVector, int[] choice ) {
		int index = choice[0];
		for (int i = 0; i < choice.length; i++) {
			if (loadVector[choice[i]] < loadVector[index]) {
				index = choice[i];
			}
		}
		return index;
	}
	private byte[] serialize(Object obj) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(out);
		os.writeObject(obj);
		return out.toByteArray();
	}
	
	@Override
	public String toString() {
		return "PARTIAL";
	}
}
