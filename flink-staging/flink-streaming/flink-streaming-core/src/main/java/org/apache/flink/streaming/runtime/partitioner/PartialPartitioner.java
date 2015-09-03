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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 *Partial Key Grouping maps each message to two of the n possible channels
 *(round robin maps to n channels, field grouping maps to 1 channel). 
 *Among the two possible channels it forwards the key to least loaded 
 *(each source maintains the list of past messages) of two channels.
 * 
 * @param <T>
 *            Type of the Tuple
 */

public class PartialPartitioner<T> extends StreamPartitioner<T> {
	private static final long serialVersionUID = 1L;

	private long[] targetChannelStats; 
	private HashFunction h1 = Hashing.murmur3_128(13);
	private HashFunction h2 = Hashing.murmur3_128(17);
	KeySelector<T, ?> keySelector;
	private int[] returnArray = new int[1];
	private boolean initializedStats;

	public PartialPartitioner(KeySelector<T, ?> keySelector) {
		this.initializedStats = false;
		this.keySelector = keySelector;
	}	
	@Override
	public StreamPartitioner<T> copy() {
		return this;
	}

	@Override
	public String toString() {
		return "HASH";
	}

	@Override
	public int[] selectChannels(SerializationDelegate<StreamRecord<T>> record,
			int numChannels) {
		// TODO Auto-generated method stub
		if(!initializedStats) {
			this.targetChannelStats = new long[numChannels];
			this.initializedStats = true;
		}
		Object key;
		try {
			key = keySelector.getKey(record.getInstance().getValue());
		} catch (Exception e) {
			throw new RuntimeException("Could not extract key from " + record.getInstance().getValue(), e);
		}
		
		int firstChoice = Math.abs(key.hashCode()) 
				% numChannels;
		int secondChoice = ( firstChoice + 1 ) 
				% numChannels; 
				
		int selected = targetChannelStats[firstChoice] > targetChannelStats[secondChoice] ? secondChoice : firstChoice;
		targetChannelStats[selected]++;
		
		returnArray[0] = selected;
		return returnArray;
	}
	
	public int[] selectChannels(SerializationDelegate<StreamRecord<T>> record,
			int numChannels, int numWorkersPerKey) {
		// TODO Auto-generated method stub
		if(!initializedStats) {
			this.targetChannelStats = new long[numChannels];
			this.initializedStats = true;
		}
		Object key;
		try {
			key = keySelector.getKey(record.getInstance().getValue());
		} catch (Exception e) {
			throw new RuntimeException("Could not extract key from " + record.getInstance().getValue(), e);
		}
		if(numWorkersPerKey < 2 ) {
			numWorkersPerKey = 2;
		}
		int choices[]  = new int[numWorkersPerKey];
		int counter = 0;
		choices[counter] = Math.abs(key.hashCode()) 
				% numChannels;
		counter++;
		
		while (counter <= numWorkersPerKey) {
			choices[counter] = (choices[counter-1] + 1 ) 
				% numChannels; 
			counter++;		
		}
				
		int selected = selectMinWorker(targetChannelStats,choices);
		targetChannelStats[selected]++;
		
		returnArray[0] = selected;
		return returnArray;
	}
	
	private int selectMinWorker(long loadVector[], int choice[]) {
		int index = choice[0];
		for(int i = 0; i< choice.length; i++) {
			if (loadVector[choice[i]]<loadVector[index])
				index = choice[i];
		}
		return index;
	}
}
