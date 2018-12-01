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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Partitioner that implements hash-based partitioning using
 *  * Java's `Object.hashCode`.
 *
 * @param <T>
 *            Type of the Tuple
 */
@Internal
public class HashPartitioner<T> extends StreamPartitioner<T> {
	private static final long serialVersionUID = 1L;

	private int partitions;
	private final int[] returnArray = new int[1];

	public HashPartitioner(int partitions) {
		this.partitions = partitions;
	}

	@Override
	public int[] selectChannels(SerializationDelegate<StreamRecord<T>> record, int numChannels) {
		T value = record.getInstance().getValue();
		if (value != null){
			returnArray[0] = nonNegativeMod(value.hashCode() + Integer.hashCode(numChannels), partitions);
		}
		else {
			returnArray[0] = nonNegativeMod(Integer.hashCode(numChannels), partitions);
		}
		return returnArray;
	}

	@Override
	public StreamPartitioner<T> copy() {
		return this;
	}

	@Override
	public String toString() {
		return "HASH";
	}

	private int nonNegativeMod(int x, int mod) {
		int rawMod = x % mod;
		if (rawMod < 0){
			rawMod = rawMod + mod;
		} else {
			rawMod = rawMod + 0;
		}
		return rawMod;
	}
}
