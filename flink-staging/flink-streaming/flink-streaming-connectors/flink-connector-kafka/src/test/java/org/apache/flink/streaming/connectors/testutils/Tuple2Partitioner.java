/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.testutils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.SerializableKafkaPartitioner;

/**
 * Special partitioner that uses the first field of a 2-tuple as the partition,
 * and that expects a specific number of partitions.
 */
public class Tuple2Partitioner implements SerializableKafkaPartitioner {
	
	private static final long serialVersionUID = 1L;

	private final int expectedPartitions;

	
	public Tuple2Partitioner(int expectedPartitions) {
		this.expectedPartitions = expectedPartitions;
	}

	@Override
	public int partition(Object key, int numPartitions) {
		if (numPartitions != expectedPartitions) {
			throw new IllegalArgumentException("Expected " + expectedPartitions + " partitions");
		}
		@SuppressWarnings("unchecked")
		Tuple2<Integer, Integer> element = (Tuple2<Integer, Integer>) key;
		
		return element.f0;
	}
}