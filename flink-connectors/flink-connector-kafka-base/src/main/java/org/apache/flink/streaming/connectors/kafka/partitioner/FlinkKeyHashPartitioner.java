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

package org.apache.flink.streaming.connectors.kafka.partitioner;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Arrays;

/**
 * A partitioner that uses the hash of the provided key to distribute
 * the values over the partitions as evenly as possible.
 * This partitioner ensures that all records with the same key will be sent to
 * the same Kafka partition.
 *
 * <p>Note that this will cause a lot of network connections to be created between
 * all the Flink instances and all the Kafka brokers.
 */
@PublicEvolving
public class FlinkKeyHashPartitioner<T> extends FlinkKafkaPartitioner<T> {

	private static final long serialVersionUID = -2006468063065010594L;

	@Override
	public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
		Preconditions.checkArgument(
			partitions != null && partitions.length > 0,
			"Partitions of the target topic is empty.");

		return partitions[Math.abs(hash(key)) % partitions.length];
	}

	/**
	 * The overridable implementation of the hashing algorithm.
	 * @param key The key of the provided record on which the partition selection is based. (key can be null!)
	 * @return The hash value for the provided key.
	 */
	protected int hash(@Nullable byte[] key) {
		return Arrays.hashCode(key);
	}

}
