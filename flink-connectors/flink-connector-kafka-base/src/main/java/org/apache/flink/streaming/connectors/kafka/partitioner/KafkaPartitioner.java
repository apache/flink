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

package org.apache.flink.streaming.connectors.kafka.partitioner;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

/**
 * It contains a open() method which is called on each parallel instance.
 * Partitioners must be serializable!
 *
 * @deprecated This partitioner does not handle partitioning properly in the case of
 *             multiple topics, and has been deprecated. Please use {@link FlinkKafkaPartitioner} instead.
 */
@Deprecated
@Internal
public abstract class KafkaPartitioner<T> implements Serializable {

	private static final long serialVersionUID = -1974260817778593473L;

	/**
	 * Initializer for the Partitioner.
	 * @param parallelInstanceId 0-indexed id of the parallel instance in Flink
	 * @param parallelInstances the total number of parallel instances
	 * @param partitions an array describing the partition IDs of the available Kafka partitions.
	 */
	public void open(int parallelInstanceId, int parallelInstances, int[] partitions) {
		// overwrite this method if needed.
	}

	public abstract int partition(T next, byte[] serializedKey, byte[] serializedValue, int numPartitions);
}
