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

/**
 * A partitioner ensuring that each internal Flink partition ends up in one Kafka partition.
 *
 * Note, one Kafka partition can contain multiple Flink partitions.
 *
 * Cases:
 * 	# More Flink partitions than kafka partitions
 *
 * 		Flink Sinks:		Kafka Partitions
 * 			1	---------------->	1
 * 			2   --------------/
 * 			3   -------------/
 * 			4	------------/
 *
 * 	--> Some (or all) kafka partitions contain the output of more than one flink partition
 *
 *# Fewer Flink partitions than Kafka
 *
 * 		Flink Sinks:		Kafka Partitions
 * 			1	---------------->	1
 * 			2	---------------->	2
 * 									3
 * 									4
 * 									5
 *
 *  --> Not all Kafka partitions contain data
 *  To avoid such an unbalanced partitioning, use a round-robin kafka partitioner. (note that this will
 *  cause a lot of network connections between all the Flink instances and all the Kafka brokers
 *
 *
 */
public class FixedPartitioner extends RichKafkaPartitioner {
	private static final long serialVersionUID = 1627268846962918126L;

	int targetPartition = -1;

	@Override
	public void prepare(int parallelInstanceId, int parallelInstances, int[] partitions) {
		int p = 0;
		for(int i = 0; i < parallelInstances; i++) {
			if(i == parallelInstanceId) {
				targetPartition = partitions[p];
				return;
			}
			if(++p == partitions.length) {
				p = 0;
			}
		}
	}

	@Override
	public int partition(Object element, int numPartitions) {
		if(targetPartition == -1) {
			throw new RuntimeException("The partitioner has not been initialized properly");
		}
		return targetPartition;
	}
}
