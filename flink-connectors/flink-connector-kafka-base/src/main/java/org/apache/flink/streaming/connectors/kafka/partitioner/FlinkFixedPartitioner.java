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

/**
 * A partitioner ensuring that each internal Flink partition ends up in one Kafka partition.
 *
 * <p>Note, one Kafka partition can contain multiple Flink partitions.
 *
 * <p>Cases:
 * 	# More Flink partitions than kafka partitions
 * <pre>
 * 		Flink Sinks:		Kafka Partitions
 * 			1	----------------&gt;	1
 * 			2   --------------/
 * 			3   -------------/
 * 			4	------------/
 * </pre>
 * Some (or all) kafka partitions contain the output of more than one flink partition
 *
 * <p>Fewer Flink partitions than Kafka
 * <pre>
 * 		Flink Sinks:		Kafka Partitions
 * 			1	----------------&gt;	1
 * 			2	----------------&gt;	2
 * 										3
 * 										4
 * 										5
 * </pre>
 *
 * <p>Not all Kafka partitions contain data
 * To avoid such an unbalanced partitioning, use a round-robin kafka partitioner (note that this will
 * cause a lot of network connections between all the Flink instances and all the Kafka brokers).
 */
@PublicEvolving
public class FlinkFixedPartitioner<T> extends FlinkKafkaPartitioner<T> {

	private static final long serialVersionUID = -3785320239953858777L;

	private int parallelInstanceId;

	@Override
	public void open(int parallelInstanceId, int parallelInstances) {
		Preconditions.checkArgument(parallelInstanceId >= 0, "Id of this subtask cannot be negative.");
		Preconditions.checkArgument(parallelInstances > 0, "Number of subtasks must be larger than 0.");

		this.parallelInstanceId = parallelInstanceId;
	}

	@Override
	public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
		Preconditions.checkArgument(
			partitions != null && partitions.length > 0,
			"Partitions of the target topic is empty.");

		return partitions[parallelInstanceId % partitions.length];
	}
}
