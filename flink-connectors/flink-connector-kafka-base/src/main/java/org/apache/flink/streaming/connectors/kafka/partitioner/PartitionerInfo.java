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

import org.apache.flink.util.Preconditions;

import java.io.Serializable;

public class PartitionerInfo<IN> implements Serializable {
	private final String topic;
	private final KafkaPartitioner<IN> partitioner;
	private int[] partitions;
	private int partitionNum;

	public PartitionerInfo(String topic, KafkaPartitioner<IN> partitionner) {
		this.topic = topic;
		this.partitioner = partitionner;
		this.partitions = null;
	}

	public void setPartitions(int[] partitions) {
		Preconditions.checkArgument(null == this.partitions && null != partitions && partitions.length > 0);
		this.partitions = partitions;
		this.partitionNum = partitions.length;
	}

	public KafkaPartitioner<IN> getPartitioner() {
		return this.partitioner;
	}

	public int[] getPartitions() {
		return this.partitions;
	}

	public int getPartitionNum() {
		return this.partitionNum;
	}

	public String getTopic() {
		return this.topic;
	}
}
