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

package org.apache.flink.streaming.connectors.kafka.internals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;


/**
 * A serializable representation of a kafka topic and a partition.
 * Used as an operator state for the Kafka consumer
 */
public class KafkaTopicPartition implements Serializable {

	private static final long serialVersionUID = 722083576322742325L;

	private final String topic;
	private final int partition;
	private final int cachedHash;

	public KafkaTopicPartition(String topic, int partition) {
		this.topic = requireNonNull(topic);
		this.partition = partition;
		this.cachedHash = 31 * topic.hashCode() + partition;
	}

	public String getTopic() {
		return topic;
	}

	public int getPartition() {
		return partition;
	}

	@Override
	public String toString() {
		return "KafkaTopicPartition{" +
				"topic='" + topic + '\'' +
				", partition=" + partition +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof KafkaTopicPartition)) {
			return false;
		}

		KafkaTopicPartition that = (KafkaTopicPartition) o;

		if (partition != that.partition) {
			return false;
		}
		return topic.equals(that.topic);
	}

	@Override
	public int hashCode() {
		return cachedHash;
	}


	// ------------------- Utilities -------------------------------------

	public static String toString(Map<KafkaTopicPartition, Long> map) {
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<KafkaTopicPartition, Long> p: map.entrySet()) {
			KafkaTopicPartition ktp = p.getKey();
			sb.append(ktp.getTopic()).append(":").append(ktp.getPartition()).append("=").append(p.getValue()).append(", ");
		}
		return sb.toString();
	}

	public static String toString(List<KafkaTopicPartition> partitions) {
		StringBuilder sb = new StringBuilder();
		for (KafkaTopicPartition p: partitions) {
			sb.append(p.getTopic()).append(":").append(p.getPartition()).append(", ");
		}
		return sb.toString();
	}


	public static List<KafkaTopicPartition> dropLeaderData(List<KafkaTopicPartitionLeader> partitionInfos) {
		List<KafkaTopicPartition> ret = new ArrayList<>(partitionInfos.size());
		for(KafkaTopicPartitionLeader ktpl: partitionInfos) {
			ret.add(ktpl.getTopicPartition());
		}
		return ret;
	}
}
