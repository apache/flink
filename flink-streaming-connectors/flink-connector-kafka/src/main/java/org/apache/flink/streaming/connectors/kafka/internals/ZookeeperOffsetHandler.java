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

package org.apache.flink.streaming.connectors.kafka.internals;

import kafka.common.TopicAndPartition;
import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.zookeeper.data.Stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ZookeeperOffsetHandler implements OffsetHandler {

	private static final Logger LOG = LoggerFactory.getLogger(ZookeeperOffsetHandler.class);
	
	private static final long OFFSET_NOT_SET = FlinkKafkaConsumer.OFFSET_NOT_SET;
	
	
	private final ZkClient zkClient;
	
	private final String groupId;

	
	public ZookeeperOffsetHandler(Properties props) {
		this.groupId = props.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
		
		if (this.groupId == null) {
			throw new IllegalArgumentException("Required property '"
					+ ConsumerConfig.GROUP_ID_CONFIG + "' has not been set");
		}
		
		String zkConnect = props.getProperty("zookeeper.connect");
		if (zkConnect == null) {
			throw new IllegalArgumentException("Required property 'zookeeper.connect' has not been set");
		}
		
		zkClient = new ZkClient(zkConnect,
				Integer.valueOf(props.getProperty("zookeeper.session.timeout.ms", "6000")),
				Integer.valueOf(props.getProperty("zookeeper.connection.timeout.ms", "6000")),
				new ZooKeeperStringSerializer());
	}


	@Override
	public void commit(Map<KafkaTopicPartition, Long> offsetsToCommit) {
		for (Map.Entry<KafkaTopicPartition, Long> entry : offsetsToCommit.entrySet()) {
			KafkaTopicPartition tp = entry.getKey();
			long offset = entry.getValue();
			
			if (offset >= 0) {
				setOffsetInZooKeeper(zkClient, groupId, tp.getTopic(), tp.getPartition(), offset);
			}
		}
	}

	@Override
	public void seekFetcherToInitialOffsets(List<KafkaTopicPartitionLeader> partitions, Fetcher fetcher) {
		for (KafkaTopicPartitionLeader tp : partitions) {
			long offset = getOffsetFromZooKeeper(zkClient, groupId, tp.getTopicPartition().getTopic(), tp.getTopicPartition().getPartition());

			if (offset != OFFSET_NOT_SET) {
				LOG.info("Offset for partition {} was set to {} in ZooKeeper. Seeking fetcher to that position.",
						tp.getTopicPartition().getPartition(), offset);

				// the offset in Zookeeper was the last read offset, seek is accepting the next-to-read-offset.
				fetcher.seek(tp.getTopicPartition(), offset + 1);
			}
		}
	}

	@Override
	public void close() throws IOException {
		zkClient.close();
	}

	// ------------------------------------------------------------------------
	//  Communication with Zookeeper
	// ------------------------------------------------------------------------
	
	public static void setOffsetInZooKeeper(ZkClient zkClient, String groupId, String topic, int partition, long offset) {
		TopicAndPartition tap = new TopicAndPartition(topic, partition);
		ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(groupId, tap.topic());
		ZkUtils.updatePersistentPath(zkClient, topicDirs.consumerOffsetDir() + "/" + tap.partition(), Long.toString(offset));
	}

	public static long getOffsetFromZooKeeper(ZkClient zkClient, String groupId, String topic, int partition) {
		TopicAndPartition tap = new TopicAndPartition(topic, partition);
		ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(groupId, tap.topic());

		scala.Tuple2<Option<String>, Stat> data = ZkUtils.readDataMaybeNull(zkClient,
				topicDirs.consumerOffsetDir() + "/" + tap.partition());

		if (data._1().isEmpty()) {
			return OFFSET_NOT_SET;
		} else {
			return Long.valueOf(data._1().get());
		}
	}
}
