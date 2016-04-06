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

import kafka.utils.ZKGroupTopicDirs;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Handler for committing Kafka offsets to Zookeeper and to retrieve them again.
 */
public class ZookeeperOffsetHandler {

	private static final Logger LOG = LoggerFactory.getLogger(ZookeeperOffsetHandler.class);

	private final String groupId;

	private final CuratorFramework curatorClient;


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

		// we use Curator's default timeouts
		int sessionTimeoutMs =  Integer.valueOf(props.getProperty("zookeeper.session.timeout.ms", "60000"));
		int connectionTimeoutMs = Integer.valueOf(props.getProperty("zookeeper.connection.timeout.ms", "15000"));
		
		// undocumented config options allowing users to configure the retry policy. (they are "flink." prefixed as they are no official kafka configs)
		int backoffBaseSleepTime = Integer.valueOf(props.getProperty("flink.zookeeper.base-sleep-time.ms", "100"));
		int backoffMaxRetries =  Integer.valueOf(props.getProperty("flink.zookeeper.max-retries", "10"));
		
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(backoffBaseSleepTime, backoffMaxRetries);
		curatorClient = CuratorFrameworkFactory.newClient(zkConnect, sessionTimeoutMs, connectionTimeoutMs, retryPolicy);
		curatorClient.start();
	}
	
	// ------------------------------------------------------------------------
	//  Offset access and manipulation
	// ------------------------------------------------------------------------

	/**
	 * Writes given set of offsets for Kafka partitions to ZooKeeper.
	 * 
	 * @param offsetsToWrite The offsets for the partitions to write.
	 * @throws Exception The method forwards exceptions.
	 */
	public void writeOffsets(Map<KafkaTopicPartition, Long> offsetsToWrite) throws Exception {
		for (Map.Entry<KafkaTopicPartition, Long> entry : offsetsToWrite.entrySet()) {
			KafkaTopicPartition tp = entry.getKey();
			long offset = entry.getValue();

			if (offset >= 0) {
				setOffsetInZooKeeper(curatorClient, groupId, tp.getTopic(), tp.getPartition(), offset);
			}
		}
	}

	/**
	 * 
	 * @param partitions The partitions to read offsets for.
	 * @return The mapping from partition to offset.
	 * @throws Exception This method forwards exceptions.
	 */
	public Map<KafkaTopicPartition, Long> getOffsets(List<KafkaTopicPartition> partitions) throws Exception {
		Map<KafkaTopicPartition, Long> ret = new HashMap<>(partitions.size());
		for (KafkaTopicPartition tp : partitions) {
			Long offset = getOffsetFromZooKeeper(curatorClient, groupId, tp.getTopic(), tp.getPartition());

			if (offset != null) {
				LOG.info("Offset for TopicPartition {}:{} was set to {} in ZooKeeper. Seeking fetcher to that position.",
						tp.getTopic(), tp.getPartition(), offset);
				ret.put(tp, offset);
			}
		}
		return ret;
	}

	/**
	 * Closes the offset handler.
	 * 
	 * @throws IOException Thrown, if the handler cannot be closed properly.
	 */
	public void close() throws IOException {
		curatorClient.close();
	}

	// ------------------------------------------------------------------------
	//  Communication with Zookeeper
	// ------------------------------------------------------------------------
	
	public static void setOffsetInZooKeeper(CuratorFramework curatorClient, String groupId, String topic, int partition, long offset) throws Exception {
		ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(groupId, topic);
		String path = topicDirs.consumerOffsetDir() + "/" + partition;
		curatorClient.newNamespaceAwareEnsurePath(path).ensure(curatorClient.getZookeeperClient());
		byte[] data = Long.toString(offset).getBytes();
		curatorClient.setData().forPath(path, data);
	}

	public static Long getOffsetFromZooKeeper(CuratorFramework curatorClient, String groupId, String topic, int partition) throws Exception {
		ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(groupId, topic);
		String path = topicDirs.consumerOffsetDir() + "/" + partition;
		curatorClient.newNamespaceAwareEnsurePath(path).ensure(curatorClient.getZookeeperClient());
		
		byte[] data = curatorClient.getData().forPath(path);
		
		if (data == null) {
			return null;
		} else {
			String asString = new String(data);
			if (asString.length() == 0) {
				return null;
			} else {
				try {
					return Long.valueOf(asString);
				}
				catch (NumberFormatException e) {
					LOG.error(
							"The offset in ZooKeeper for group '{}', topic '{}', partition {} is a malformed string: {}",
						groupId, topic, partition, asString);
					return null;
				}
			}
		}
	}
}
