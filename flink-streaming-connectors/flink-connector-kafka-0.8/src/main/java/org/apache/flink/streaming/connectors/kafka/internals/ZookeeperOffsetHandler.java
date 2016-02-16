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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
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
public class ZookeeperOffsetHandler implements OffsetHandler {

	private static final Logger LOG = LoggerFactory.getLogger(ZookeeperOffsetHandler.class);
	
	private static final long OFFSET_NOT_SET = FlinkKafkaConsumer08.OFFSET_NOT_SET;

	public static final String IGNORE_ZK_ERRORS_KEY = "flink.zookeeper-offset-handler.ignore-errors";
	public static final String IGNORE_ZK_ERRORS_DEFAULT = "true";

	/**
	 * Controls whether soon as one offset cannot be fetched, all follow the auto-reset behavior
	 * If set to "false", those offsets set in ZK will be used, the rest is determined from Kafka
	 */
	public static final String FALLBACK_ALL_KEY = "flink.zookeeper-offset-handler.fallback-all";
	public static final String FALLBACK_ALL_DEFAULT = "true";

	private final String groupId;

	private final CuratorFramework curatorClient;
	private final String zkConnect;
	private final boolean ignoreErrors;
	private final boolean fallbackAll;


	public ZookeeperOffsetHandler(Properties props) {
		this.groupId = props.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
		if (this.groupId == null) {
			throw new IllegalArgumentException("Required property '"
					+ ConsumerConfig.GROUP_ID_CONFIG + "' has not been set");
		}
		
		zkConnect = props.getProperty("zookeeper.connect");
		if (zkConnect == null) {
			throw new IllegalArgumentException("Required property 'zookeeper.connect' has not been set");
		}

		this.ignoreErrors = Boolean.valueOf(props.getProperty(IGNORE_ZK_ERRORS_KEY, IGNORE_ZK_ERRORS_DEFAULT));

		this.fallbackAll = Boolean.valueOf(props.getProperty(FALLBACK_ALL_KEY, FALLBACK_ALL_DEFAULT));

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


	@Override
	public void commit(Map<KafkaTopicPartition, Long> offsetsToCommit) throws Exception {
		try {
			for (Map.Entry<KafkaTopicPartition, Long> entry : offsetsToCommit.entrySet()) {
				KafkaTopicPartition tp = entry.getKey();
				long offset = entry.getValue();

				if (offset >= 0) {
					setOffsetInZooKeeper(curatorClient, groupId, tp.getTopic(), tp.getPartition(), offset);
				}
			}
		} catch(Exception e) {
			if(ignoreErrors) {
				LOG.warn("Error while committing offsets to Zookeeper with connection string: " + zkConnect, e);
			} else {
				throw new RuntimeException("Error while committing initial offsets to Zookeeper", e);
			}
		}
	}

	@Override
	public void seekFetcherToInitialOffsets(List<KafkaTopicPartitionLeader> partitions, Fetcher fetcher) throws Exception {
		// we put all seek offsets into this map to either seek for all partitions or for none (in an error case)
		Map<KafkaTopicPartition, Long> seekOffsets = new HashMap<>();
		try {
			for (KafkaTopicPartitionLeader tp : partitions) {
				long offset = getOffsetFromZooKeeper(curatorClient, groupId, tp.getTopicPartition().getTopic(), tp.getTopicPartition().getPartition());

				if (offset != OFFSET_NOT_SET) {
					// the offset in Zookeeper was the last read offset, seek is accepting the next-to-read-offset.
					seekOffsets.put(tp.getTopicPartition(), offset + 1);
				}
			}
		} catch(Exception e) {
			if(ignoreErrors) {
				LOG.warn("Error while retrieving initial offsets from Zookeeper with connection string: " + zkConnect, e);
				if(fallbackAll) {
					seekOffsets = null;
				}
			} else {
				throw new RuntimeException("Error while getting initial offsets from Zookeeper", e);
			}
		}

		if (seekOffsets != null) {
			// seek
			for (Map.Entry<KafkaTopicPartition, Long> seekOffset : seekOffsets.entrySet()) {
				KafkaTopicPartition partition = seekOffset.getKey();
				LOG.info("Offset for partition {} was set to {} in ZooKeeper. Seeking fetcher to that position.",
						partition, seekOffset.getValue());
				fetcher.seek(partition, seekOffset.getValue());
			}
		}
	}

	@Override
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

	public static long getOffsetFromZooKeeper(CuratorFramework curatorClient, String groupId, String topic, int partition) throws Exception {
		ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(groupId, topic);
		String path = topicDirs.consumerOffsetDir() + "/" + partition;
		curatorClient.newNamespaceAwareEnsurePath(path).ensure(curatorClient.getZookeeperClient());
		
		byte[] data = curatorClient.getData().forPath(path);
		
		if (data == null) {
			return OFFSET_NOT_SET;
		} else {
			String asString = new String(data);
			if (asString.length() == 0) {
				return OFFSET_NOT_SET;
			} else {
				try {
					return Long.parseLong(asString);
				} catch (NumberFormatException e) {
					throw new Exception(String.format(
						"The offset in ZooKeeper for group '%s', topic '%s', partition %d is a malformed string: %s",
						groupId, topic, partition, asString));
				}
			}
		}
	}
}
