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

package org.apache.flink.streaming.connectors.kafka.api.simple;

import java.io.UnsupportedEncodingException;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import kafka.admin.AdminUtils;

/**
 * Factory for creating custom Kafka partitions.
 */
public class KafkaTopicFactory {

	public static void createTopic(String zookeeperServer, String topicName, int numOfPartitions, int replicationFactor) {
		createTopic(zookeeperServer, topicName, numOfPartitions, replicationFactor, new Properties(), 10000, 10000);
	}

	public static void createTopic(String zookeeperServer, String topicName, int numOfPartitions, int replicationFactor, Properties topicProperties, int sessionTimeoutMs, int connectionTimeoutMs) {
		ZkClient zkClient = new ZkClient(zookeeperServer, sessionTimeoutMs, connectionTimeoutMs,
				new KafkaZKStringSerializer());

		Properties topicConfig = new Properties();
		AdminUtils.createTopic(zkClient, topicName, numOfPartitions, replicationFactor, topicConfig);
	}

	private static class KafkaZKStringSerializer implements ZkSerializer {

		@Override
		public byte[] serialize(Object data) throws ZkMarshallingError {
			try {
				return ((String) data).getBytes("UTF-8");
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public Object deserialize(byte[] bytes) throws ZkMarshallingError {
			if (bytes == null) {
				return null;
			} else {
				try {
					return new String(bytes, "UTF-8");
				} catch (UnsupportedEncodingException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}
}
