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

package org.apache.flink.tests.util;

import java.io.IOException;
import java.util.List;

/**
 * Kafka resource to manage the kafka cluster, such as setUp, start, stop clusters and so on. Notice that the Kafka
 * resource can be a standalone cluster ({@link LocalStandaloneKafkaResource}) or distributed cluster ({@link DistributionKafkaResource})
 */
public interface KafkaResource {

	/**
	 * Prepare the kafka distribution package installation and configuration files setting.
	 *
	 * @throws IOException if any IO error happen
	 */
	void setUp() throws IOException;

	/**
	 * Start the Kafka cluster services.
	 *
	 * @throws IOException if any IO error happen
	 */
	void start() throws IOException;

	/**
	 * Create a Kafka topic with the given arguments.
	 *
	 * @param replicationFactor for the newly created topic
	 * @param partitions        for the newly created topic.
	 * @param topics            name of the topic.
	 * @throws IOException if any IO error happen
	 */
	void createTopic(int replicationFactor, int partitions, String topics) throws IOException;

	/**
	 * Send a message to the given Kafka topic.
	 *
	 * @param topic   name of the topic
	 * @param message content of the message to send.
	 * @throws IOException if any IO error happen
	 */
	void sendMessage(String topic, String message) throws IOException;

	/**
	 * Read multiple messages from the given topic.
	 *
	 * @param maxMessage max message count to read in the given topic
	 * @param topic      name of topic
	 * @param groupId    group name of the consumer.
	 * @return the message list that we've read.
	 * @throws IOException if any IO error happen
	 */
	List<String> readMessage(int maxMessage, String topic, String groupId) throws IOException;

	/**
	 * Set the partition number of given topic.
	 *
	 * @param topic name of the topic
	 * @param num   partition number.
	 * @throws IOException if any IO error happen
	 */
	void setNumPartitions(String topic, int num) throws IOException;

	/**
	 * Get the partition number of given topic.
	 *
	 * @param topic name of the topic.
	 * @return partition number.
	 * @throws IOException if any IO error happen
	 */
	int getNumPartitions(String topic) throws IOException;

	/**
	 * Get the end offset of the partition for given topic.
	 *
	 * @param topic     name of the topic.
	 * @param partition partition number
	 * @return the offset value.
	 * @throws IOException if any IO error happen.
	 */
	int getPartitionEndOffset(String topic, int partition) throws IOException;

	/**
	 * Shutdown the kafka cluster.
	 *
	 * @throws IOException if any IO error happen.
	 */
	void shutdown() throws IOException;
}
