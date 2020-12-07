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

package org.apache.flink.tests.util.kafka;

import org.apache.flink.tests.util.util.FactoryUtils;
import org.apache.flink.util.ExternalResource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;

/**
 * Generic interface for interacting with Kafka.
 */
public interface KafkaResource extends ExternalResource {

	/**
	 * Creates a topic with the given name, replication factor and number of partitions.
	 *
	 * @param replicationFactor replication factor
	 * @param numPartitions number of partitions
	 * @param topic desired topic name
	 * @throws IOException
	 */
	void createTopic(int replicationFactor, int numPartitions, String topic) throws IOException;

	/**
	 * Sends the given messages to the given topic.
	 *
	 * @param topic topic name
	 * @param messages messages to send
	 * @throws IOException
	 */
	void sendMessages(String topic, String ... messages) throws IOException;

	/**
	 * Sends the given keyed messages to the given topic. The messages themselves should contain
	 * the specified {@code keySeparator}.
	 *
	 * @param topic topic name
	 * @param keySeparator the separator used to parse key from value in the messages
	 * @param messages messages to send
	 * @throws IOException
	 */
	void sendKeyedMessages(
			String topic, String keySeparator, String ... messages) throws IOException;

	/**
	 * Returns the kafka bootstrap server addresses.
	 * @return kafka bootstrap server addresses
	 */
	Collection<InetSocketAddress> getBootstrapServerAddresses();

	/**
	 * Returns the address of Zookeeper.
	 * @return zookeeper address
	 */
	InetSocketAddress getZookeeperAddress();

	/**
	 * Reads {@code expectedNumMessages} from the given topic. If we can't read the expected number
	 * of messages we throw an exception.
	 *
	 * @param expectedNumMessages expected number of messages that should be read
	 * @param groupId group id to identify consumer
	 * @param topic topic name
	 * @return read messages
	 * @throws IOException
	 */
	List<String> readMessage(int expectedNumMessages, String groupId, String topic) throws IOException;

	/**
	 * Modifies the number of partitions for the given topic.
	 * @param numPartitions desired number of partitions
	 * @param topic topic to modify
	 * @throws IOException
	 */
	void setNumPartitions(int numPartitions, String topic) throws IOException;

	/**
	 * Returns the current number of partitions for the given topic.
	 * @param topic topic name
	 * @return number of partitions for the given topic
	 * @throws IOException
	 */
	int getNumPartitions(String topic) throws IOException;

	/**
	 * Returns the current partition offset for the given partition of the given topic.
	 * @param topic topic name
	 * @param partition topic partition
	 * @return partition offset for the given partition
	 * @throws IOException
	 */
	long getPartitionOffset(String topic, int partition) throws IOException;

	/**
	 * Returns the configured KafkaResource implementation, or a {@link LocalStandaloneKafkaResource} if none is configured.
	 *
	 * @return configured KafkaResource, or {@link LocalStandaloneKafkaResource} if none is configured
	 */
	static KafkaResource get(final String version) {
		return FactoryUtils.loadAndInvokeFactory(
			KafkaResourceFactory.class,
			factory -> factory.create(version),
			LocalStandaloneKafkaResourceFactory::new);
	}
}
