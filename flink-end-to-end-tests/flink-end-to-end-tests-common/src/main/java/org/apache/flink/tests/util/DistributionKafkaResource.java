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
import java.nio.file.Path;
import java.util.List;

/**
 * Kafka resource to manage the distributed kafka cluster, such as setUp, start, stop clusters and so on.
 * TODO all the interfaces are marked as {@link UnsupportedOperationException} now, can implement those later if the
 * end-to-end need a distributed Kafka cluster.
 */
public class DistributionKafkaResource implements KafkaResource {

	public DistributionKafkaResource(String fileURL, String packageName, Path testDataDir) {

	}

	@Override
	public void setUp() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void start() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void createTopic(int replicationFactor, int partitions, String topics) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void sendMessage(String topic, String message) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<String> readMessage(int maxMessage, String topic, String groupId) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setNumPartitions(String topic, int num) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getNumPartitions(String topic) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getPartitionEndOffset(String topic, int partition) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void shutdown() throws IOException {
		throw new UnsupportedOperationException();
	}
}
