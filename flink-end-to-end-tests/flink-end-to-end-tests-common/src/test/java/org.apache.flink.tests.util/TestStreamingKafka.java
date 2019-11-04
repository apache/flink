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

import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Test the Kafka streaming connectors.
 */
public class TestStreamingKafka {
	private static final Logger LOG = LoggerFactory.getLogger(TestStreamingKafka.class);

	private FlinkResource flinkResource;
	protected KafkaResource kafkaResource;
	protected Path jarFile;
	private Path testDataDir;

	@Before
	public void setUp() throws IOException {
		// Prepare the Kafka environment.
		this.prepareKafkaEnv();
		Preconditions.checkNotNull(kafkaResource);
		Preconditions.checkNotNull(jarFile);
		Preconditions.checkNotNull(testDataDir);

		this.flinkResource = FlinkResourceFactory.create();

		// Initialize the Kafka Distribution directory.
		if (!Files.exists(testDataDir)) {
			Files.createDirectory(testDataDir);
		}

		// Start the Flink cluster
		flinkResource.startCluster(2);

		// Prepare the Kafka binary package and configurations
		kafkaResource.setUp();

		// Start the Kafka cluster.
		kafkaResource.start();
	}

	protected void prepareKafkaEnv() {
		this.testDataDir = End2EndUtil.getTestDataDir();
		this.kafkaResource = KafkaResourceFactory.create(
			"https://mirrors.tuna.tsinghua.edu.cn/apache/kafka/2.1.1/kafka_2.11-2.1.1.tgz",
			"kafka_2.11-2.1.1.tgz",
			testDataDir
		);
		this.jarFile = End2EndUtil
			.getEnd2EndModuleDir()
			.resolve("flink-streaming-kafka-test/target/KafkaExample.jar");
	}

	@After
	public void tearDown() throws IOException {
		kafkaResource.shutdown();
		flinkResource.stopCluster();
		if (Files.exists(testDataDir)) {
			FileUtils.deleteDirectory(testDataDir.toFile());
		}
	}

	@Test
	public void test() throws IOException {
		String testInputTopic = "test-input";
		String testOutputTopic = "test-output";

		// Initialize the kafka topics
		LOG.info("Create the Kafka topics: {}, {}", testInputTopic, testOutputTopic);
		kafkaResource.createTopic(1, 1, testInputTopic);
		kafkaResource.createTopic(1, 1, testOutputTopic);

		String[] extraArgs = new String[]{
			"--input-topic", testInputTopic,
			"--output-topic", testOutputTopic,
			"--prefix=PREFIX",
			"--bootstrap.servers", "localhost:9092",
			"--zookeeper.connect", "localhost:2181",
			"--group.id", "myconsumer",
			"--auto.offset.reset", "earliest",
			"--transaction.timeout.ms", "900000",
			"--flink.partition-discovery.interval-millis", "1000"
		};

		flinkResource.createFlinkClient()
			.action(FlinkClient.Action.RUN)
			.isDettached(true)
			.extraArgs(extraArgs)
			.jarFile(this.jarFile)
			.createProcess()
			.runBlocking(Duration.ofSeconds(40));

		String[] inputMessages = new String[]{
			"elephant,5,45218",
			"squirrel,12,46213",
			"bee,3,51348",
			"squirrel,22,52444",
			"bee,10,53412",
			"elephant,9,54867"
		};
		for (String message : inputMessages) {
			kafkaResource.sendMessage(testInputTopic, message);
		}

		List<String> messages = kafkaResource.readMessage(6, testOutputTopic, "elephant_consumer");
		List<String> results = messages.stream().filter(msg -> msg.contains("elephant")).collect(Collectors.toList());
		verifyOutput(Lists.newArrayList("elephant,5,45218", "elephant,14,54867"), results);

		messages = kafkaResource.readMessage(6, testOutputTopic, "squirrel_consumer");
		results = messages.stream().filter(msg -> msg.contains("squirrel")).collect(Collectors.toList());
		verifyOutput(Lists.newArrayList("squirrel,12,46213", "squirrel,34,52444"), results);

		messages = kafkaResource.readMessage(6, testOutputTopic, "bee_consumer");
		results = messages.stream().filter(msg -> msg.contains("bee")).collect(Collectors.toList());
		verifyOutput(Lists.newArrayList("bee,3,51348", "bee,13,53412"), results);

		// Repartition the topic.
		kafkaResource.setNumPartitions(testInputTopic, 2);

		// Verify the topic partition
		Assert.assertEquals(2, kafkaResource.getNumPartitions(testInputTopic));

		// Send some more messages to Kafka
		String[] moreMessages = new String[]{
			"elephant,13,64213",
			"giraffe,9,65555",
			"bee,5,65647",
			"squirrel,18,66413"
		};
		for (String message : moreMessages) {
			kafkaResource.sendMessage(testInputTopic, message);
		}
		// verify that our assumption that the new partition actually has written messages is correct
		Assert.assertTrue(kafkaResource.getPartitionEndOffset(testInputTopic, 1) > 0);

		messages = kafkaResource.readMessage(4, testOutputTopic, "elephant_consumer");
		results = messages.stream().filter(msg -> msg.contains("elephant")).collect(Collectors.toList());
		verifyOutput(Lists.newArrayList("elephant,27,64213"), results);

		messages = kafkaResource.readMessage(4, testOutputTopic, "squirrel_consumer");
		results = messages.stream().filter(msg -> msg.contains("squirrel")).collect(Collectors.toList());
		verifyOutput(Lists.newArrayList("squirrel,52,66413"), results);

		messages = kafkaResource.readMessage(4, testOutputTopic, "bee_consumer");
		results = messages.stream().filter(msg -> msg.contains("bee")).collect(Collectors.toList());
		verifyOutput(Lists.newArrayList("bee,18,65647"), results);

		messages = kafkaResource.readMessage(10, testOutputTopic, "giraffe_consumer");
		results = messages.stream().filter(msg -> msg.contains("giraffe")).collect(Collectors.toList());
		verifyOutput(Lists.newArrayList("giraffe,9,65555"), results);
	}

	private void verifyOutput(List<String> expectedMessages, List<String> actualMessages) {
		if (expectedMessages == actualMessages) {
			return;
		}
		if (expectedMessages == null || actualMessages == null) {
			Assert.fail("Shouldn't have any nullable list");
		}
		Assert.assertEquals(expectedMessages.size(), actualMessages.size());
		for (int i = 0; i < expectedMessages.size(); i++) {
			Assert.assertEquals(expectedMessages.get(i), actualMessages.get(i));
		}
	}
}
