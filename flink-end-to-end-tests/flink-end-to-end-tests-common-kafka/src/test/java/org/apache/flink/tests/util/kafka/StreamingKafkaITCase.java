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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.categories.PreCommit;
import org.apache.flink.tests.util.categories.TravisGroup1;
import org.apache.flink.tests.util.flink.ClusterController;
import org.apache.flink.tests.util.flink.FlinkResource;
import org.apache.flink.tests.util.flink.FlinkResourceSetup;
import org.apache.flink.tests.util.flink.JobSubmission;
import org.apache.flink.testutils.junit.FailsOnJava11;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * End-to-end test for the kafka connectors.
 */
@RunWith(Parameterized.class)
@Category(value = {TravisGroup1.class, PreCommit.class, FailsOnJava11.class})
public class StreamingKafkaITCase extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(StreamingKafkaITCase.class);

	@Parameterized.Parameters(name = "{index}: kafka-version:{1}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
			{"flink-streaming-kafka010-test.*", "0.10.2.0"},
			{"flink-streaming-kafka011-test.*", "0.11.0.2"},
			{"flink-streaming-kafka-test.*", "2.2.0"}
		});
	}

	private final Path kafkaExampleJar;

	@Rule
	public final KafkaResource kafka;

	@Rule
	public final FlinkResource flink = FlinkResource.get(FlinkResourceSetup.builder().addConfiguration(getConfiguration()).build());

	private static Configuration getConfiguration() {
		// modify configuration to have enough slots
		final Configuration flinkConfig = new Configuration();
		flinkConfig.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 3);
		return flinkConfig;
	}

	public StreamingKafkaITCase(final String kafkaExampleJarPattern, final String kafkaVersion) {
		this.kafkaExampleJar = TestUtils.getResourceJar(kafkaExampleJarPattern);
		this.kafka = KafkaResource.get(kafkaVersion);
	}

	@Test
	public void testKafka() throws Exception {
		try (final ClusterController clusterController = flink.startCluster(1)) {

			final String inputTopic = "test-input";
			final String outputTopic = "test-output";

			// create the required topics
			kafka.createTopic(1, 1, inputTopic);
			kafka.createTopic(1, 1, outputTopic);

			// run the Flink job (detached mode)
			clusterController.submitJob(new JobSubmission.JobSubmissionBuilder(kafkaExampleJar)
				.setDetached(true)
				.addArgument("--input-topic", inputTopic)
				.addArgument("--output-topic", outputTopic)
				.addArgument("--prefix", "PREFIX")
				.addArgument("--bootstrap.servers", kafka.getBootstrapServerAddresses().stream().map(address -> address.getHostString() + ':' + address.getPort()).collect(Collectors.joining(",")))
				.addArgument("--zookeeper.connect ", kafka.getZookeeperAddress().getHostString() + ':' + kafka.getZookeeperAddress().getPort())
				.addArgument("--group.id", "myconsumer")
				.addArgument("--auto.offset.reset", "earliest")
				.addArgument("--transaction.timeout.ms", "900000")
				.addArgument("--flink.partition-discovery.interval-millis", "1000")
				.build());

			LOG.info("Sending messages to Kafka topic [{}] ...", inputTopic);
			// send some data to Kafka
			kafka.sendMessages(inputTopic,
				"elephant,5,45218",
				"squirrel,12,46213",
				"bee,3,51348",
				"squirrel,22,52444",
				"bee,10,53412",
				"elephant,9,54867");

			LOG.info("Verifying messages from Kafka topic [{}] ...", outputTopic);
			{
				final List<String> messages = kafka.readMessage(6, "kafka-e2e-driver", outputTopic);

				final List<String> elephants = filterMessages(messages, "elephant");
				final List<String> squirrels = filterMessages(messages, "squirrel");
				final List<String> bees = filterMessages(messages, "bee");

				// check all keys
				Assert.assertEquals(Arrays.asList("elephant,5,45218", "elephant,14,54867"), elephants);
				Assert.assertEquals(Arrays.asList("squirrel,12,46213", "squirrel,34,52444"), squirrels);
				Assert.assertEquals(Arrays.asList("bee,3,51348", "bee,13,53412"), bees);
			}

			// now, we add a new partition to the topic
			LOG.info("Repartitioning Kafka topic [{}] ...", inputTopic);
			kafka.setNumPartitions(2, inputTopic);
			Assert.assertEquals("Failed adding a partition to input topic.", 2, kafka.getNumPartitions(inputTopic));

			// send some more messages to Kafka
			LOG.info("Sending more messages to Kafka topic [{}] ...", inputTopic);
			kafka.sendMessages(inputTopic,
				"elephant,13,64213",
				"giraffe,9,65555",
				"bee,5,65647",
				"squirrel,18,66413");

			// verify that our assumption that the new partition actually has written messages is correct
			Assert.assertNotEquals(
				"The newly created partition does not have any new messages, and therefore partition discovery cannot be verified.",
				0L,
				kafka.getPartitionOffset(inputTopic, 1));

			LOG.info("Verifying messages from Kafka topic [{}] ...", outputTopic);
			{
				final List<String> messages = kafka.readMessage(4, "kafka-e2e-driver", outputTopic);

				final List<String> elephants = filterMessages(messages, "elephant");
				final List<String> squirrels = filterMessages(messages, "squirrel");
				final List<String> bees = filterMessages(messages, "bee");
				final List<String> giraffes = filterMessages(messages, "giraffe");

				Assert.assertEquals(Arrays.asList("elephant,27,64213"), elephants);
				Assert.assertEquals(Arrays.asList("squirrel,52,66413"), squirrels);
				Assert.assertEquals(Arrays.asList("bee,18,65647"), bees);
				Assert.assertEquals(Arrays.asList("giraffe,9,65555"), giraffes);
			}
		}
	}

	private static List<String> filterMessages(final List<String> messages, final String keyword) {
		return messages.stream()
			.filter(msg -> msg.contains(keyword))
			.collect(Collectors.toList());
	}
}
