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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Kafka resource to manage the local standalone kafka cluster, such as setUp, start, stop clusters and so on.
 */
public class LocalStandaloneKafkaResource implements KafkaResource {
	private static final Logger LOG = LoggerFactory.getLogger(LocalStandaloneKafkaResource.class);

	private final String fileURL;
	private final String packageName;
	private final Path testDataDir;

	public LocalStandaloneKafkaResource(String fileURL, String packageName, Path testDataDir) {
		this.fileURL = fileURL;
		this.packageName = packageName;
		this.testDataDir = testDataDir;
	}

	public Path getTestDataDir() {
		return this.testDataDir;
	}

	private String getKafkaDir() {
		String extractKafkaDir = packageName;
		for (String suffix : new String[]{".tar.gz", ".tgz"}) {
			if (extractKafkaDir.endsWith(suffix)) {
				extractKafkaDir = extractKafkaDir.substring(0, extractKafkaDir.length() - suffix.length());
			}
		}
		return testDataDir + "/" + extractKafkaDir;
	}

	@Override
	public void setUp() throws IOException {
		String kafkaTarGz = testDataDir + "/kafka.tgz";

		// Download kafka release
		AutoClosableProcess
			.create(
				CommandLineWrapper
					.wget(fileURL)
					.saveAs(kafkaTarGz)
					.build())
			.runBlocking(Duration.ofMinutes(5));

		// Extract the tar.gz package.
		AutoClosableProcess
			.create(
				CommandLineWrapper
					.tar(Paths.get(kafkaTarGz))
					.targetDir(testDataDir)
					.extract()
					.zipped()
					.build())
			.runBlocking(Duration.ofSeconds(30));

		// Fix the kafka configurations: zookeeper.properties and server.properties are included.
		AutoClosableProcess
			.create(
				CommandLineWrapper
					.sed("s+^\\(dataDir\\s*=\\s*\\).*$+\\1" + testDataDir + "/zookeeper+",
						Paths.get(getKafkaDir() + "/config/zookeeper.properties"))
					.inPlace()
					.build())
			.runBlocking();
		AutoClosableProcess
			.create(
				CommandLineWrapper
					.sed("s+^\\(log\\.dirs\\s*=\\s*\\).*$+\\1" + testDataDir + "/kafka+",
						Paths.get(getKafkaDir() + "/config/server.properties"))
					.inPlace()
					.build())
			.runBlocking();
		LOG.info("setup kafka cluster");
	}

	@Override
	public void start() throws IOException {
		// Start the zookeeper.
		String[] args = new String[]{getKafkaDir() + "/bin/zookeeper-server-start.sh",
			"-daemon", getKafkaDir() + "/config/zookeeper.properties"};
		AutoClosableProcess.create(args).runBlocking(Duration.ofSeconds(50));

		// Start the kafka server.
		args = new String[]{getKafkaDir() + "/bin/kafka-server-start.sh",
			"-daemon", getKafkaDir() + "/config/server.properties"};
		AutoClosableProcess.create(args).runBlocking(Duration.ofSeconds(50));

		// Ensure that the kafka cluster start successfully.
		args = new String[]{getKafkaDir() + "/bin/zookeeper-shell.sh", "localhost:2181", "get", "/brokers/ids/0"};
		final AtomicBoolean unavailable = new AtomicBoolean(false);
		for (int i = 0; i < 10; i++) {
			unavailable.set(false);
			Consumer<String> checker = (line) -> {
				if (line.contains("Node does not exist")) {
					unavailable.set(true);
				}
			};
			AutoClosableProcess.create(args).setStdoutProcessor(checker).runBlocking(Duration.ofSeconds(30));
			if (!unavailable.get()) {
				LOG.info("The kafka cluster has been started now.");
				break;
			}
		}
		if (unavailable.get()) {
			throw new IOException("Timeout(300 seconds) to wait the kafka cluster to be available.");
		}
	}

	@Override
	public void createTopic(int replicationFactor, int partitions, String topics) throws IOException {
		String[] args = new String[]{getKafkaDir() + "/bin/kafka-topics.sh",
			"--create",
			"--zookeeper", "localhost:2181",
			"--replication-factor", String.valueOf(replicationFactor),
			"--partitions", String.valueOf(partitions),
			"--topic", topics};
		AutoClosableProcess.create(args).runBlocking();
	}

	/**
	 * Send a single message to the Kafka Producer, Note that the message shouldn't contain any new line char, otherwise
	 * the command will be broken.
	 *
	 * @param topic   to send the message to.
	 * @param message content of message.
	 * @throws IOException
	 */
	@Override
	public void sendMessage(String topic, String message) throws IOException {
		String[] pipelineArgs = new String[]{
			"echo", message, "|",
			getKafkaDir() + "/bin/kafka-console-producer.sh",
			"--broker-list", "localhost:9092",
			"--topic", topic
		};
		String[] commands = new String[]{
			"/bin/sh",
			"-c",
			StringUtils.join(pipelineArgs, " ")
		};
		AutoClosableProcess.create(commands).runBlocking();
	}

	@Override
	public List<String> readMessage(int maxMessage, String topic, String groupId) throws IOException {
		String[] args = new String[]{
			getKafkaDir() + "/bin/kafka-console-consumer.sh",
			"--bootstrap-server", "localhost:9092",
			"--from-beginning",
			"--max-messages", String.valueOf(maxMessage),
			"--topic", topic,
			"--consumer-property",
			"group.id=" + groupId,
		};
		AutoClosableProcess.LineFetcher lineFetcher = new AutoClosableProcess.LineFetcher();
		AutoClosableProcess.create(args).setStdoutProcessor(lineFetcher).runBlocking();
		return lineFetcher.getLines();
	}

	@Override
	public void setNumPartitions(String topic, int num) throws IOException {
		String[] args = new String[]{
			getKafkaDir() + "/bin/kafka-topics.sh",
			"--alter",
			"--topic", topic,
			"--partitions", String.valueOf(num),
			"--zookeeper", "localhost:2181"
		};
		AutoClosableProcess.create(args).runBlocking();
	}

	@Override
	public int getNumPartitions(String topic) throws IOException {
		String[] pipelineCommands = new String[]{
			getKafkaDir() + "/bin/kafka-topics.sh",
			"--describe",
			"--topic", topic,
			"--zookeeper", "localhost:2181",
			"|", "grep -Eo \"PartitionCount:[0-9]+\"",
			"|", "cut -d \":\" -f 2"
		};
		String[] args = new String[]{
			"/bin/sh",
			"-c",
			StringUtils.join(pipelineCommands, " ")
		};
		AutoClosableProcess.LineFetcher lineFetcher = new AutoClosableProcess.LineFetcher();
		AutoClosableProcess.create(args).setStdoutProcessor(lineFetcher).runBlocking();
		return Integer.parseInt(lineFetcher.toString());
	}

	@Override
	public int getPartitionEndOffset(String topic, int partition) throws IOException {
		String[] args = new String[]{
			getKafkaDir() + "/bin/kafka-run-class.sh",
			"kafka.tools.GetOffsetShell",
			"--broker-list", "localhost:9092",
			"--topic", topic,
			"--partitions", String.valueOf(partition),
			"--time", "-1"
		};
		AutoClosableProcess.LineFetcher lineFetcher = new AutoClosableProcess.LineFetcher();
		AutoClosableProcess.create(args).setStdoutProcessor(lineFetcher).runBlocking();
		return Integer.parseInt(lineFetcher.toString().split(":")[2]);
	}

	@Override
	public void shutdown() throws IOException {
		LOG.info("Try to shutdown the zookeeper cluster in kafka.");
		AutoClosableProcess.killJavaProcess("QuorumPeerMain", false);

		LOG.info("Try to shutdown the kafka cluster");
		AutoClosableProcess.killJavaProcess("kafka", true);
	}
}
