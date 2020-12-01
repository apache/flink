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

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.cache.DownloadCache;
import org.apache.flink.tests.util.categories.TravisGroup1;
import org.apache.flink.tests.util.flink.ClusterController;
import org.apache.flink.tests.util.flink.FlinkResource;
import org.apache.flink.tests.util.flink.FlinkResourceSetup;
import org.apache.flink.tests.util.flink.LocalStandaloneFlinkResourceFactory;
import org.apache.flink.tests.util.flink.SQLJobSubmission;
import org.apache.flink.testutils.junit.FailsOnJava11;
import org.apache.flink.util.TestLogger;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.junit.Assert.assertThat;

/**
 * End-to-end test for the kafka SQL connectors.
 */
@RunWith(Parameterized.class)
@Category(value = {TravisGroup1.class, FailsOnJava11.class})
public class SQLClientKafkaITCase extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(SQLClientKafkaITCase.class);

	private static final String KAFKA_E2E_SQL = "kafka_e2e.sql";

	@Parameterized.Parameters(name = "{index}: kafka-version:{0} kafka-sql-version:{1}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
				{"2.4.1", "universal", "kafka", ".*kafka.jar"}
		});
	}

	private static Configuration getConfiguration() {
		// we have to enable checkpoint to trigger flushing for filesystem sink
		final Configuration flinkConfig = new Configuration();
		flinkConfig.setString("execution.checkpointing.interval", "5s");
		return flinkConfig;
	}

	@Rule
	public final FlinkResource flink = new LocalStandaloneFlinkResourceFactory()
		.create(FlinkResourceSetup.builder().addConfiguration(getConfiguration()).build());

	@Rule
	public final KafkaResource kafka;

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	private final String kafkaVersion;
	private final String kafkaSQLVersion;
	private final String kafkaIdentifier;
	private Path result;

	@ClassRule
	public static final DownloadCache DOWNLOAD_CACHE = DownloadCache.get();

	private static final Path sqlAvroJar = TestUtils.getResource(".*avro.jar");
	private static final Path sqlToolBoxJar = TestUtils.getResource(".*SqlToolbox.jar");
	private final List<Path> apacheAvroJars = new ArrayList<>();
	private final Path sqlConnectorKafkaJar;

	public SQLClientKafkaITCase(String kafkaVersion, String kafkaSQLVersion, String kafkaIdentifier, String kafkaSQLJarPattern) {
		this.kafka = KafkaResource.get(kafkaVersion);
		this.kafkaVersion = kafkaVersion;
		this.kafkaSQLVersion = kafkaSQLVersion;
		this.kafkaIdentifier = kafkaIdentifier;

		this.sqlConnectorKafkaJar = TestUtils.getResource(kafkaSQLJarPattern);
	}

	@Before
	public void before() throws Exception {
		DOWNLOAD_CACHE.before();
		Path tmpPath = tmp.getRoot().toPath();
		LOG.info("The current temporary path: {}", tmpPath);
		this.result = tmpPath.resolve("result");
	}

	@Test
	public void testKafka() throws Exception {
		try (ClusterController clusterController = flink.startCluster(2)) {
			// Create topic and send message
			String testJsonTopic = "test-json-" + kafkaVersion + "-" + UUID.randomUUID().toString();
			String testAvroTopic = "test-avro-" + kafkaVersion + "-" + UUID.randomUUID().toString();
			kafka.createTopic(1, 1, testJsonTopic);
			String[] messages = new String[]{
					"{\"rowtime\": \"2018-03-12 08:00:00\", \"user\": \"Alice\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is a warning.\"}}",
					"{\"rowtime\": \"2018-03-12 08:10:00\", \"user\": \"Alice\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is a warning.\"}}",
					"{\"rowtime\": \"2018-03-12 09:00:00\", \"user\": \"Bob\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is another warning.\"}}",
					"{\"rowtime\": \"2018-03-12 09:10:00\", \"user\": \"Alice\", \"event\": { \"type\": \"INFO\", \"message\": \"This is a info.\"}}",
					"{\"rowtime\": \"2018-03-12 09:20:00\", \"user\": \"Steve\", \"event\": { \"type\": \"INFO\", \"message\": \"This is another info.\"}}",
					"{\"rowtime\": \"2018-03-12 09:30:00\", \"user\": \"Steve\", \"event\": { \"type\": \"INFO\", \"message\": \"This is another info.\"}}",
					"{\"rowtime\": \"2018-03-12 09:30:00\", \"user\": null, \"event\": { \"type\": \"WARNING\", \"message\": \"This is a bad message because the user is missing.\"}}",
					"{\"rowtime\": \"2018-03-12 10:40:00\", \"user\": \"Bob\", \"event\": { \"type\": \"ERROR\", \"message\": \"This is an error.\"}}"
			};
			kafka.sendMessages(testJsonTopic, messages);

			// Create topic test-avro
			kafka.createTopic(1, 1, testAvroTopic);

			// Initialize the SQL statements from "kafka_e2e.sql" file
			Map<String, String> varsMap = new HashMap<>();
			varsMap.put("$KAFKA_IDENTIFIER", this.kafkaIdentifier);
			varsMap.put("$TOPIC_JSON_NAME", testJsonTopic);
			varsMap.put("$TOPIC_AVRO_NAME", testAvroTopic);
			varsMap.put("$RESULT", this.result.toAbsolutePath().toString());
			varsMap.put("$KAFKA_BOOTSTRAP_SERVERS", StringUtils.join(kafka.getBootstrapServerAddresses().toArray(), ","));
			List<String> sqlLines = initializeSqlLines(varsMap);

			// Execute SQL statements in "kafka_e2e.sql" file
			executeSqlStatements(clusterController, sqlLines);

			// Wait until all the results flushed to the CSV file.
			LOG.info("Verify the CSV result.");
			checkCsvResultFile();
			LOG.info("The Kafka({}) SQL client test run successfully.", this.kafkaSQLVersion);
		}
	}

	private void executeSqlStatements(ClusterController clusterController, List<String> sqlLines) throws IOException {
		LOG.info("Executing Kafka {} end-to-end SQL statements.", kafkaSQLVersion);
		clusterController.submitSQLJob(
			new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines)
				.addJar(sqlAvroJar)
				.addJars(apacheAvroJars)
				.addJar(sqlConnectorKafkaJar)
				.addJar(sqlToolBoxJar)
				.build(),
			Duration.ofMinutes(2L));
	}

	private List<String> initializeSqlLines(Map<String, String> vars) throws IOException {
		URL url = SQLClientKafkaITCase.class.getClassLoader().getResource(KAFKA_E2E_SQL);
		if (url == null) {
			throw new FileNotFoundException(KAFKA_E2E_SQL);
		}

		List<String> lines = Files.readAllLines(new File(url.getFile()).toPath());
		List<String> result = new ArrayList<>();
		for (String line : lines) {
			for (Map.Entry<String, String> var : vars.entrySet()) {
				line = line.replace(var.getKey(), var.getValue());
			}
			result.add(line);
		}

		return result;
	}

	private void checkCsvResultFile() throws Exception {
		boolean success = false;
		final Deadline deadline = Deadline.fromNow(Duration.ofSeconds(120));
		while (deadline.hasTimeLeft()) {
			if (Files.exists(result)) {
				List<String> lines = readCsvResultFiles(result);
				if (lines.size() == 4) {
					success = true;
					assertThat(
						lines.toArray(new String[0]),
						arrayContainingInAnyOrder(
							"2018-03-12 08:00:00.000,Alice,This was a warning.,2,Success constant folding.",
							"2018-03-12 09:00:00.000,Bob,This was another warning.,1,Success constant folding.",
							"2018-03-12 09:00:00.000,Steve,This was another info.,2,Success constant folding.",
							"2018-03-12 09:00:00.000,Alice,This was a info.,1,Success constant folding."
						)
					);
					break;
				} else {
					LOG.info("The target CSV {} does not contain enough records, current {} records, left time: {}s",
						result, lines.size(), deadline.timeLeft().getSeconds());
				}
			} else {
				LOG.info("The target CSV {} does not exist now", result);
			}
			Thread.sleep(500);
		}
		Assert.assertTrue("Did not get expected results before timeout.", success);
	}

	private static List<String> readCsvResultFiles(Path path) throws IOException {
		File filePath = path.toFile();
		// list all the non-hidden files
		File[] csvFiles = filePath.listFiles((dir, name) -> !name.startsWith("."));
		List<String> result = new ArrayList<>();
		if (csvFiles != null) {
			for (File file : csvFiles) {
				result.addAll(Files.readAllLines(file.toPath()));
			}
		}
		return result;
	}
}
