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

import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.categories.Hadoop;
import org.apache.flink.tests.util.categories.TravisGroup1;
import org.apache.flink.tests.util.flink.ClusterController;
import org.apache.flink.tests.util.flink.FlinkResource;
import org.apache.flink.tests.util.flink.FlinkResourceSetup;
import org.apache.flink.tests.util.flink.LocalStandaloneFlinkResourceFactory;
import org.apache.flink.tests.util.flink.SQLJobSubmission;
import org.apache.flink.testutils.junit.FailsOnJava11;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.base.Charsets;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
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
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.junit.Assert.assertThat;

/**
 * End-to-end test for the kafka SQL connectors.
 */
@RunWith(Parameterized.class)
@Category(value = {TravisGroup1.class, FailsOnJava11.class, Hadoop.class})
public class SQLClientKafkaITCase extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(SQLClientKafkaITCase.class);

	private static final String KAFKA_JSON_SOURCE_SCHEMA_YAML = "kafka_json_source_schema.yaml";

	@Parameterized.Parameters(name = "{index}: kafka-version:{1} kafka-sql-version:{2}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
				{"0.10.2.0", "0.10", ".*kafka-0.10.jar"},
				{"0.11.0.2", "0.11", ".*kafka-0.11.jar"},
				{"2.2.0", "universal", ".*kafka.jar"}
		});
	}

	@Rule
	public final FlinkResource flink = new LocalStandaloneFlinkResourceFactory()
		.create(FlinkResourceSetup.builder().build())
		.get();

	@Rule
	public final KafkaResource kafka;

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	private final String kafkaSQLVersion;
	private Path result;
	private Path sqlClientSessionConf;

	private static final Path sqlAvroJar = TestUtils.getResourceJar(".*avro.jar");
	private static final Path sqlJsonJar = TestUtils.getResourceJar(".*json.jar");
	private static final Path sqlToolBoxJar = TestUtils.getResourceJar(".*SqlToolbox.jar");
	private final Path sqlConnectorKafkaJar;

	public SQLClientKafkaITCase(String kafkaVersion, String kafkaSQLVersion, String kafkaSQLJarPattern) {
		this.kafka = KafkaResource.get(kafkaVersion);
		this.kafkaSQLVersion = kafkaSQLVersion;

		this.sqlConnectorKafkaJar = TestUtils.getResourceJar(kafkaSQLJarPattern);
	}

	@Before
	public void before() {
		Path tmpPath = tmp.getRoot().toPath();
		LOG.info("The current temporary path: {}", tmpPath);
		this.sqlClientSessionConf = tmpPath.resolve("sql-client-session.conf");
		this.result = tmpPath.resolve("result");
	}

	@Test
	public void testKafka() throws Exception {
		try (ClusterController clusterController = flink.startCluster(2)) {
			// Create topic and send message
			String testJsonTopic = "test-json";
			String testAvroTopic = "test-avro";
			kafka.createTopic(1, 1, testJsonTopic);
			String[] messages = new String[]{
					"{\"timestamp\": \"2018-03-12T08:00:00Z\", \"user\": \"Alice\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is a warning.\"}}",
					"{\"timestamp\": \"2018-03-12T08:10:00Z\", \"user\": \"Alice\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is a warning.\"}}",
					"{\"timestamp\": \"2018-03-12T09:00:00Z\", \"user\": \"Bob\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is another warning.\"}}",
					"{\"timestamp\": \"2018-03-12T09:10:00Z\", \"user\": \"Alice\", \"event\": { \"type\": \"INFO\", \"message\": \"This is a info.\"}}",
					"{\"timestamp\": \"2018-03-12T09:20:00Z\", \"user\": \"Steve\", \"event\": { \"type\": \"INFO\", \"message\": \"This is another info.\"}}",
					"{\"timestamp\": \"2018-03-12T09:30:00Z\", \"user\": \"Steve\", \"event\": { \"type\": \"INFO\", \"message\": \"This is another info.\"}}",
					"{\"timestamp\": \"2018-03-12T09:30:00Z\", \"user\": null, \"event\": { \"type\": \"WARNING\", \"message\": \"This is a bad message because the user is missing.\"}}",
					"{\"timestamp\": \"2018-03-12T10:40:00Z\", \"user\": \"Bob\", \"event\": { \"type\": \"ERROR\", \"message\": \"This is an error.\"}}"
			};
			kafka.sendMessages(testJsonTopic, messages);

			// Create topic test-avro
			kafka.createTopic(1, 1, testAvroTopic);

			// Initialize the SQL client session configuration file
			Map<String, String> varsMap = new HashMap<>();
			varsMap.put("$TABLE_NAME", "JsonSourceTable");
			varsMap.put("$KAFKA_SQL_VERSION", this.kafkaSQLVersion);
			varsMap.put("$TOPIC_NAME", testJsonTopic);
			varsMap.put("$RESULT", this.result.toAbsolutePath().toString());
			varsMap.put("$KAFKA_ZOOKEEPER_ADDRESS", kafka.getZookeeperAddress().toString());
			varsMap.put("$KAFKA_BOOTSTRAP_SERVERS", StringUtils.join(kafka.getBootstrapServerAddresses().toArray(), ","));
			String schemaContent = initializeSessionYaml(varsMap);
			Files.write(this.sqlClientSessionConf,
					schemaContent.getBytes(Charsets.UTF_8),
					StandardOpenOption.CREATE,
					StandardOpenOption.WRITE);

			// Executing SQL, redirect the data from Kafka JSON to Kafka Avro.
			insertIntoAvroTable(clusterController);

			// Executing SQL, redirect the data from Kafka Avro to CSV sink.
			insertIntoCsvSinkTable(clusterController);

			// Wait until all the results flushed to the CSV file.
			LOG.info("Verify the CSV result.");
			checkCsvResultFile();
			LOG.info("The Kafka({}) SQL client test run successfully.", this.kafkaSQLVersion);
		}
	}

	private void insertIntoAvroTable(ClusterController clusterController) throws IOException {
		LOG.info("Executing SQL: Kafka {} JSON -> Kafka {} Avro", kafkaSQLVersion, kafkaSQLVersion);
		String sqlStatement1 = "INSERT INTO AvroBothTable\n" +
				"  SELECT\n" +
				"    CAST(TUMBLE_START(rowtime, INTERVAL '1' HOUR) AS VARCHAR) AS event_timestamp,\n" +
				"    user,\n" +
				"    RegReplace(event.message, ' is ', ' was ') AS message,\n" +
				"    COUNT(*) AS duplicate_count\n" +
				"  FROM JsonSourceTable\n" +
				"  WHERE user IS NOT NULL\n" +
				"  GROUP BY\n" +
				"    user,\n" +
				"    event.message,\n" +
				"    TUMBLE(rowtime, INTERVAL '1' HOUR)";

		clusterController.submitSQLJob(new SQLJobSubmission.SQLJobSubmissionBuilder(sqlStatement1)
				.addJar(sqlAvroJar)
				.addJar(sqlJsonJar)
				.addJar(sqlConnectorKafkaJar)
				.addJar(sqlToolBoxJar)
				.setSessionEnvFile(this.sqlClientSessionConf.toAbsolutePath().toString())
				.build());
	}

	private void insertIntoCsvSinkTable(ClusterController clusterController) throws IOException {
		LOG.info("Executing SQL: Kafka {} Avro -> Csv sink", kafkaSQLVersion);
		String sqlStatement2 = "INSERT INTO CsvSinkTable\n" +
				"   SELECT AvroBothTable.*, RegReplace('Test constant folding.', 'Test', 'Success') AS constant\n" +
				"   FROM AvroBothTable";

		clusterController.submitSQLJob(new SQLJobSubmission.SQLJobSubmissionBuilder(sqlStatement2)
				.addJar(sqlAvroJar)
				.addJar(sqlJsonJar)
				.addJar(sqlConnectorKafkaJar)
				.addJar(sqlToolBoxJar)
				.setSessionEnvFile(this.sqlClientSessionConf.toAbsolutePath().toString())
				.build()
		);
	}

	private String initializeSessionYaml(Map<String, String> vars) throws IOException {
		URL url = SQLClientKafkaITCase.class.getClassLoader().getResource(KAFKA_JSON_SOURCE_SCHEMA_YAML);
		if (url == null) {
			throw new FileNotFoundException(KAFKA_JSON_SOURCE_SCHEMA_YAML);
		}

		String schema = FileUtils.readFileUtf8(new File(url.getFile()));
		for (Map.Entry<String, String> var : vars.entrySet()) {
			schema = schema.replace(var.getKey(), var.getValue());
		}
		return schema;
	}

	private void checkCsvResultFile() throws Exception {
		boolean success = false;
		long maxRetries = 10, duration = 5000L;
		for (int i = 0; i < maxRetries; i++) {
			if (Files.exists(result)) {
				byte[] bytes = Files.readAllBytes(result);
				String[] lines = new String(bytes, Charsets.UTF_8).split("\n");
				if (lines.length == 4) {
					success = true;
					assertThat(
						lines,
						arrayContainingInAnyOrder(
							"2018-03-12 08:00:00.000,Alice,This was a warning.,2,Success constant folding.",
							"2018-03-12 09:00:00.000,Bob,This was another warning.,1,Success constant folding.",
							"2018-03-12 09:00:00.000,Steve,This was another info.,2,Success constant folding.",
							"2018-03-12 09:00:00.000,Alice,This was a info.,1,Success constant folding."
						)
					);
					break;
				}
			} else {
				LOG.info("The target CSV {} does not exist now", result);
			}
			Thread.sleep(duration);
		}
		Assert.assertTrue("Timeout(" + (maxRetries * duration) + " sec) to read the correct CSV results.", success);
	}
}
