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

import org.apache.commons.io.Charsets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.tests.util.FlinkSQLClient.findSQLJarPath;
import static org.apache.flink.util.StringUtils.byteToHexString;

public class TestSQLClientKafka {
	private static final Logger LOG = LoggerFactory.getLogger(TestSQLClientKafka.class);

	@Rule
	public final FlinkDistribution flinkDist = new FlinkDistribution();

	private final KafkaDistribution kafkaDist;
	private final Path testDataDir;
	private final String kafkaSQLVersion;
	private final String kafkaSQLJarVersion;
	private final Path sqlClientSessionConf;
	private final Path result;

	public TestSQLClientKafka() {
		this(
			DEFAULT_TEST_DATA_DIR,
			"https://mirrors.tuna.tsinghua.edu.cn/apache/kafka/2.1.1/kafka_2.11-2.1.1.tgz",
			"kafka_2.11-2.1.1.tgz",
			"universal",
			"kafka"
		);
	}

	protected TestSQLClientKafka(
		Path testDataDir,
		String kafkaDistURL,
		String kafkaDistName,
		String kafkaSQLVersion,
		String kafkaSQLJarVersion) {
		this.testDataDir = testDataDir;
		this.kafkaSQLVersion = kafkaSQLVersion;
		this.kafkaSQLJarVersion = kafkaSQLJarVersion;
		this.kafkaDist = new KafkaDistribution(
			kafkaDistURL,
			kafkaDistName,
			testDataDir);
		this.sqlClientSessionConf = testDataDir.resolve("sql-client-session.conf");
		this.result = this.testDataDir.resolve("result");
	}

	private static final String END_TO_END_TEST_DIR = End2EndUtil.getEnd2EndModuleDir();
	private static final Path DEFAULT_TEST_DATA_DIR = End2EndUtil.getTestDataDir();
	private static final String SQL_JARS = END_TO_END_TEST_DIR + "flink-sql-client-test/target/sql-jars";
	private static final String SQL_TOOL_BOX_JAR = END_TO_END_TEST_DIR + "flink-sql-client-test/target/SqlToolbox.jar";
	private static final String KAFKA_JSON_SOURCE_SCHEMA_YAML = "kafka_json_source_schema.yaml";

	public void setUpFlinkCluster() throws IOException {
		flinkDist.startFlinkCluster(3);
	}

	private String initializeSessionYaml(Map<String, String> vars) throws IOException {
		URL url = TestSQLClientKafka.class.getClassLoader().getResource(KAFKA_JSON_SOURCE_SCHEMA_YAML);
		if (url == null) {
			throw new FileNotFoundException(KAFKA_JSON_SOURCE_SCHEMA_YAML);
		}

		String schema = FileUtils.readFileUtf8(new File(url.getFile()));
		for (Map.Entry<String, String> var : vars.entrySet()) {
			schema = schema.replace(var.getKey(), var.getValue());
		}
		return schema;
	}

	@Before
	public void setUp() throws IOException {
		if (!Files.exists(testDataDir)) {
			Files.createDirectory(testDataDir);
		}
	}

	@After
	public void tearDown() throws IOException {
		this.shutdown();
		FileUtils.deleteDirectory(testDataDir.toFile());
	}

	@Test
	public void testKafka() throws Exception {
		// setup flink cluster
		setUpFlinkCluster();

		// setup kafa cluster
		kafkaDist.setUp();
		kafkaDist.start();

		// Create topic and send message
		String testJsonTopic = "test-json";
		kafkaDist.createTopic(1, 1, testJsonTopic);
		String[] messages = new String[]{
			"'{\"timestamp\": \"2018-03-12T08:00:00Z\", \"user\": \"Alice\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is a warning.\"}}'",
			"'{\"timestamp\": \"2018-03-12T08:10:00Z\", \"user\": \"Alice\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is a warning.\"}}'",
			"'{\"timestamp\": \"2018-03-12T09:00:00Z\", \"user\": \"Bob\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is another warning.\"}}'",
			"'{\"timestamp\": \"2018-03-12T09:10:00Z\", \"user\": \"Alice\", \"event\": { \"type\": \"INFO\", \"message\": \"This is a info.\"}}'",
			"'{\"timestamp\": \"2018-03-12T09:20:00Z\", \"user\": \"Steve\", \"event\": { \"type\": \"INFO\", \"message\": \"This is another info.\"}}'",
			"'{\"timestamp\": \"2018-03-12T09:30:00Z\", \"user\": \"Steve\", \"event\": { \"type\": \"INFO\", \"message\": \"This is another info.\"}}'",
			"'{\"timestamp\": \"2018-03-12T09:30:00Z\", \"user\": null, \"event\": { \"type\": \"WARNING\", \"message\": \"This is a bad message because the user is missing.\"}}'",
			"'{\"timestamp\": \"2018-03-12T10:40:00Z\", \"user\": \"Bob\", \"event\": { \"type\": \"ERROR\", \"message\": \"This is an error.\"}}'"
		};
		for (String message : messages) {
			kafkaDist.sendMessage(testJsonTopic, message);
		}

		// Create topic test-avro
		kafkaDist.createTopic(1, 1, "test-avro");

		// Initialize the SQL client session configuration file
		Map<String, String> varsMap = new HashMap<>();
		varsMap.put("$TABLE_NAME", "JsonSourceTable");
		varsMap.put("$KAFKA_SQL_VERSION", this.kafkaSQLVersion);
		varsMap.put("$TOPIC_NAME", "test-json");
		varsMap.put("$RESULT", this.result.toAbsolutePath().toString());
		String schemaContent = initializeSessionYaml(varsMap);
		Files.write(this.sqlClientSessionConf,
			schemaContent.getBytes(Charsets.UTF_8),
			StandardOpenOption.CREATE,
			StandardOpenOption.WRITE);

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
		flinkDist.newSQLClient()
			.embedded(true)
			.addJAR(findSQLJarPath(SQL_JARS, "avro"))
			.addJAR(findSQLJarPath(SQL_JARS, "json"))
			.addJAR(findSQLJarPath(SQL_JARS, this.kafkaSQLJarVersion + "_"))
			.addJAR(SQL_TOOL_BOX_JAR)
			.sessionEnvironmentFile(this.sqlClientSessionConf.toAbsolutePath().toString())
			.createProcess(sqlStatement1)
			.runBlocking(Duration.ofMinutes(1));

		String sqlStatement2 = "INSERT INTO CsvSinkTable\n" +
			"   SELECT AvroBothTable.*, RegReplace('Test constant folding.', 'Test', 'Success') AS constant\n" +
			"   FROM AvroBothTable";

		flinkDist.newSQLClient()
			.embedded(true)
			.addJAR(findSQLJarPath(SQL_JARS, "avro"))
			.addJAR(findSQLJarPath(SQL_JARS, "json"))
			.addJAR(findSQLJarPath(SQL_JARS, this.kafkaSQLJarVersion + "_"))
			.addJAR(SQL_TOOL_BOX_JAR)
			.sessionEnvironmentFile(this.sqlClientSessionConf.toAbsolutePath().toString())
			.createProcess(sqlStatement2)
			.runBlocking(Duration.ofMinutes(1));

		// Wait until all the results flushed to the CSV file.
		checkCsvResultFile();
		LOG.info("The Kafka({}) SQL client test run successfully.", this.kafkaSQLVersion);
	}

	private void checkCsvResultFile() throws IOException, InterruptedException, NoSuchAlgorithmException {
		boolean success = false;
		long maxRetries = 10, duration = 5000L;
		for (int i = 0; i < maxRetries; i++) {
			if (Files.exists(result)) {
				List<String> lines = Files.readAllLines(result);
				if (lines.size() == 4) {
					success = true;
					// Check the MD5SUM of the result file.
					Assert.assertEquals("MD5 checksum mismatch", "390b2985cbb001fbf4d301980da0e7f0", getMd5Sum(result));
					break;
				}
			} else {
				LOG.info("The target CSV {} does not exist now", result);
			}
			Thread.sleep(duration);
		}
		Assert.assertTrue("Timeout(" + (maxRetries * duration) + " sec) to read the correct CSV results.", success);
	}

	private static String getMd5Sum(Path path) throws IOException, NoSuchAlgorithmException {
		MessageDigest md = MessageDigest.getInstance("MD5");
		try (InputStream is = Files.newInputStream(path)) {
			DigestInputStream dis = new DigestInputStream(is, md);
			byte[] buf = new byte[1024];
			for (; dis.read(buf) > 0; ) {
			}
		}
		return byteToHexString(md.digest());
	}

	private void shutdown() {
		try {
			kafkaDist.shutdown();
		} catch (IOException e) {
			LOG.info("Failed to shutdown the kafka cluster.", e);
		}
		try {
			flinkDist.stopFlinkCluster();
		} catch (IOException e) {
			LOG.info("Failed to shutdown the flink cluster.", e);
		}
	}
}
