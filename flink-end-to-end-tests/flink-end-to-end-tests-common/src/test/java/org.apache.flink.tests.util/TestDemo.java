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
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.internal.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.tests.util.FlinkSQLClient.findSQLJarPath;

public class TestDemo {
	private static final Logger LOG = LoggerFactory.getLogger(TestDemo.class);

	private static final String KAFKA_SQL_VERSION = "universal";
	private static final String KAFKA_SQL_JAR_VERSION = "kafka";
	private static final String TEST_DATA_DIR = "/Users/openinx/test/tmp-" + System.currentTimeMillis();
	private static final String SQL_CLIENT_SESSION_CONF = TEST_DATA_DIR + "/sql-client-session.conf";

	@Rule
	public final FlinkDistribution flinkDist = new FlinkDistribution();
	private final KafkaDistribution kafkaDist;

	public TestDemo() {
		kafkaDist = new KafkaDistribution(
			"https://mirrors.tuna.tsinghua.edu.cn/apache/kafka/2.1.1/kafka_2.11-2.1.1.tgz",
			"kafka_2.11-2.1.1.tgz",
			TEST_DATA_DIR);
	}

	private static final String END_TO_END_TEST_DIR = "/Users/openinx/software/flink/flink-end-to-end-tests";
	private static final String SQL_JARS = END_TO_END_TEST_DIR + "/flink-sql-client-test/target/sql-jars";
	private static final String SQL_TOOL_BOX_JAR = END_TO_END_TEST_DIR + "/flink-sql-client-test/target/SqlToolbox.jar";
	private static final String KAFKA_JSON_SOURCE_SCHEMA_YAML = "kafka_json_source_schema.yaml";
	private static final String RESULT_FILE = TEST_DATA_DIR + "/result";

	public void setUpFlinkCluster() throws IOException {
		flinkDist.startFlinkCluster();
	}

	private String initializeSessionYaml(Map<String, String> vars) throws IOException {
		URL url = TestDemo.class.getClassLoader().getResource(KAFKA_JSON_SOURCE_SCHEMA_YAML);
		if (url == null) {
			throw new FileNotFoundException(KAFKA_JSON_SOURCE_SCHEMA_YAML);
		}

		String schema = FileUtils.readFileUtf8(new File(url.getFile()));
		for (Map.Entry<String, String> var : vars.entrySet()) {
			schema = schema.replace(var.getKey(), var.getValue());
		}
		return schema;
	}

	@Test
	public void testKafka() throws IOException, InterruptedException {
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
		varsMap.put("$KAFKA_SQL_VERSION", KAFKA_SQL_VERSION);
		varsMap.put("$TOPIC_NAME", "test-json");
		varsMap.put("$RESULT", RESULT_FILE);
		String schemaContent = initializeSessionYaml(varsMap);
		LOG.info("===> schema content: {}", schemaContent);
		Files.write(Paths.get(SQL_CLIENT_SESSION_CONF), schemaContent.getBytes(Charsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.WRITE);

		LOG.info("Executing SQL: Kafka 2.1.1 JSON -> Kafka 2.1.1 Avro");
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
			.addJAR(findSQLJarPath(SQL_JARS, KAFKA_SQL_JAR_VERSION + "_"))
			.addJAR(SQL_TOOL_BOX_JAR)
			.sessionEnvironmentFile(SQL_CLIENT_SESSION_CONF)
			.createProcess(sqlStatement1)
			.setStdoutProcessor(line -> {
				LOG.info("stdout => {}", line);
			})
			.setStderrProcessor(line -> {
				LOG.info("stderr => {}", line);
			})
			.runBlocking(Duration.ofMinutes(1));

		String sqlStatement2 = "INSERT INTO CsvSinkTable\n" +
			"   SELECT AvroBothTable.*, RegReplace('Test constant folding.', 'Test', 'Success') AS constant\n" +
			"   FROM AvroBothTable";

		flinkDist.newSQLClient()
			.embedded(true)
			.addJAR(findSQLJarPath(SQL_JARS, "avro"))
			.addJAR(findSQLJarPath(SQL_JARS, "json"))
			.addJAR(findSQLJarPath(SQL_JARS, KAFKA_SQL_JAR_VERSION + "_"))
			.addJAR(SQL_TOOL_BOX_JAR)
			.sessionEnvironmentFile(SQL_CLIENT_SESSION_CONF)
			.createProcess(sqlStatement2)
			.setStdoutProcessor(line -> {
				LOG.info("stdout => {}", line);
			})
			.setStderrProcessor(line -> {
				LOG.info("stderr => {}", line);
			})
			.runBlocking(Duration.ofMinutes(1));

		// Wait until all the results flushed to the CSV file.
		waitUntilFlushToTheCSV();
	}

	private void waitUntilFlushToTheCSV() throws IOException, InterruptedException {
		Path target = Paths.get(RESULT_FILE);
		boolean success = false;
		for (int i = 0; i < 10; i++) {
			if (Files.exists(target)) {
				List<String> lines = Files.readAllLines(target);
				LOG.info("content ==> {}", StringUtil.join(lines, "\n"));
				if (lines.size() == 4) {
					success = true;
					break;
				}
			} else {
				LOG.info("The target CSV {} does not exist now", target);
			}
			Thread.sleep(5000L);
		}
		Assert.assertTrue("Timeout(50 sec) to read the correct CSV results.", success);
	}

	public void shutdown() {
		try {
			kafkaDist.shutdown();
		} catch (IOException e) {
			LOG.info("Failed to shutdown the kafka cluster.");
		}
		try {
			flinkDist.stopFlinkCluster();
		} catch (IOException e) {
			LOG.info("Failed to shutdown the flink cluster.");
		}
	}

	public static void main(String[] args) throws IOException {
		Path testDataDir = Files.createDirectory(Paths.get(TEST_DATA_DIR));
		TestDemo testDemo = new TestDemo();
		try {
			testDemo.testKafka();
		} catch (IOException | InterruptedException e) {
			LOG.error("The kafka testing encountered error", e);
		} finally {
			testDemo.shutdown();
			//Files.deleteIfExists(testDataDir);
		}
	}
}
