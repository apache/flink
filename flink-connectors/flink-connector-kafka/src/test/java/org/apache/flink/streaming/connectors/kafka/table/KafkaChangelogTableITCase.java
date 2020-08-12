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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase;
import org.apache.flink.streaming.connectors.kafka.KafkaTestBaseWithFlink;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * IT cases for Kafka with changelog format for Table API & SQL.
 */
public class KafkaChangelogTableITCase extends KafkaTestBaseWithFlink {

	protected StreamExecutionEnvironment env;
	protected StreamTableEnvironment tEnv;

	@Before
	public void setup() {
		TestValuesTableFactory.clearAllData();
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		tEnv = StreamTableEnvironment.create(
			env,
			EnvironmentSettings.newInstance()
				// Watermark is only supported in blink planner
				.useBlinkPlanner()
				.inStreamingMode()
				.build()
		);
		env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		// we have to use single parallelism,
		// because we will count the messages in sink to terminate the job
		env.setParallelism(1);
	}

	@Test
	public void testKafkaDebeziumChangelogSource() throws Exception {
		final String topic = "changelog_topic";
		createTestTopic(topic, 1, 1);

		// enables MiniBatch processing to verify MiniBatch + FLIP-95, see FLINK-18769
		Configuration tableConf = tEnv.getConfig().getConfiguration();
		tableConf.setString("table.exec.mini-batch.enabled", "true");
		tableConf.setString("table.exec.mini-batch.allow-latency", "1s");
		tableConf.setString("table.exec.mini-batch.size", "5000");
		tableConf.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");

		// ---------- Write the Debezium json into Kafka -------------------
		List<String> lines = readLines("debezium-data-schema-exclude.txt");
		DataStreamSource<String> stream = env.fromCollection(lines);
		SerializationSchema<String> serSchema = new SimpleStringSchema();
		FlinkKafkaPartitioner<String> partitioner = new FlinkFixedPartitioner<>();

		// the producer must not produce duplicates
		Properties producerProperties = FlinkKafkaProducerBase.getPropertiesFromBrokerList(brokerConnectionStrings);
		producerProperties.setProperty("retries", "0");
		producerProperties.putAll(secureProps);
		kafkaServer.produceIntoKafka(stream, topic, serSchema, producerProperties, partitioner);
		try {
			env.execute("Write sequence");
		}
		catch (Exception e) {
			throw new Exception("Failed to write debezium data to Kafka.", e);
		}

		// ---------- Produce an event time stream into Kafka -------------------
		String bootstraps = standardProps.getProperty("bootstrap.servers");
		String sourceDDL = String.format(
			"CREATE TABLE debezium_source (" +
			" id INT NOT NULL," +
			" name STRING," +
			" description STRING," +
			" weight DECIMAL(10,3)" +
			") WITH (" +
			" 'connector' = 'kafka'," +
			" 'topic' = '%s'," +
			" 'properties.bootstrap.servers' = '%s'," +
			" 'scan.startup.mode' = 'earliest-offset'," +
			" 'format' = 'debezium-json'" +
			")", topic, bootstraps);
		String sinkDDL = "CREATE TABLE sink (" +
			" name STRING," +
			" weightSum DECIMAL(10,3)," +
			" PRIMARY KEY (name) NOT ENFORCED" +
			") WITH (" +
			" 'connector' = 'values'," +
			" 'sink-insert-only' = 'false'" +
			")";
		tEnv.executeSql(sourceDDL);
		tEnv.executeSql(sinkDDL);
		TableResult tableResult = tEnv.executeSql(
			"INSERT INTO sink SELECT name, SUM(weight) FROM debezium_source GROUP BY name");

		// Debezium captures change data on the `products` table:
		//
		// CREATE TABLE products (
		//  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
		//  name VARCHAR(255),
		//  description VARCHAR(512),
		//  weight FLOAT
		// );
		// ALTER TABLE products AUTO_INCREMENT = 101;
		//
		// INSERT INTO products
		// VALUES (default,"scooter","Small 2-wheel scooter",3.14),
		//        (default,"car battery","12V car battery",8.1),
		//        (default,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",0.8),
		//        (default,"hammer","12oz carpenter's hammer",0.75),
		//        (default,"hammer","14oz carpenter's hammer",0.875),
		//        (default,"hammer","16oz carpenter's hammer",1.0),
		//        (default,"rocks","box of assorted rocks",5.3),
		//        (default,"jacket","water resistent black wind breaker",0.1),
		//        (default,"spare tire","24 inch spare tire",22.2);
		// UPDATE products SET description='18oz carpenter hammer' WHERE id=106;
		// UPDATE products SET weight='5.1' WHERE id=107;
		// INSERT INTO products VALUES (default,"jacket","water resistent white wind breaker",0.2);
		// INSERT INTO products VALUES (default,"scooter","Big 2-wheel scooter ",5.18);
		// UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;
		// UPDATE products SET weight='5.17' WHERE id=111;
		// DELETE FROM products WHERE id=111;
		//
		// > SELECT * FROM products;
		// +-----+--------------------+---------------------------------------------------------+--------+
		// | id  | name               | description                                             | weight |
		// +-----+--------------------+---------------------------------------------------------+--------+
		// | 101 | scooter            | Small 2-wheel scooter                                   |   3.14 |
		// | 102 | car battery        | 12V car battery                                         |    8.1 |
		// | 103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from #40 to #3 |    0.8 |
		// | 104 | hammer             | 12oz carpenter's hammer                                 |   0.75 |
		// | 105 | hammer             | 14oz carpenter's hammer                                 |  0.875 |
		// | 106 | hammer             | 18oz carpenter hammer                                   |      1 |
		// | 107 | rocks              | box of assorted rocks                                   |    5.1 |
		// | 108 | jacket             | water resistent black wind breaker                      |    0.1 |
		// | 109 | spare tire         | 24 inch spare tire                                      |   22.2 |
		// | 110 | jacket             | new water resistent white wind breaker                  |    0.5 |
		// +-----+--------------------+---------------------------------------------------------+--------+

		List<String> expected = Arrays.asList(
			"scooter,3.140", "car battery,8.100", "12-pack drill bits,0.800",
			"hammer,2.625", "rocks,5.100", "jacket,0.600", "spare tire,22.200");

		waitingExpectedResults("sink", expected, Duration.ofSeconds(10));

		// ------------- cleanup -------------------

		tableResult.getJobClient().get().cancel().get(); // stop the job
		deleteTestTopic(topic);
	}

	// --------------------------------------------------------------------------------------------
	// Utilities
	// --------------------------------------------------------------------------------------------

	private static List<String> readLines(String resource) throws IOException {
		final URL url = KafkaChangelogTableITCase.class.getClassLoader().getResource(resource);
		assert url != null;
		Path path = new File(url.getFile()).toPath();
		return Files.readAllLines(path);
	}

	private static void waitingExpectedResults(String sinkName, List<String> expected, Duration timeout) throws InterruptedException {
		long now = System.currentTimeMillis();
		long stop = now + timeout.toMillis();
		Collections.sort(expected);
		while (System.currentTimeMillis() < stop) {
			List<String> actual = TestValuesTableFactory.getResults(sinkName);
			Collections.sort(actual);
			if (expected.equals(actual)) {
				return;
			}
			Thread.sleep(100);
		}

		// timeout, assert again
		List<String> actual = TestValuesTableFactory.getResults(sinkName);
		Collections.sort(actual);
		assertEquals(expected, actual);
	}
}
