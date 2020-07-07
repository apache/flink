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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.descriptors.KafkaValidator;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.TableEnvUtil;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

/**
 * IT cases for Kafka for Table API & SQL.
 */
public class KafkaTableITCase extends KafkaTableTestBase {

	@Override
	public String factoryIdentifier() {
		return KafkaDynamicTableFactory.IDENTIFIER;
	}

	@Override
	public String kafkaVersion() {
		return KafkaValidator.CONNECTOR_VERSION_VALUE_UNIVERSAL;
	}

	@Test
	public void testKafkaDebeziumChangelogSource() throws Exception {
		if (isLegacyConnector) {
			// skip tests for legacy connector, because changelog source is only supported in new connector
			return;
		}
		final String topic = "changelog_topic";
		createTestTopic(topic, 1, 1);

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
			" 'connector' = '%s'," +
			" 'topic' = '%s'," +
			" 'properties.bootstrap.servers' = '%s'," +
			" 'scan.startup.mode' = 'earliest-offset'," +
			" 'format' = 'debezium-json'" +
			")", factoryIdentifier(), topic, bootstraps);
		String sinkDDL = "CREATE TABLE sink (" +
			" name STRING," +
			" weightSum DECIMAL(10,3)," +
			" PRIMARY KEY (name) NOT ENFORCED" +
			") WITH (" +
			" 'connector' = 'values'," +
			" 'sink-insert-only' = 'false'," +
			" 'sink-expected-messages-num' = '20'" +
			")";
		tEnv.executeSql(sourceDDL);
		tEnv.executeSql(sinkDDL);

		try {
			TableEnvUtil.execInsertSqlAndWaitResult(
				tEnv,
				"INSERT INTO sink SELECT name, SUM(weight) FROM debezium_source GROUP BY name");
		} catch (Throwable t) {
			// we have to use a specific exception to indicate the job is finished,
			// because the registered Kafka source is infinite.
			if (!isCausedByJobFinished(t)) {
				// re-throw
				throw t;
			}
		}

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

		String[] expected = new String[]{
			"scooter,3.140", "car battery,8.100", "12-pack drill bits,0.800",
			"hammer,2.625", "rocks,5.100", "jacket,0.600", "spare tire,22.200"};

		List<String> actual = TestValuesTableFactory.getResults("sink");
		assertThat(actual, containsInAnyOrder(expected));

		// ------------- cleanup -------------------

		deleteTestTopic(topic);
	}

	// --------------------------------------------------------------------------------------------
	// Utilities
	// --------------------------------------------------------------------------------------------

	private static List<String> readLines(String resource) throws IOException {
		final URL url = KafkaTableITCase.class.getClassLoader().getResource(resource);
		assert url != null;
		Path path = new File(url.getFile()).toPath();
		return Files.readAllLines(path);
	}
}
