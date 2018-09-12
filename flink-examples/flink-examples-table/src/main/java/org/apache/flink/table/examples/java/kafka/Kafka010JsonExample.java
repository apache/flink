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

package org.apache.flink.table.examples.java.kafka;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSink;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSink;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
 * An example that shows how to read from and write to Kafka table rows as JSON-encoded records.
 *
 * <p>This example shows how to:
 *  - Use Kafka JSON TableSource
 *  - Use SQL api and a group window to compute a result
 *
 * <p>Example usage:
 * Run Consumer with the following arguments:
 * 	--input-topic consumer-input --output-topic consumer-output --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id myconsumer
 */
public class Kafka010JsonExample {
	private static final String TABLE_NAME = "tableName";

	static final String WORD_COL = "word";
	static final String FREQUENCY_COL = "frequency";
	static final String TIMESTAMP_COL = "currentTimestamp";

	public static void main(String[] args) throws Exception {
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);

		if (parameterTool.getNumberOfParameters() < 5) {
			System.out.println("Missing parameters!\n" +
				"Usage: Kafka --input-topic <topic> --output-topic <topic> " +
				"--bootstrap.servers <kafka brokers> " +
				"--zookeeper.connect <zk quorum> --group.id <some id>");
			return;
		}

		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnvironment = StreamTableEnvironment.getTableEnvironment(environment);

		tableEnvironment
			.connect(
				new Kafka()
					.version("0.10")
					.topic(parameterTool.getRequired("input-topic"))
					.properties(parameterTool.getProperties())
			)
			.withFormat(
				new Json()
					.deriveSchema()
			)
			.withSchema(new Schema()
				.field(WORD_COL, Types.STRING)
				.field(FREQUENCY_COL, Types.INT)
				.field(TIMESTAMP_COL, Types.SQL_TIMESTAMP)
					.proctime()
			)
			.inAppendMode()
			.registerTableSource(TABLE_NAME);

		// run a SQL query on the Table and retrieve the result as a new Table
		Table table = tableEnvironment.sqlQuery("" +
			"SELECT " + WORD_COL + ", SUM(" + FREQUENCY_COL + ") as " + FREQUENCY_COL + " " +
			"FROM " + TABLE_NAME + " " +
			"GROUP BY TUMBLE(" + TIMESTAMP_COL + ", INTERVAL '30' second), " + WORD_COL);

		KafkaTableSink sink = new Kafka010JsonTableSink(
			parameterTool.getRequired("output-topic"),
			parameterTool.getProperties());

		table.writeToSink(sink);

		environment.execute();
	}
}
