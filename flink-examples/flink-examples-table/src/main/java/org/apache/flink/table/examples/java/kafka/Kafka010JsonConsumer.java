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
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSource;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;

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
public class Kafka010JsonConsumer {
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

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.getTableEnvironment(env);

		Kafka010JsonTableSource sensorSource = Kafka010JsonTableSource.builder()
			.withKafkaProperties(parameterTool.getProperties())
			.forTopic(parameterTool.getRequired("input-topic"))
			.withSchema(TableSchema.builder()
				.field(WORD_COL, Types.STRING)
				.field(FREQUENCY_COL, Types.INT)
				.field(TIMESTAMP_COL, Types.SQL_TIMESTAMP)
				.build())
			.withProctimeAttribute(TIMESTAMP_COL)
			.build();
		tEnv.registerTableSource(TABLE_NAME, sensorSource);

		// run a SQL query on the Table and retrieve the result as a new Table
		Table table = tEnv.sqlQuery("" +
			"SELECT " + WORD_COL + ", SUM(" + FREQUENCY_COL + ") as " + FREQUENCY_COL + " " +
			"FROM " + TABLE_NAME + " " +
			"GROUP BY TUMBLE(" + TIMESTAMP_COL + ", INTERVAL '30' second), " + WORD_COL);

		Kafka010JsonTableSink sink = new Kafka010JsonTableSink(
			parameterTool.getRequired("output-topic"),
			parameterTool.getProperties());

		table.writeToSink(sink);

		env.execute();
	}
}
