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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSink;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSink;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.Arrays;

/**
 * An example that shows how to write table rows as JSON-encoded records to Kafka.
 *
 * <p>Example usage:
 * 1. Run Consumer.
 * 2. Run Producer with the following arguments:
 * 	--output-topic consumer-input --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id myconsumer
 */
public class Kafka010JsonProducer {
	private static final String TABLE_NAME = "producerTable";

	private static final String[] FIELD_NAMES = new String[] {
		Kafka010JsonConsumer.WORD_COL, Kafka010JsonConsumer.FREQUENCY_COL, Kafka010JsonConsumer.TIMESTAMP_COL
	};
	private static final TypeInformation[] FIELD_TYPES = new TypeInformation[] {
		Types.STRING(), Types.INT(), Types.SQL_TIMESTAMP()
	};
	private static final RowTypeInfo ROW_TYPE = new RowTypeInfo(FIELD_TYPES, FIELD_NAMES);

	public static void main(String[] args) throws Exception {
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);

		if (parameterTool.getNumberOfParameters() < 4) {
			System.out.println("Missing parameters!\n" +
				"Usage: Kafka --output-topic <topic> " +
				"--bootstrap.servers <kafka brokers> " +
				"--zookeeper.connect <zk quorum> --group.id <some id>");
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.getTableEnvironment(env);

		long time = System.currentTimeMillis();
		DataStreamSource<Row> source = env.fromCollection(Arrays.asList(
			Row.of("hello", 1, new Timestamp(time)),
			Row.of("world", 1, new Timestamp(time + 1)),
			Row.of("world", 1, new Timestamp(time + 2)),
			Row.of("hello", 1, new Timestamp(time + 3)),
			Row.of("hello", 1, new Timestamp(time + 4))
		), ROW_TYPE);
		tEnv.registerDataStreamInternal(TABLE_NAME, source);

		KafkaTableSink sink = new Kafka010JsonTableSink(
				parameterTool.getRequired("output-topic"),
				parameterTool.getProperties());

		tEnv.scan(TABLE_NAME).writeToSink(sink);

		env.execute();
	}
}
