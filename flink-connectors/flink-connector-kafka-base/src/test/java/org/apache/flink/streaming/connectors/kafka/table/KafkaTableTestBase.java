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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaTestBase;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.runtime.utils.TableEnvUtil;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Basic Tests for Kafka connector for Table API & SQL.
 */
@RunWith(Parameterized.class)
public abstract class KafkaTableTestBase extends KafkaTestBase {

	@Parameterized.Parameter
	public boolean isLegacyConnector;

	@Parameterized.Parameter(1)
	public int topicID;

	@Parameterized.Parameters(name = "legacy = {0}, topicId = {1}")
	public static Object[] parameters() {
		return new Object[][]{
				new Object[]{true, 0},
				new Object[]{false, 1}
		};
	}

	public abstract String factoryIdentifier();

	// Used for legacy planner.
	public abstract String kafkaVersion();

	@Test
	public void testKafkaSourceSink() throws Exception {
		final String topic = "tstopic" + topicID;
		createTestTopic(topic, 1, 1);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(
			env,
			EnvironmentSettings.newInstance()
				// Watermark is only supported in blink planner
				.useBlinkPlanner()
				.inStreamingMode()
				.build()
		);
		env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		env.setParallelism(1);

		// ---------- Produce an event time stream into Kafka -------------------
		String groupId = standardProps.getProperty("group.id");
		String bootstraps = standardProps.getProperty("bootstrap.servers");

		final String createTable;
		if (!isLegacyConnector) {
			createTable = String.format(
					"create table kafka (\n" +
					"  `computed-price` as price + 1.0,\n" +
					"  price decimal(38, 18),\n" +
					"  currency string,\n" +
					"  log_ts timestamp(3),\n" +
					"  ts as log_ts + INTERVAL '1' SECOND,\n" +
					"  watermark for ts as ts\n" +
					") with (\n" +
					"  'connector' = '%s',\n" +
					"  'topic' = '%s',\n" +
					"  'properties.bootstrap.servers' = '%s',\n" +
					"  'properties.group.id' = '%s',\n" +
					"  'scan.startup.mode' = 'earliest-offset',\n" +
					"  'format' = 'json'\n" +
					")",
				factoryIdentifier(),
				topic,
				bootstraps,
				groupId);
		} else {
			createTable = String.format(
					"create table kafka (\n" +
					"  `computed-price` as price + 1.0,\n" +
					"  price decimal(38, 18),\n" +
					"  currency string,\n" +
					"  log_ts timestamp(3),\n" +
					"  ts as log_ts + INTERVAL '1' SECOND,\n" +
					"  watermark for ts as ts\n" +
					") with (\n" +
					"  'connector.type' = 'kafka',\n" +
					"  'connector.version' = '%s',\n" +
					"  'connector.topic' = '%s',\n" +
					"  'connector.properties.bootstrap.servers' = '%s',\n" +
					"  'connector.properties.group.id' = '%s',\n" +
					"  'connector.startup-mode' = 'earliest-offset',\n" +
					"  'format.type' = 'json',\n" +
					"  'update-mode' = 'append'\n" +
					")",
				kafkaVersion(),
				topic,
				bootstraps,
				groupId);
		}

		tEnv.executeSql(createTable);

		String initialValues = "INSERT INTO kafka\n" +
			"SELECT CAST(price AS DECIMAL(10, 2)), currency, CAST(ts AS TIMESTAMP(3))\n" +
			"FROM (VALUES (2.02,'Euro','2019-12-12 00:00:00.001001'), \n" +
			"  (1.11,'US Dollar','2019-12-12 00:00:01.002001'), \n" +
			"  (50,'Yen','2019-12-12 00:00:03.004001'), \n" +
			"  (3.1,'Euro','2019-12-12 00:00:04.005001'), \n" +
			"  (5.33,'US Dollar','2019-12-12 00:00:05.006001'), \n" +
			"  (0,'DUMMY','2019-12-12 00:00:10'))\n" +
			"  AS orders (price, currency, ts)";
		TableEnvUtil.execInsertSqlAndWaitResult(tEnv, initialValues);

		// ---------- Consume stream from Kafka -------------------

		String query = "SELECT\n" +
			"  CAST(TUMBLE_END(ts, INTERVAL '5' SECOND) AS VARCHAR),\n" +
			"  CAST(MAX(ts) AS VARCHAR),\n" +
			"  COUNT(*),\n" +
			"  CAST(MAX(price) AS DECIMAL(10, 2))\n" +
			"FROM kafka\n" +
			"GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)";

		DataStream<RowData> result = tEnv.toAppendStream(tEnv.sqlQuery(query), RowData.class);
		TestingSinkFunction sink = new TestingSinkFunction(2);
		result.addSink(sink).setParallelism(1);

		try {
			env.execute("Job_2");
		} catch (Throwable e) {
			// we have to use a specific exception to indicate the job is finished,
			// because the registered Kafka source is infinite.
			if (!isCausedByJobFinished(e)) {
				// re-throw
				throw e;
			}
		}

		List<String> expected = Arrays.asList(
			"+I(2019-12-12 00:00:05.000,2019-12-12 00:00:04.004,3,50.00)",
			"+I(2019-12-12 00:00:10.000,2019-12-12 00:00:06.006,2,5.33)");

		assertEquals(expected, TestingSinkFunction.rows);

		// ------------- cleanup -------------------

		deleteTestTopic(topic);
	}

	private static final class TestingSinkFunction implements SinkFunction<RowData> {

		private static final long serialVersionUID = 455430015321124493L;
		private static List<String> rows = new ArrayList<>();

		private final int expectedSize;

		private TestingSinkFunction(int expectedSize) {
			this.expectedSize = expectedSize;
			rows.clear();
		}

		@Override
		public void invoke(RowData value, Context context) throws Exception {
			rows.add(value.toString());
			if (rows.size() >= expectedSize) {
				// job finish
				throw new JobFinishedException("All records are received, job is finished.");
			}
		}
	}

	private static final class JobFinishedException extends RuntimeException {

		private static final long serialVersionUID = -4684689851069516182L;

		private JobFinishedException(String message) {
			super(message);
		}
	}

	private static boolean isCausedByJobFinished(Throwable e) {
		if (e instanceof JobFinishedException) {
			return true;
		} else if (e.getCause() != null) {
			return isCausedByJobFinished(e.getCause());
		} else {
			return false;
		}
	}

}
