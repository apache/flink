/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import org.junit.Test;

import java.util.Arrays;

/**
 * Test cases for MLEnvironment.
 */
public class MLEnvironmentTest {
	@Test
	public void setTableEnvironment() {
		MLEnvironment mlEnvironment = new MLEnvironment();
		ExecutionEnvironment env = MLEnvironmentFactory.getDefault().getExecutionEnvironment();
		mlEnvironment.setExecutionEnvironment(env);

		BatchTableEnvironment tEnv = MLEnvironmentFactory.getDefault().getBatchTableEnvironment();

		DataSet<String> input = env.fromElements("a");

		// register the DataSet as table "TestStringDataSet"
		tEnv.registerDataSet("TestStringDataSet", input, "word");

		// run a SQL query on the Table and retrieve the result as a new Table
		Table table = tEnv.sqlQuery("SELECT word FROM TestStringDataSet");

		mlEnvironment.setTableEnvironment(tEnv, table);

		MLEnvironmentFactory.getDefault().getBatchTableEnvironment().registerDataSet("TestStringDataSetB", input, "word");
		Table tableB = tEnv.sqlQuery("SELECT word FROM TestStringDataSetB");

		mlEnvironment.setTableEnvironment(tEnv, tableB);
	}

	@Test
	public void setStreamTableEnvironment() {
		// set up execution environment
		MLEnvironment mlEnvironment = new MLEnvironment();
		StreamExecutionEnvironment env = MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment();
		mlEnvironment.setStreamExecutionEnvironment(env);

		StreamTableEnvironment tEnv = MLEnvironmentFactory.getDefault().getStreamTableEnvironment();

		DataStream<String> orderA = env.fromCollection(Arrays.asList("beer", "pen"));

		// register the DataStream as table "OrderA"
		tEnv.registerDataStream("OrderA", orderA, "product");

		// run a SQL query on the Table and retrieve the result as a new Table
		Table table = tEnv.sqlQuery("SELECT * FROM OrderA");

		mlEnvironment.setTableEnvironment(tEnv, table);

		MLEnvironmentFactory.getDefault().getStreamTableEnvironment().registerDataStream("OrderB", orderA, "product");

		Table tableB = tEnv.sqlQuery("SELECT * FROM OrderA");

		mlEnvironment.setTableEnvironment(tEnv, tableB);
	}
}
