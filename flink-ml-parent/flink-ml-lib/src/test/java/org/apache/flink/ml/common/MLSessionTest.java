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
 * MLSessionTest.
 */
public class MLSessionTest {
	@Test
	public void setTableEnvironment() {
		synchronized (MLSession.class) {
			// set up execution environment
			ExecutionEnvironment env = MLSession.getExecutionEnvironment();
			MLSession.setExecutionEnvironment(env);

			BatchTableEnvironment tEnv = MLSession.getBatchTableEnvironment();

			DataSet <String> input = env.fromElements("a");

			// register the DataSet as table "TestStringDataSet"
			tEnv.registerDataSet("TestStringDataSet", input, "word");

			// run a SQL query on the Table and retrieve the result as a new Table
			Table table = tEnv.sqlQuery("SELECT word FROM TestStringDataSet");

			MLSession.setTableEnvironment(tEnv, table);

			MLSession.getBatchTableEnvironment().registerDataSet("TestStringDataSetB", input, "word");
			Table tableB = tEnv.sqlQuery("SELECT word FROM TestStringDataSetB");

			MLSession.setTableEnvironment(tEnv, tableB);
		}
	}

	@Test
	public void setStreamTableEnvironment() {
		synchronized (MLSession.class) {
			// set up execution environment
			StreamExecutionEnvironment env = MLSession.getStreamExecutionEnvironment();
			MLSession.setStreamExecutionEnvironment(env);

			StreamTableEnvironment tEnv = MLSession.getStreamTableEnvironment();

			DataStream <String> orderA = env.fromCollection(Arrays.asList("beer", "pen"));

			// register the DataStream as table "OrderA"
			tEnv.registerDataStream("OrderA", orderA, "product");

			// run a SQL query on the Table and retrieve the result as a new Table
			Table table = tEnv.sqlQuery("SELECT * FROM OrderA");

			MLSession.setTableEnvironment(tEnv, table);

			MLSession.getStreamTableEnvironment().registerDataStream("OrderB", orderA, "product");

			Table tableB = tEnv.sqlQuery("SELECT * FROM OrderA");

			MLSession.setTableEnvironment(tEnv, tableB);
		}
	}
}
