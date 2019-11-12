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

package org.apache.flink.table.examples.java;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * Simple example that shows how the Batch SQL API is used in Java.
 *
 * <p>This example shows how to:
 *  - Convert DataSets to Tables
 *  - Register a Table under a name
 *  - Run a SQL query on the registered Table
 */
public class WordCountSQL {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

		DataSet<WC> input = env.fromElements(
			new WC("Hello", 1),
			new WC("Ciao", 1),
			new WC("Hello", 1));

		// register the DataSet as table "WordCount"
		tEnv.registerDataSet("WordCount", input, "word, frequency");

		// run a SQL query on the Table and retrieve the result as a new Table
		Table table = tEnv.sqlQuery(
			"SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word");

		DataSet<WC> result = tEnv.toDataSet(table, WC.class);

		result.print();
	}

	// *************************************************************************
	//     USER DATA TYPES
	// *************************************************************************

	/**
	 * Simple POJO containing a word and its respective count.
	 */
	public static class WC {
		public String word;
		public long frequency;

		// public constructor to make it a Flink POJO
		public WC() {}

		public WC(String word, long frequency) {
			this.word = word;
			this.frequency = frequency;
		}

		@Override
		public String toString() {
			return "WC " + word + " " + frequency;
		}
	}
}
