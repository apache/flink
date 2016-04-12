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
package org.apache.flink.examples.java;

import org.apache.flink.api.table.Table;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.BatchTableEnvironment;
import org.apache.flink.api.table.TableEnvironment;

/**
 * Very simple example that shows how the Java Table API can be used.
 */
public class JavaTableExample {

	public static class WC {
		public String word;
		public long count;

		// Public constructor to make it a Flink POJO
		public WC() {

		}

		public WC(String word, long count) {
			this.word = word;
			this.count = count;
		}

		@Override
		public String toString() {
			return "WC " + word + " " + count;
		}
	}

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<WC> input = env.fromElements(
				new WC("Hello", 1),
				new WC("Ciao", 1),
				new WC("Hello", 1));

		Table table = tableEnv.fromDataSet(input);

		Table filtered = table
				.groupBy("word")
				.select("word.count as count, word")
				.filter("count = 2");

		DataSet<WC> result = tableEnv.toDataSet(filtered, WC.class);

		result.print();
	}
}
