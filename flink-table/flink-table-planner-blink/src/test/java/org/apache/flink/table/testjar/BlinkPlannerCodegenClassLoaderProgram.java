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

package org.apache.flink.table.testjar;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;

/**
 * A test program for blink planner generate job graph.
 */
public class BlinkPlannerCodegenClassLoaderProgram {

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			throw new IllegalArgumentException("Missing parameters. Expected: <outputFileCsvReaderITCase> ");
		}
		String outputFile = args[0];
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings envSet = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, envSet);
		env.enableCheckpointing(2 * 60 * 1000);

		TableConfig config = tableEnv.getConfig();
		config.setIdleStateRetentionTime(Time.hours(24),
			Time.hours(25));

		SingleOutputStreamOperator<CustomerPojo> source = env.fromElements(new CustomerPojo(1));

		tableEnv.createTemporaryView("customer_pojo_table", source, "id");
		Table table = tableEnv.sqlQuery("select id from customer_pojo_table");

		tableEnv.connect(new org.apache.flink.table.descriptors.FileSystem()
			.path(outputFile)).withFormat(new OldCsv()).withSchema(new Schema()
			.field("id", "INT")).createTemporaryTable("customer_pojo_sink");
		table.insertInto("customer_pojo_sink");

		env.execute("Blink_Codegen_Class_Loader_Test_Job");
	}

	/**
	 * a customer pojo will be loaded by UserCodeClassLoader.
	 */
	public static class CustomerPojo {

		private int id;

		public CustomerPojo() {
		}

		public CustomerPojo(int id) {
			this.id = id;
		}

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}
	}
}
