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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Simple example that shows how to use the Stream SQL API to calculate pv and uv
 * for website visits.
 *
 * <p>It also shows how to use toRetractStream.
 *
 * <p>Usage: <code>RetractPvUvSQL --output &lt;path&gt;</code><br>
 * If no parameters are provided, the program will print result to stdout.
 *
 */
public class RetractPvUvSQL {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

		DataStreamSource<PageVisit> input = env.fromElements(
			new PageVisit("2017-09-16 09:00:00", 1001, "/page1"),
			new PageVisit("2017-09-16 09:00:00", 1001, "/page2"),

			new PageVisit("2017-09-16 10:30:00", 1005, "/page1"),
			new PageVisit("2017-09-16 10:30:00", 1005, "/page1"),
			new PageVisit("2017-09-16 10:30:00", 1005, "/page2"));

		// register the DataStream as table "visit_table"
		tEnv.registerDataStream("visit_table", input, "visitTime, userId, visitPage");

		// run a SQL query on the Table
		Table table = tEnv.sqlQuery(
			"SELECT " +
				"visitTime, " +
				"DATE_FORMAT(max(visitTime), 'HH') as ts, " +
				"count(userId) as pv, " +
				"count(distinct userId) as uv " +
			"FROM visit_table " +
			"GROUP BY visitTime");

		DataStream<Tuple2<Boolean, Row>> dataStream = tEnv.toRetractStream(table, Row.class);

		if (params.has("output")) {
			String outPath = params.get("output");
			System.out.println("Output path: " + outPath);
			dataStream.writeAsCsv(outPath);
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			dataStream.print();
		}
		env.execute();
	}

	// *************************************************************************
	//     USER DATA TYPES
	// *************************************************************************

	/**
	 * Simple POJO containing a website page visitor.
	 */
	public static class PageVisit {
		public String visitTime;
		public long userId;
		public String visitPage;

		// public constructor to make it a Flink POJO
		public PageVisit() {}

		public PageVisit(String visitTime, long userId, String visitPage) {
			this.visitTime = visitTime;
			this.userId = userId;
			this.visitPage = visitPage;
		}

		@Override
		public String toString() {
			return "PageVisit " + visitTime  + " " + userId + " " + visitPage;
		}
	}
}
