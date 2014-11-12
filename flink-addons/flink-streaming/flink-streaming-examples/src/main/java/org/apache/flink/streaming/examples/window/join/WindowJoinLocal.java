/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.window.join;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WindowJoinLocal {

	// This example will join two streams with a sliding window. One which emits
	// people's grades and one which emits people's salaries.

	public static void main(String[] args) throws Exception {

		// Obtain execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Connect to the data sources for grades and salaries
		DataStream<Tuple2<String, Integer>> grades = env.addSource(new GradeSource());
		DataStream<Tuple2<String, Integer>> salaries = env.addSource(new SalarySource());

		// Apply a temporal join over the two stream based on the names in one
		// second windows
		DataStream<Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> joinedStream = grades
				.windowJoin(salaries, 1000, 1000, 0, 0);

		// Print the results
		joinedStream.print();

		env.execute();

	}
}
