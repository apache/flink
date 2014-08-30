/**
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

package org.apache.flink.streaming.examples.join;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JoinLocal {

	private static final int PARALLELISM = 1;
	private static final int SOURCE_PARALLELISM = 1;

	// This example will join two streams. One which emits people's grades and
	// one which emits people's salaries.

	public static void main(String[] args) {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(
				PARALLELISM).setBufferTimeout(100);

		DataStream<Tuple2<String, Integer>> grades = env.addSource(new GradeSource(),
				SOURCE_PARALLELISM);
		
		DataStream<Tuple2<String, Integer>> salaries = env.addSource(new SalarySource(),
				SOURCE_PARALLELISM);

		DataStream<Tuple3<String, Integer, Integer>> joinedStream = grades.connect(salaries)
				.flatMap(new JoinTask());
		
		System.out.println("(NAME, GRADE, SALARY)");
		joinedStream.print();

		env.execute();

	}
}
