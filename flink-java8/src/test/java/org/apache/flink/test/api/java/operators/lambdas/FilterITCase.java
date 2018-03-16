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

package org.apache.flink.test.api.java.operators.lambdas;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.test.util.JavaProgramTestBase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * IT cases for lambda filter functions.
 */
public class FilterITCase extends JavaProgramTestBase {

	private static final String EXPECTED_RESULT = "3,2,Hello world\n" +
			"4,3,Hello world, how are you?\n";

	public static DataSet<Tuple3<Integer, Long, String>> get3TupleDataSet(ExecutionEnvironment env) {

		List<Tuple3<Integer, Long, String>> data = new ArrayList<Tuple3<Integer, Long, String>>();
		data.add(new Tuple3<>(1, 1L, "Hi"));
		data.add(new Tuple3<>(2, 2L, "Hello"));
		data.add(new Tuple3<>(3, 2L, "Hello world"));
		data.add(new Tuple3<>(4, 3L, "Hello world, how are you?"));
		data.add(new Tuple3<>(5, 3L, "I am fine."));
		data.add(new Tuple3<>(6, 3L, "Luke Skywalker"));
		data.add(new Tuple3<>(7, 4L, "Comment#1"));
		data.add(new Tuple3<>(8, 4L, "Comment#2"));
		data.add(new Tuple3<>(9, 4L, "Comment#3"));
		data.add(new Tuple3<>(10, 4L, "Comment#4"));
		data.add(new Tuple3<>(11, 5L, "Comment#5"));
		data.add(new Tuple3<>(12, 5L, "Comment#6"));
		data.add(new Tuple3<>(13, 5L, "Comment#7"));
		data.add(new Tuple3<>(14, 5L, "Comment#8"));
		data.add(new Tuple3<>(15, 5L, "Comment#9"));
		data.add(new Tuple3<>(16, 6L, "Comment#10"));
		data.add(new Tuple3<>(17, 6L, "Comment#11"));
		data.add(new Tuple3<>(18, 6L, "Comment#12"));
		data.add(new Tuple3<>(19, 6L, "Comment#13"));
		data.add(new Tuple3<>(20, 6L, "Comment#14"));
		data.add(new Tuple3<>(21, 6L, "Comment#15"));

		Collections.shuffle(data);

		return env.fromCollection(data);
	}

	private String resultPath;

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = get3TupleDataSet(env);
		DataSet<Tuple3<Integer, Long, String>> filterDs = ds.
				filter(value -> value.f2.contains("world"));
		filterDs.writeAsCsv(resultPath);
		env.execute();
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(EXPECTED_RESULT, resultPath);
	}
}

