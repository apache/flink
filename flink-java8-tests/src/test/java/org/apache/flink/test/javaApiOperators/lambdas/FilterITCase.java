/**
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

package org.apache.flink.test.javaApiOperators.lambdas;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

@SuppressWarnings("serial")
public class FilterITCase extends JavaProgramTestBase {

	private static final String EXPECTED_RESULT = "3,2,Hello world\n" +
													"4,3,Hello world, how are you?\n";

	public static DataSet<Tuple3<Integer, Long, String>> get3TupleDataSet(ExecutionEnvironment env) {

		List<Tuple3<Integer, Long, String>> data = new ArrayList<Tuple3<Integer, Long, String>>();
		data.add(new Tuple3<Integer, Long, String>(1,1l,"Hi"));
		data.add(new Tuple3<Integer, Long, String>(2,2l,"Hello"));
		data.add(new Tuple3<Integer, Long, String>(3,2l,"Hello world"));
		data.add(new Tuple3<Integer, Long, String>(4,3l,"Hello world, how are you?"));
		data.add(new Tuple3<Integer, Long, String>(5,3l,"I am fine."));
		data.add(new Tuple3<Integer, Long, String>(6,3l,"Luke Skywalker"));
		data.add(new Tuple3<Integer, Long, String>(7,4l,"Comment#1"));
		data.add(new Tuple3<Integer, Long, String>(8,4l,"Comment#2"));
		data.add(new Tuple3<Integer, Long, String>(9,4l,"Comment#3"));
		data.add(new Tuple3<Integer, Long, String>(10,4l,"Comment#4"));
		data.add(new Tuple3<Integer, Long, String>(11,5l,"Comment#5"));
		data.add(new Tuple3<Integer, Long, String>(12,5l,"Comment#6"));
		data.add(new Tuple3<Integer, Long, String>(13,5l,"Comment#7"));
		data.add(new Tuple3<Integer, Long, String>(14,5l,"Comment#8"));
		data.add(new Tuple3<Integer, Long, String>(15,5l,"Comment#9"));
		data.add(new Tuple3<Integer, Long, String>(16,6l,"Comment#10"));
		data.add(new Tuple3<Integer, Long, String>(17,6l,"Comment#11"));
		data.add(new Tuple3<Integer, Long, String>(18,6l,"Comment#12"));
		data.add(new Tuple3<Integer, Long, String>(19,6l,"Comment#13"));
		data.add(new Tuple3<Integer, Long, String>(20,6l,"Comment#14"));
		data.add(new Tuple3<Integer, Long, String>(21,6l,"Comment#15"));

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

