/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.operators;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.Collector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * These check whether the object-reuse execution mode does really reuse objects.
 */
@RunWith(Parameterized.class)
public class ObjectReuseITCase extends JavaProgramTestBase {

	private static int NUM_PROGRAMS = 4;

	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;

	private static String inReducePath;
	private static String inGroupReducePath;

	private String IN_REDUCE = "a,1\na,2\na,3\na,4\na,50\n";
	private String IN_GROUP_REDUCE = "a,1\na,2\na,3\na,4\na,5\n";

	public ObjectReuseITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		inReducePath = createTempFile("in_reduce.txt", IN_REDUCE);
		inGroupReducePath = createTempFile("in_group_reduce.txt", IN_GROUP_REDUCE);
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		expectedResult = Progs.runProgram(curProgId, resultPath);
	}
	
	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(expectedResult, resultPath);
	}

	@Override
	protected boolean skipCollectionExecution() {
		return true;
	}

	
	@Parameters
	public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		for(int i=1; i <= NUM_PROGRAMS; i++) {
			Configuration config = new Configuration();
			config.setInteger("ProgramId", i);
			tConfigs.add(config);
		}
		
		return toParameterList(tConfigs);
	}
	
	@SuppressWarnings({"unchecked", "serial"})
	private static class Progs {
		
		public static String runProgram(int progId, String resultPath) throws Exception {
			
			switch(progId) {

			case 1: {

				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				env.getConfig().enableObjectReuse();

				DataSet<Tuple2<String, Integer>> input = env.readCsvFile(inReducePath).types(String.class, Integer.class).setParallelism(1);
				DataSet<Tuple2<String, Integer>> result = input.groupBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {

					@Override
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws
							Exception {
						value2.f1 += value1.f1;
						return value2;
					}

				});

				result.writeAsCsv(resultPath);
				env.execute();

				// return expected result
				return "a,100\n";

			}

			case 2: {

				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				env.getConfig().enableObjectReuse();

				DataSet<Tuple2<String, Integer>> input = env.readCsvFile(inReducePath).types(String.class, Integer.class).setParallelism(1);

				DataSet<Tuple2<String, Integer>> result = input
						.reduce(new ReduceFunction<Tuple2<String, Integer>>() {

							@Override
							public Tuple2<String, Integer> reduce(
									Tuple2<String, Integer> value1,
									Tuple2<String, Integer> value2) throws Exception {
								value2.f1 += value1.f1;
								return value2;
							}

						});

				result.writeAsCsv(resultPath);
				env.execute();

				// return expected result
				return "a,100\n";

			}

			case 3: {

				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				env.getConfig().enableObjectReuse();

				DataSet<Tuple2<String, Integer>> input = env.readCsvFile(inGroupReducePath).types(String.class, Integer.class).setParallelism(1);

				DataSet<Tuple2<String, Integer>> result = input.reduceGroup(new GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

					@Override
					public void reduce(Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out) throws Exception {
						List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
						for (Tuple2<String, Integer> val : values) {
							list.add(val);
						}

						for (Tuple2<String, Integer> val : list) {
							out.collect(val);
						}
					}

				});

				result.writeAsCsv(resultPath);
				env.execute();

				// return expected result
				return "a,4\n" +
						"a,4\n" +
						"a,5\n" +
						"a,5\n" +
						"a,5\n";

			}

			case 4: {

				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				env.getConfig().enableObjectReuse();

				DataSet<Tuple2<String, Integer>> input = env.readCsvFile(inGroupReducePath).types(String.class, Integer.class).setParallelism(1);

				DataSet<Tuple2<String, Integer>> result = input.reduceGroup(new GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

					@Override
					public void reduce(Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out) throws Exception {
						List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
						for (Tuple2<String, Integer> val : values) {
							list.add(val);
						}

						for (Tuple2<String, Integer> val : list) {
							out.collect(val);
						}
					}

				});

				result.writeAsCsv(resultPath);
				env.execute();

				// return expected result
				return "a,4\n" +
						"a,4\n" +
						"a,5\n" +
						"a,5\n" +
						"a,5\n";

			}

			default:
				throw new IllegalArgumentException("Invalid program id");
			}
			
		}
	
	}
}
