/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.test.javaApiOperators;


import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.javaApiOperators.util.CollectionDataSets;
import eu.stratosphere.test.util.JavaProgramTestBase;
import org.junit.runners.Parameterized;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

public class SumMinMaxITCase extends JavaProgramTestBase  {

	private static int NUM_PROGRAMS = 3;

	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;

	public SumMinMaxITCase(Configuration config) {
		super(config);
	}

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		expectedResult = SumMinMaxProgs.runProgram(curProgId, resultPath);
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(expectedResult, resultPath);
	}

	@Parameterized.Parameters
	public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		for(int i=1; i <= NUM_PROGRAMS; i++) {
			Configuration config = new Configuration();
			config.setInteger("ProgramId", i);
			tConfigs.add(config);
		}

		return toParameterList(tConfigs);
	}

	/**
	 * These tests are copied from
	 * @see eu.stratosphere.test.javaApiOperators.AggregateITCase
	 * replacing calls to aggregate with calls to sum, min, and max
	 */
	private static class SumMinMaxProgs {

		public static String runProgram(int progId, String resultPath) throws Exception {
			switch(progId) {
				case 1: {
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

					DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
					DataSet<Tuple2<Integer, Long>> sumDs = ds
							.sum(0)
							.andMax(1)
							.project(0, 1).types(Integer.class, Long.class);

					sumDs.writeAsCsv(resultPath);
					env.execute();

					// return expected result
					return "231,6\n";
				}
				case 2: {
				/*
				 * Grouped Aggregate
				 */

					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

					DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
					DataSet<Tuple2<Long, Integer>> aggregateDs = ds.groupBy(1)
							.sum(0)
							.project(1, 0).types(Long.class, Integer.class);

					aggregateDs.writeAsCsv(resultPath);
					env.execute();

					// return expected result
					return "1,1\n" +
							"2,5\n" +
							"3,15\n" +
							"4,34\n" +
							"5,65\n" +
							"6,111\n";
				}
				case 3: {
				/*
				 * Nested Aggregate
				 */

					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

					DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
					DataSet<Tuple1<Integer>> aggregateDs = ds.groupBy(1)
							.min(0)
							.min(0)
							.project(0).types(Integer.class);

					aggregateDs.writeAsCsv(resultPath);
					env.execute();

					// return expected result
					return "1\n";
				}
				default:
					throw new IllegalArgumentException("Invalid program id");
			}
		}
	}
}
