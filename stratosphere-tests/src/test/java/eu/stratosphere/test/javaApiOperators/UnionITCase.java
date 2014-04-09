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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.FilterFunction;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.javaApiOperators.util.CollectionDataSets;
import eu.stratosphere.test.util.JavaProgramTestBase;

@RunWith(Parameterized.class)
public class UnionITCase extends JavaProgramTestBase {
	
	private static int NUM_PROGRAMS = 3; 
	
	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;

	public UnionITCase(Configuration config) {
		super(config);	
	}
	
	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		expectedResult = FilterProgs.runProgram(curProgId, resultPath);
	}
	
	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(expectedResult, resultPath);
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
	
	private static class FilterProgs {

		private static final String FULL_TUPLE_3_STRING = "1,1,Hi\n" +
				"2,2,Hello\n" +
				"3,2,Hello world\n" +
				"4,3,Hello world, how are you?\n" +
				"5,3,I am fine.\n" +
				"6,3,Luke Skywalker\n" +
				"7,4,Comment#1\n" +
				"8,4,Comment#2\n" +
				"9,4,Comment#3\n" +
				"10,4,Comment#4\n" +
				"11,5,Comment#5\n" +
				"12,5,Comment#6\n" +
				"13,5,Comment#7\n" +
				"14,5,Comment#8\n" +
				"15,5,Comment#9\n" +
				"16,6,Comment#10\n" +
				"17,6,Comment#11\n" +
				"18,6,Comment#12\n" +
				"19,6,Comment#13\n" +
				"20,6,Comment#14\n" +
				"21,6,Comment#15\n";
		
		public static String runProgram(int progId, String resultPath) throws Exception {
			
			switch(progId) {
			case 1: {
				/*
				 * Union of 2 Same Data Sets
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> unionDs = ds.union(CollectionDataSets.get3TupleDataSet(env));
				
				unionDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return FULL_TUPLE_3_STRING + FULL_TUPLE_3_STRING;
			}
			case 2: {
				/*
				 * Union of 5 same Data Sets, with multiple unions
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> unionDs = ds.union(CollectionDataSets.get3TupleDataSet(env))
						.union(CollectionDataSets.get3TupleDataSet(env))
						.union(CollectionDataSets.get3TupleDataSet(env))
						.union(CollectionDataSets.get3TupleDataSet(env));
				
				unionDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return FULL_TUPLE_3_STRING + FULL_TUPLE_3_STRING + FULL_TUPLE_3_STRING + FULL_TUPLE_3_STRING + FULL_TUPLE_3_STRING;
			}
			case 3: {
				/*
				 * Test on union with empty dataset
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				// Don't know how to make an empty result in an other way than filtering it 
				DataSet<Tuple3<Integer, Long, String>> empty = CollectionDataSets.get3TupleDataSet(env).
						filter(new FilterFunction<Tuple3<Integer,Long,String>>() {
							private static final long serialVersionUID = 1L;

							@Override
							public boolean filter(Tuple3<Integer, Long, String> value) throws Exception {
								return false;
							}
						});
				
				DataSet<Tuple3<Integer, Long, String>> unionDs = CollectionDataSets.get3TupleDataSet(env)
					.union(empty);
			
				unionDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return FULL_TUPLE_3_STRING;				
			}
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
			
		}
	
	}
	
}
