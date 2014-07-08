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
import eu.stratosphere.test.javaApiOperators.util.CollectionDataSets.CustomType;
import eu.stratosphere.test.util.JavaProgramTestBase;

@RunWith(Parameterized.class)
public class FilterITCase extends JavaProgramTestBase {
	
	private static int NUM_PROGRAMS = 8; 
	
	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;
	
	public FilterITCase(Configuration config) {
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
		
		public static String runProgram(int progId, String resultPath) throws Exception {
			
			switch(progId) {
			case 1: {
				/*
				 * Test all-rejecting filter.
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> filterDs = ds.
						filter(new FilterFunction<Tuple3<Integer,Long,String>>() {
							private static final long serialVersionUID = 1L;

							@Override
							public boolean filter(Tuple3<Integer, Long, String> value) throws Exception {
								return false;
							}
						});
				
				filterDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "\n";
			}
			case 2: {
				/*
				 * Test all-passing filter.
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> filterDs = ds.
						filter(new FilterFunction<Tuple3<Integer,Long,String>>() {
							private static final long serialVersionUID = 1L;

							@Override
							public boolean filter(Tuple3<Integer, Long, String> value) throws Exception {
								return true;
							}
						});
				filterDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,1,Hi\n" +
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
			}
			case 3: {
				/*
				 * Test filter on String tuple field.
				 */
					
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> filterDs = ds.
						filter(new FilterFunction<Tuple3<Integer,Long,String>>() {
							private static final long serialVersionUID = 1L;

							@Override
							public boolean filter(Tuple3<Integer, Long, String> value) throws Exception {
								return value.f2.contains("world");
							}
						});
				filterDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "3,2,Hello world\n" +
						"4,3,Hello world, how are you?\n";
				
			}
			case 4: {
				/*
				 * Test filter on Integer tuple field.
				 */
					
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> filterDs = ds.
						filter(new FilterFunction<Tuple3<Integer,Long,String>>() {
							private static final long serialVersionUID = 1L;

							@Override
							public boolean filter(Tuple3<Integer, Long, String> value) throws Exception {
								return (value.f0 % 2) == 0;
							}
						});
				filterDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "2,2,Hello\n" +
						"4,3,Hello world, how are you?\n" +
						"6,3,Luke Skywalker\n" +
						"8,4,Comment#2\n" +
						"10,4,Comment#4\n" +
						"12,5,Comment#6\n" +
						"14,5,Comment#8\n" +
						"16,6,Comment#10\n" +
						"18,6,Comment#12\n" +
						"20,6,Comment#14\n";
			}
			case 5: {
				/*
				 * Test filter on basic type
				 */
						
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<String> ds = CollectionDataSets.getStringDataSet(env);
				DataSet<String> filterDs = ds.
						filter(new FilterFunction<String>() {
							private static final long serialVersionUID = 1L;

							@Override
							public boolean filter(String value) throws Exception {
								return value.startsWith("H");
							}
						});
				filterDs.writeAsText(resultPath);
				env.execute();
				
				// return expected result
				return "Hi\n" +
					   "Hello\n" + 
					   "Hello world\n" +
					   "Hello world, how are you?\n";
			}
			case 6: {
				/*
				 * Test filter on custom type
				 */
						
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<CustomType> ds = CollectionDataSets.getCustomTypeDataSet(env);
				DataSet<CustomType> filterDs = ds.
						filter(new FilterFunction<CustomType>() {
							private static final long serialVersionUID = 1L;

							@Override
							public boolean filter(CustomType value) throws Exception {
								return value.myString.contains("a");
							}
						});
				filterDs.writeAsText(resultPath);
				env.execute();
				
				// return expected result
				return "3,3,Hello world, how are you?\n" +
						"3,4,I am fine.\n" +
						"3,5,Luke Skywalker\n";
			}
			case 7: {
				/*
				 * Test filter on String tuple field.
				 */
					
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Integer> ints = CollectionDataSets.getIntegerDataSet(env);
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> filterDs = ds.
						filter(new FilterFunction<Tuple3<Integer,Long,String>>() {
							private static final long serialVersionUID = 1L;

							int literal = -1;
							
							@Override
							public void open(Configuration config) {
								Collection<Integer> ints = this.getRuntimeContext().getBroadcastVariable("ints");
								for(int i: ints) {
									literal = literal < i ? i : literal;
								}
							}
							
							@Override
							public boolean filter(Tuple3<Integer, Long, String> value) throws Exception {
								return value.f0 < literal;
							}
						}).withBroadcastSet(ints, "ints");
				filterDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,1,Hi\n" +
						"2,2,Hello\n" +
						"3,2,Hello world\n" +
						"4,3,Hello world, how are you?\n";
			}
			case 8: {
				/*
				 * Test filter with broadcast variables
				 */
					
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Integer> intDs = CollectionDataSets.getIntegerDataSet(env);
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> filterDs = ds.
						filter(new FilterFunction<Tuple3<Integer,Long,String>>() {
							private static final long serialVersionUID = 1L;
							private  int broadcastSum = 0;
							
							@Override
							public void open(Configuration config) {
								Collection<Integer> ints = this.getRuntimeContext().getBroadcastVariable("ints");
								for(Integer i : ints) {
									broadcastSum += i;
								}
							}

							@Override
							public boolean filter(Tuple3<Integer, Long, String> value) throws Exception {
								return (value.f1 == (broadcastSum / 11));
							}
						}).withBroadcastSet(intDs, "ints");
				filterDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "11,5,Comment#5\n" +
						"12,5,Comment#6\n" +
						"13,5,Comment#7\n" +
						"14,5,Comment#8\n" +
						"15,5,Comment#9\n";
				
			}
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
			
		}
	
	}
	
}
