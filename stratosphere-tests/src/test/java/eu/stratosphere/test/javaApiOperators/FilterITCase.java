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
	
	private static int NUM_PROGRAMS = 6; 
	
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
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.getTupleDataSet(env);
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
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.getTupleDataSet(env);
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
				return "1,10,Hi\n" +
						"-1,11,Hello\n" +
						"10,12,Hello world\n" +
						"-10,13,Hello world, how are you?\n" +
						"100,14,I am fine.\n" +
						"1000,15,Luke Skywalker\n" +
						"-1000,16,Random comment\n" +
						"-100,17,LOL\n";
			}
			case 3: {
				/*
				 * Test filter on String tuple field.
				 */
					
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.getTupleDataSet(env);
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
				return "10,12,Hello world\n" +
					   "-10,13,Hello world, how are you?\n";
				
			}
			case 4: {
				/*
				 * Test filter on Integer tuple field.
				 */
					
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.getTupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> filterDs = ds.
						filter(new FilterFunction<Tuple3<Integer,Long,String>>() {
							private static final long serialVersionUID = 1L;

							@Override
							public boolean filter(Tuple3<Integer, Long, String> value) throws Exception {
								return value.f0 < -1;
							}
						});
				filterDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "-10,13,Hello world, how are you?\n" +
					   "-1000,16,Random comment\n" +
					   "-100,17,LOL\n";
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
				return "-10,13,Hello world, how are you?\n" +
					   "100,14,I am fine.\n" +
					   "1000,15,Luke Skywalker\n" +
					   "-1000,16,Random comment\n";
			}
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
			
		}
	
	}
	
}
