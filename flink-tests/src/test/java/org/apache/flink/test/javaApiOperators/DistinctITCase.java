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

package org.apache.flink.test.javaApiOperators;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets.CustomType;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@SuppressWarnings("serial")
@RunWith(Parameterized.class)
public class DistinctITCase extends JavaProgramTestBase {
	
	private static int NUM_PROGRAMS = 6;
	
	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;
	
	public DistinctITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		expectedResult = DistinctProgs.runProgram(curProgId, resultPath);
	}
	
	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(expectedResult, resultPath);
	}
	
	@Parameters
	public static Collection<Object[]> getConfigurations() throws IOException {
		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		for(int i=1; i <= NUM_PROGRAMS; i++) {
			Configuration config = new Configuration();
			config.setInteger("ProgramId", i);
			tConfigs.add(config);
		}
		
		return toParameterList(tConfigs);
	}
	
	private static class DistinctProgs {
		
		public static String runProgram(int progId, String resultPath) throws Exception {
			
			switch(progId) {
			case 1: {
				
				/*
				 * check correctness of distinct on tuples with key field selector
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.getSmall3TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> distinctDs = ds.union(ds).distinct(0, 1, 2);
				
				distinctDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,1,Hi\n" +
						"2,2,Hello\n" +
						"3,2,Hello world\n";
			}
			case 2: {
				
				/*
				 * check correctness of distinct on tuples with key field selector with not all fields selected
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple5<Integer, Long,  Integer, String, Long>> ds = CollectionDataSets.getSmall5TupleDataSet(env);
				DataSet<Tuple1<Integer>> distinctDs = ds.union(ds).distinct(0).project(0).types(Integer.class);
				
				distinctDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1\n" +
						"2\n";
			}
			case 3: {
				
				/*
				 * check correctness of distinct on tuples with key extractor
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple5<Integer, Long,  Integer, String, Long>> ds = CollectionDataSets.getSmall5TupleDataSet(env);
				DataSet<Tuple1<Integer>> reduceDs = ds.union(ds)
						.distinct(new KeySelector<Tuple5<Integer, Long,  Integer, String, Long>, Integer>() {
									private static final long serialVersionUID = 1L;
									@Override
									public Integer getKey(Tuple5<Integer, Long,  Integer, String, Long> in) {
										return in.f0;
									}
								}).project(0).types(Integer.class);
				
				reduceDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1\n" +
						"2\n";
								
			}
			case 4: {
				
				/*
				 * check correctness of distinct on custom type with type extractor
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<CustomType> ds = CollectionDataSets.getCustomTypeDataSet(env);
				DataSet<Tuple1<Integer>> reduceDs = ds
						.distinct(new KeySelector<CustomType, Integer>() {
									private static final long serialVersionUID = 1L;
									@Override
									public Integer getKey(CustomType in) {
										return in.myInt;
									}
								})
						.map(new RichMapFunction<CustomType, Tuple1<Integer>>() {
							@Override
							public Tuple1<Integer> map(CustomType value) throws Exception {
								return new Tuple1<Integer>(value.myInt);
							}
						});
				
				reduceDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1\n" +
						"2\n" +
						"3\n" +
						"4\n" +
						"5\n" +
						"6\n";
				
			}
			case 5: {
				
				/*
				 * check correctness of distinct on tuples
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.getSmall3TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> distinctDs = ds.union(ds).distinct();
				
				distinctDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,1,Hi\n" +
						"2,2,Hello\n" +
						"3,2,Hello world\n";
			}
			case 6: {
				
				/*
				 * check correctness of distinct on custom type with tuple-returning type extractor
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = CollectionDataSets.get5TupleDataSet(env);
				DataSet<Tuple2<Integer, Long>> reduceDs = ds
						.distinct(new KeySelector<Tuple5<Integer, Long, Integer, String, Long>, Tuple2<Integer, Long>>() {
									private static final long serialVersionUID = 1L;
									@Override
									public Tuple2<Integer,Long> getKey(Tuple5<Integer, Long, Integer, String, Long> t) {
										return new Tuple2<Integer, Long>(t.f0, t.f4);
									}
								})
						.project(0,4).types(Integer.class, Long.class);
				
				reduceDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,1\n" +
						"2,1\n" +
						"2,2\n" +
						"3,2\n" +
						"3,3\n" +
						"4,1\n" +
						"4,2\n" +
						"5,1\n" +
						"5,2\n" +
						"5,3\n";
			}
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
		}
	}
}
