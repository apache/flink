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

package org.apache.flink.test.javaApiOperators;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets.POJO;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.Collector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class PartitionITCase extends JavaProgramTestBase {
	
	private static int NUM_PROGRAMS = 4;
	
	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;
	
	public PartitionITCase(Configuration config) {
		super(config);	
	}
	
	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		expectedResult = PartitionProgs.runProgram(curProgId, resultPath);
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
	
	private static class PartitionProgs {
		
		public static String runProgram(int progId, String resultPath) throws Exception {
			
			switch(progId) {
			case 0: {
				/*
				 * Test hash partition by key field
				 */
		
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Long> uniqLongs = ds
						.partitionByHash(1)
						.mapPartition(new UniqueLongMapper());
				uniqLongs.writeAsText(resultPath);
				env.execute();
				
				// return expected result
				return 	"1\n" +
						"2\n" +
						"3\n" +
						"4\n" +
						"5\n" +
						"6\n";
			}
			case 1: {
				/*
				 * Test hash partition by key selector
				 */
		
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Long> uniqLongs = ds
						.partitionByHash(new KeySelector<Tuple3<Integer,Long,String>, Long>() {
							private static final long serialVersionUID = 1L;

							@Override
							public Long getKey(Tuple3<Integer, Long, String> value) throws Exception {
								return value.f1;
							}
							
						})
						.mapPartition(new UniqueLongMapper());
				uniqLongs.writeAsText(resultPath);
				env.execute();
				
				// return expected result
				return 	"1\n" +
						"2\n" +
						"3\n" +
						"4\n" +
						"5\n" +
						"6\n";
			}
			case 2: {
				/*
				 * Test forced rebalancing
				 */
		
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

				// generate some number in parallel
				DataSet<Long> ds = env.generateSequence(1,3000);
				DataSet<Tuple2<Integer, Integer>> uniqLongs = ds
						// introduce some partition skew by filtering
						.filter(new FilterFunction<Long>() {
							private static final long serialVersionUID = 1L;

							@Override
							public boolean filter(Long value) throws Exception {
								if (value <= 780) {
									return false;
								} else {
									return true;
								}
							}
						})
						// rebalance
						.rebalance()
						// count values in each partition
						.map(new PartitionIndexMapper())
						.groupBy(0)
						.reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
							private static final long serialVersionUID = 1L;

							public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) {
								return new Tuple2<Integer, Integer>(v1.f0, v1.f1+v2.f1);
							}
						})
						// round counts to mitigate runtime scheduling effects (lazy split assignment)
						.map(new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>(){
							private static final long serialVersionUID = 1L;

							@Override
							public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
								value.f1 = (value.f1 / 10);
								return value;
							}
							
						});
				
				uniqLongs.writeAsText(resultPath);
				
				env.execute();
				
				StringBuilder result = new StringBuilder();
				int numPerPartition = 2220 / env.getDegreeOfParallelism() / 10;
				for (int i = 0; i < env.getDegreeOfParallelism(); i++) {
					result.append('(').append(i).append(',').append(numPerPartition).append(")\n");
				}
				// return expected result
				return result.toString();
			}
			case 3: {
				/*
				 * Test hash partition by key field and different DOP
				 */
		
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				env.setDegreeOfParallelism(3);
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Long> uniqLongs = ds
						.partitionByHash(1).setParallelism(4)
						.mapPartition(new UniqueLongMapper());
				uniqLongs.writeAsText(resultPath);
				
				env.execute();
				
				// return expected result
				return 	"1\n" +
						"2\n" +
						"3\n" +
						"4\n" +
						"5\n" +
						"6\n";
			}
			case 4: {
				/*
				 * Test hash partition with key expression
				 */
		
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				env.setDegreeOfParallelism(3);
				
				DataSet<POJO> ds = CollectionDataSets.getDuplicatePojoDataSet(env);
				DataSet<Long> uniqLongs = ds
						.partitionByHash("nestedPojo.longNumber").setParallelism(4)
						.mapPartition(new UniqueNestedPojoLongMapper());
				uniqLongs.writeAsText(resultPath);
				
				env.execute();
				
				// return expected result
				return 	"10000\n" +
						"20000\n" +
						"30000\n";
			}
			
			
			
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
		}
	}
	
	public static class UniqueLongMapper implements MapPartitionFunction<Tuple3<Integer,Long,String>, Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public void mapPartition(Iterable<Tuple3<Integer, Long, String>> records, Collector<Long> out) throws Exception {
			HashSet<Long> uniq = new HashSet<Long>();
			for(Tuple3<Integer,Long,String> t : records) {
				uniq.add(t.f1);
			}
			for(Long l : uniq) {
				out.collect(l);
			}
		}
	}
	
	public static class UniqueNestedPojoLongMapper implements MapPartitionFunction<POJO, Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public void mapPartition(Iterable<POJO> records, Collector<Long> out) throws Exception {
			HashSet<Long> uniq = new HashSet<Long>();
			for(POJO t : records) {
				uniq.add(t.nestedPojo.longNumber);
			}
			for(Long l : uniq) {
				out.collect(l);
			}
		}
	}
	
	public static class PartitionIndexMapper extends RichMapFunction<Long, Tuple2<Integer, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Integer, Integer> map(Long value) throws Exception {
			return new Tuple2<Integer, Integer>(this.getRuntimeContext().getIndexOfThisSubtask(), 1);
		}
	}
}
