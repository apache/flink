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

import static org.apache.flink.api.java.aggregation.Aggregations.key;
import static org.apache.flink.api.java.aggregation.Aggregations.max;
import static org.apache.flink.api.java.aggregation.Aggregations.average;
import static org.apache.flink.api.java.aggregation.Aggregations.count;
import static org.apache.flink.api.java.aggregation.Aggregations.min;
import static org.apache.flink.api.java.aggregation.Aggregations.sum;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class AggregateITCase extends JavaProgramTestBase {
	
	private static int NUM_PROGRAMS = 3;
	
	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;
	
	public AggregateITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		expectedResult = AggregateProgs.runProgram(curProgId, resultPath);
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
	
	private static class AggregateProgs {
		
		public static String runProgram(int progId, String resultPath) throws Exception {
			
			switch(progId) {
			case 1: {
				/*
				 * Full Aggregate
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple2<Integer, Long>> aggregateDs = ds
						.aggregate(count(), sum(0), min(0), max(0), average(0), sum(1), min(1), max(1), average(1), min(2), max(2));
				
				aggregateDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "21,231,1,21,11.0,91,1,6,4.333333333333333,Comment#1,Luke Skywalker\n";
			}
			case 2: {
				/*
				 * Grouped Aggregate
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple2<Long, Integer>> aggregateDs = ds.groupBy(1)
						.aggregate(key(1), count(), sum(0), min(0), max(0), average(0), min(2), max(2));
				
				aggregateDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,1,1,1,1,1.0,Hi,Hi\n" +
				"2,2,5,2,3,2.5,Hello,Hello world\n" +
				"3,3,15,4,6,5.0,Hello world, how are you?,Luke Skywalker\n" +
				"4,4,34,7,10,8.5,Comment#1,Comment#4\n" +
				"5,5,65,11,15,13.0,Comment#5,Comment#9\n" +
				"6,6,111,16,21,18.5,Comment#10,Comment#15\n";
			} 
			case 3: {
				/*
				 * Nested Aggregate
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple1<Integer>> aggregateDs = ds.groupBy(1)
						.aggregate(count(), min(0), max(0), sum(0), average(0))
						.aggregate(sum(0), sum(1), sum(2), sum(3), sum(4));
				
				aggregateDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "21,41,56,231,48.5\n";
			}
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
		}
	}
}
