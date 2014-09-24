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
import java.util.LinkedList;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
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
public class FirstNITCase extends JavaProgramTestBase {
	
	private static int NUM_PROGRAMS = 3;
	
	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;
	
	public FirstNITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		expectedResult = FirstNProgs.runProgram(curProgId, resultPath);
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
	
	private static class FirstNProgs {
		
		public static String runProgram(int progId, String resultPath) throws Exception {
			
			switch(progId) {
			case 1: {
				/*
				 * First-n on ungrouped data set
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple1<Integer>> seven = ds.first(7).map(new OneMapper()).sum(0);
				
				seven.writeAsText(resultPath);
				env.execute();
				
				// return expected result
				return "(7)\n";
			}
			case 2: {
				/*
				 * First-n on grouped data set
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple2<Long, Integer>> first = ds.groupBy(1).first(4)
															.map(new OneMapper2()).groupBy(0).sum(1);
				
				first.writeAsText(resultPath);
				env.execute();
				
				// return expected result
				return "(1,1)\n(2,2)\n(3,3)\n(4,4)\n(5,4)\n(6,4)\n";
			}
			case 3: {
				/*
				 * First-n on grouped and sorted data set
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple2<Long, Integer>> first = ds.groupBy(1).sortGroup(0, Order.DESCENDING).first(3)
															.project(1,0).types(Long.class, Integer.class);
				
				first.writeAsText(resultPath);
				env.execute();
				
				// return expected result
				return "(1,1)\n"
						+ "(2,3)\n(2,2)\n"
						+ "(3,6)\n(3,5)\n(3,4)\n"
						+ "(4,10)\n(4,9)\n(4,8)\n"
						+ "(5,15)\n(5,14)\n(5,13)\n"
						+ "(6,21)\n(6,20)\n(6,19)\n";
				
			}
			default:
				throw new IllegalArgumentException("Invalid program id");
			}
			
		}
	
	}
	
	public static class OneMapper implements MapFunction<Tuple3<Integer, Long, String>, Tuple1<Integer>> {
		private static final long serialVersionUID = 1L;
		private final Tuple1<Integer> one = new Tuple1<Integer>(1);
		@Override
		public Tuple1<Integer> map(Tuple3<Integer, Long, String> value) {
			return one;
		}
	}
	
	public static class OneMapper2 implements MapFunction<Tuple3<Integer, Long, String>, Tuple2<Long, Integer>> {
		private static final long serialVersionUID = 1L;
		private final Tuple2<Long, Integer> one = new Tuple2<Long, Integer>(0l,1);
		@Override
		public Tuple2<Long, Integer> map(Tuple3<Integer, Long, String> value) {
			one.f0 = value.f1;
			return one;
		}
	}
	
}
