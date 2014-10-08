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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.common.functions.RichReduceFunction;
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

@RunWith(Parameterized.class)
public class ReduceITCase extends JavaProgramTestBase {
	
	private static int NUM_PROGRAMS = 10;
	
	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;
	
	public ReduceITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		expectedResult = ReduceProgs.runProgram(curProgId, resultPath);
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
	
	private static class ReduceProgs {
		
		public static String runProgram(int progId, String resultPath) throws Exception {
			
			switch(progId) {
			case 1: {
				/*
				 * Reduce on tuples with key field selector
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> reduceDs = ds.
						groupBy(1).reduce(new Tuple3Reduce("B-)"));
				
				reduceDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,1,Hi\n" +
						"5,2,B-)\n" +
						"15,3,B-)\n" +
						"34,4,B-)\n" +
						"65,5,B-)\n" +
						"111,6,B-)\n";
			}
			case 2: {
				/*
				 * Reduce on tuples with multiple key field selectors
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = CollectionDataSets.get5TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> reduceDs = ds.
						groupBy(4,0).reduce(new Tuple5Reduce());
				
				reduceDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,1,0,Hallo,1\n" +
						"2,3,2,Hallo Welt wie,1\n" +
						"2,2,1,Hallo Welt,2\n" +
						"3,9,0,P-),2\n" +
						"3,6,5,BCD,3\n" +
						"4,17,0,P-),1\n" +
						"4,17,0,P-),2\n" +
						"5,11,10,GHI,1\n" +
						"5,29,0,P-),2\n" +
						"5,25,0,P-),3\n";
			} 
			case 3: {
				/*
				 * Reduce on tuples with key extractor
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> reduceDs = ds.
						groupBy(new KeySelector<Tuple3<Integer,Long,String>, Long>() {
									private static final long serialVersionUID = 1L;
									@Override
									public Long getKey(Tuple3<Integer, Long, String> in) {
										return in.f1;
									}
								}).reduce(new Tuple3Reduce("B-)"));
				
				reduceDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,1,Hi\n" +
						"5,2,B-)\n" +
						"15,3,B-)\n" +
						"34,4,B-)\n" +
						"65,5,B-)\n" +
						"111,6,B-)\n";
				
			}
			case 4: {
				/*
				 * Reduce on custom type with key extractor
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<CustomType> ds = CollectionDataSets.getCustomTypeDataSet(env);
				DataSet<CustomType> reduceDs = ds.
						groupBy(new KeySelector<CustomType, Integer>() {
									private static final long serialVersionUID = 1L;
									@Override
									public Integer getKey(CustomType in) {
										return in.myInt;
									}
								}).reduce(new CustomTypeReduce());
				
				reduceDs.writeAsText(resultPath);
				env.execute();
				
				// return expected result
				return "1,0,Hi\n" +
						"2,3,Hello!\n" +
						"3,12,Hello!\n" +
						"4,30,Hello!\n" +
						"5,60,Hello!\n" +
						"6,105,Hello!\n";
			}
			case 5: {
				/*
				 * All-reduce for tuple
				 */

				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> reduceDs = ds.
						reduce(new AllAddingTuple3Reduce());
				
				reduceDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "231,91,Hello World\n";
			}
			case 6: {
				/*
				 * All-reduce for custom types
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<CustomType> ds = CollectionDataSets.getCustomTypeDataSet(env);
				DataSet<CustomType> reduceDs = ds.
						reduce(new AllAddingCustomTypeReduce());
				
				reduceDs.writeAsText(resultPath);
				env.execute();
				
				// return expected result
				return "91,210,Hello!";
			}
			case 7: {
				
				/*
				 * Reduce with broadcast set
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Integer> intDs = CollectionDataSets.getIntegerDataSet(env);
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> reduceDs = ds.
						groupBy(1).reduce(new BCTuple3Reduce()).withBroadcastSet(intDs, "ints");
				
				reduceDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,1,Hi\n" +
						"5,2,55\n" +
						"15,3,55\n" +
						"34,4,55\n" +
						"65,5,55\n" +
						"111,6,55\n";
			}
			case 8: {
				/*
				 * Reduce with UDF that returns the second input object (check mutable object handling)
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> reduceDs = ds.
						groupBy(1).reduce(new InputReturningTuple3Reduce());
				
				reduceDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,1,Hi\n" +
						"5,2,Hi again!\n" +
						"15,3,Hi again!\n" +
						"34,4,Hi again!\n" +
						"65,5,Hi again!\n" +
						"111,6,Hi again!\n";
			}
			case 9: {
				/*
				 * Reduce with a Tuple-returning KeySelector 
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple5<Integer, Long,  Integer, String, Long>> ds = CollectionDataSets.get5TupleDataSet(env);
				DataSet<Tuple5<Integer, Long,  Integer, String, Long>> reduceDs = ds .
						groupBy(
								new KeySelector<Tuple5<Integer,Long,Integer,String,Long>, Tuple2<Integer, Long>>() {
									private static final long serialVersionUID = 1L;
		
									@Override
									public Tuple2<Integer, Long> getKey(Tuple5<Integer,Long,Integer,String,Long> t) {
										return new Tuple2<Integer, Long>(t.f0, t.f4);
									}
								}).reduce(new Tuple5Reduce());
				
				reduceDs.writeAsCsv(resultPath);
				env.execute();
				
				return "1,1,0,Hallo,1\n" +
						"2,3,2,Hallo Welt wie,1\n" +
						"2,2,1,Hallo Welt,2\n" +
						"3,9,0,P-),2\n" +
						"3,6,5,BCD,3\n" +
						"4,17,0,P-),1\n" +
						"4,17,0,P-),2\n" +
						"5,11,10,GHI,1\n" +
						"5,29,0,P-),2\n" +
						"5,25,0,P-),3\n";
			}
			case 10: {
				/*
				 * Case 2 with String-based field expression
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = CollectionDataSets.get5TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> reduceDs = ds.
						groupBy("f4","f0").reduce(new Tuple5Reduce());
				
				reduceDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,1,0,Hallo,1\n" +
						"2,3,2,Hallo Welt wie,1\n" +
						"2,2,1,Hallo Welt,2\n" +
						"3,9,0,P-),2\n" +
						"3,6,5,BCD,3\n" +
						"4,17,0,P-),1\n" +
						"4,17,0,P-),2\n" +
						"5,11,10,GHI,1\n" +
						"5,29,0,P-),2\n" +
						"5,25,0,P-),3\n";
			} 
			
			default:
				throw new IllegalArgumentException("Invalid program id");
			}
			
		}
	
	}
	
	public static class Tuple3Reduce implements ReduceFunction<Tuple3<Integer, Long, String>> {
		private static final long serialVersionUID = 1L;
		private final Tuple3<Integer, Long, String> out = new Tuple3<Integer, Long, String>();
		private final String f2Replace;
		
		public Tuple3Reduce() { 
			this.f2Replace = null;
		}
		
		public Tuple3Reduce(String f2Replace) { 
			this.f2Replace = f2Replace;
		}
		

		@Override
		public Tuple3<Integer, Long, String> reduce(
				Tuple3<Integer, Long, String> in1,
				Tuple3<Integer, Long, String> in2) throws Exception {

			if(f2Replace == null) {
				out.setFields(in1.f0+in2.f0, in1.f1, in1.f2);
			} else {
				out.setFields(in1.f0+in2.f0, in1.f1, this.f2Replace);
			}
			return out;
		}
	}
	
	public static class Tuple5Reduce implements ReduceFunction<Tuple5<Integer, Long, Integer, String, Long>> {
		private static final long serialVersionUID = 1L;
		private final Tuple5<Integer, Long, Integer, String, Long> out = new Tuple5<Integer, Long, Integer, String, Long>();
		
		@Override
		public Tuple5<Integer, Long, Integer, String, Long> reduce(
				Tuple5<Integer, Long, Integer, String, Long> in1,
				Tuple5<Integer, Long, Integer, String, Long> in2)
				throws Exception {
			
			out.setFields(in1.f0, in1.f1+in2.f1, 0, "P-)", in1.f4);
			return out;
		}
	}
	
	public static class CustomTypeReduce implements ReduceFunction<CustomType> {
		private static final long serialVersionUID = 1L;
		private final CustomType out = new CustomType();
		
		@Override
		public CustomType reduce(CustomType in1, CustomType in2)
				throws Exception {
			
			out.myInt = in1.myInt;
			out.myLong = in1.myLong + in2.myLong;
			out.myString = "Hello!";
			return out;
		}
	}
	
	public static class InputReturningTuple3Reduce implements ReduceFunction<Tuple3<Integer, Long, String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple3<Integer, Long, String> reduce(
				Tuple3<Integer, Long, String> in1,
				Tuple3<Integer, Long, String> in2) throws Exception {

			in2.f0 = in1.f0 + in2.f0;
			in2.f2 = "Hi again!";
			return in2;
		}
	}
	
	public static class AllAddingTuple3Reduce implements ReduceFunction<Tuple3<Integer, Long, String>> {
		private static final long serialVersionUID = 1L;
		private final Tuple3<Integer, Long, String> out = new Tuple3<Integer, Long, String>();
		
		@Override
		public Tuple3<Integer, Long, String> reduce(
				Tuple3<Integer, Long, String> in1,
				Tuple3<Integer, Long, String> in2) throws Exception {

			out.setFields(in1.f0+in2.f0, in1.f1+in2.f1, "Hello World");
			return out;
		}
	}
	
	public static class AllAddingCustomTypeReduce implements ReduceFunction<CustomType> {
		private static final long serialVersionUID = 1L;
		private final CustomType out = new CustomType();
		
		@Override
		public CustomType reduce(CustomType in1, CustomType in2)
				throws Exception {
			
			out.myInt = in1.myInt + in2.myInt;
			out.myLong = in1.myLong + in2.myLong;
			out.myString = "Hello!";
			return out;
		}
	}
	
	public static class BCTuple3Reduce extends RichReduceFunction<Tuple3<Integer, Long, String>> {
		private static final long serialVersionUID = 1L;
		private final Tuple3<Integer, Long, String> out = new Tuple3<Integer, Long, String>();
		private String f2Replace = "";
		
		@Override
		public void open(Configuration config) {
			
			Collection<Integer> ints = this.getRuntimeContext().getBroadcastVariable("ints");
			int sum = 0;
			for(Integer i : ints) {
				sum += i;
			}
			f2Replace = sum+"";
			
		}

		@Override
		public Tuple3<Integer, Long, String> reduce(
				Tuple3<Integer, Long, String> in1,
				Tuple3<Integer, Long, String> in2) throws Exception {

			out.setFields(in1.f0+in2.f0, in1.f1, this.f2Replace);
			return out;
		}
	}
	
}
