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
import java.util.Iterator;
import java.util.LinkedList;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.api.common.operators.Order;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.KeySelector;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple5;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.javaApiOperators.util.CollectionDataSets;
import eu.stratosphere.test.javaApiOperators.util.CollectionDataSets.CustomType;
import eu.stratosphere.test.util.JavaProgramTestBase;
import eu.stratosphere.util.Collector;

@RunWith(Parameterized.class)
public class GroupReduceITCase extends JavaProgramTestBase {
	
	private static int NUM_PROGRAMS = 9;
	
	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;
	
	public GroupReduceITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		expectedResult = GroupReduceProgs.runProgram(curProgId, resultPath);
	}
	
	@Override
	protected void postSubmit() throws Exception {
		if(expectedResult.substring(0, 6).equals("SORTED"))
			compareResultsByLinesInMemoryWithStrictOrder(expectedResult.substring(6, expectedResult.length()), resultPath);
		else
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
	
	private static class GroupReduceProgs {
		
		public static String runProgram(int progId, String resultPath) throws Exception {
			
			switch(progId) {
			case 1: {
				
				/*
				 * check correctness of groupReduce on tuples with key field selector
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple2<Integer, Long>> reduceDs = ds.
						groupBy(1).reduceGroup(new Tuple3GroupReduce());
				
				reduceDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,1\n" +
						"5,2\n" +
						"15,3\n" +
						"34,4\n" +
						"65,5\n" +
						"111,6\n";
			}
			case 2: {
				
				/*
				 * check correctness of groupReduce on tuples with multiple key field selector
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = CollectionDataSets.get5TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> reduceDs = ds.
						groupBy(4,0).reduceGroup(new Tuple5GroupReduce());
				
				reduceDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,1,0,P-),1\n" +
						"2,3,0,P-),1\n" +
						"2,2,0,P-),2\n" +
						"3,9,0,P-),2\n" +
						"3,6,0,P-),3\n" +
						"4,17,0,P-),1\n" +
						"4,17,0,P-),2\n" +
						"5,11,0,P-),1\n" +
						"5,29,0,P-),2\n" +
						"5,25,0,P-),3\n";
			}
			case 3: {
				
				/*
				 * check correctness of groupReduce on tuples with key field selector and group sorting
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				env.setDegreeOfParallelism(1);
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple2<Integer, Long>> reduceDs = ds.
						groupBy(1).sortGroup(0,Order.ASCENDING).reduceGroup(new Tuple3GroupReduce());
				
				reduceDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "SORTED1,1\n" +
						"5,2\n" +
						"15,3\n" +
						"34,4\n" +
						"65,5\n" +
						"111,6\n";
				
			}
			case 4: {
				/*
				 * check correctness of groupReduce on tuples with key extractor
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple2<Integer, Long>> reduceDs = ds.
						groupBy(new KeySelector<Tuple3<Integer,Long,String>, Long>() {
									private static final long serialVersionUID = 1L;
									@Override
									public Long getKey(Tuple3<Integer, Long, String> in) {
										return in.f1;
									}
								}).reduceGroup(new Tuple3GroupReduce());
				
				reduceDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,1\n" +
						"5,2\n" +
						"15,3\n" +
						"34,4\n" +
						"65,5\n" +
						"111,6\n";
				
			}
			case 5: {
				
				/*
				 * check correctness of groupReduce on custom type with type extractor
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
								}).reduceGroup(new CustomTypeGroupReduce());
				
				reduceDs.writeAsText(resultPath);
				env.execute();
				
				// return expected result
				return "1,0,Hello!\n" +
						"2,3,Hello!\n" +
						"3,12,Hello!\n" +
						"4,30,Hello!\n" +
						"5,60,Hello!\n" +
						"6,105,Hello!\n";
			}
			case 6: {
				
				/*
				 * check correctness of all-groupreduce for tuples with key field selector
				 */

				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> reduceDs = ds.reduceGroup(new AllAddingTuple3GroupReduce());
				
				reduceDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "231,91,Hello World\n";
			}
			case 7: {
				/*
				 * check correctness of all-groupreduce for custom types with key extractor
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<CustomType> ds = CollectionDataSets.getCustomTypeDataSet(env);
				DataSet<CustomType> reduceDs = ds.reduceGroup(new AllAddingCustomTypeGroupReduce());
				
				reduceDs.writeAsText(resultPath);
				env.execute();
				
				// return expected result
				return "91,210,Hello!";
			}
			case 8: {
				
				/*
				 * check correctness of groupReduce with broadcast set
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Integer> intDs = CollectionDataSets.getIntegerDataSet(env);
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> reduceDs = ds.
						groupBy(1).reduceGroup(new BCTuple3GroupReduce()).withBroadcastSet(intDs, "ints");
				
				reduceDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,1,55\n" +
						"5,2,55\n" +
						"15,3,55\n" +
						"34,4,55\n" +
						"65,5,55\n" +
						"111,6,55\n";
			}
			case 9: {
				
				/*
				 * check correctness of groupReduce if UDF returns input objects multiple times and changes it in between
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> reduceDs = ds.
						groupBy(1).reduceGroup(new InputReturningTuple3GroupReduce());
				
				reduceDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "11,1,Hi!\n" +
						"21,1,Hi again!\n" +
						"12,2,Hi!\n" +
						"22,2,Hi again!\n" +
						"13,2,Hi!\n" +
						"23,2,Hi again!\n";
			}
			// descending sort not working
//			case 10: {
//				
//				/*
//				 * check correctness of groupReduce on tuples with key field selector and group sorting
//				 */
//				
//				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//				env.setDegreeOfParallelism(1);
//				
//				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
//				DataSet<Tuple3<Integer, Long, String>> reduceDs = ds.
//						groupBy(1).sortGroup(0,Order.DESCENDING).reduceGroup(new Tuple3SortedGroupReduce());
//				
//				reduceDs.writeAsCsv(resultPath);
//				env.execute();
//				
//				// return expected result
//				return "SORTED1,1,Hi\n" +
//						"3,2,Hello world\n" +
//						"2,2,Hello\n" +
//						"6,3,Luke Skywalker\n" +
//						"5,3,I am fine.\n" +
//						"4,3,Hello world, how are you?\n";
//				
//			}
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
			
		}
	
	}
	
	public static class Tuple3GroupReduce extends GroupReduceFunction<Tuple3<Integer, Long, String>, Tuple2<Integer, Long>> {
		private static final long serialVersionUID = 1L;


		@Override
		public void reduce(Iterator<Tuple3<Integer, Long, String>> values,
				Collector<Tuple2<Integer, Long>> out) throws Exception {
			
			int i = 0;
			long l = 0l;
			
			while(values.hasNext()) {
				Tuple3<Integer, Long, String> t = values.next();
				i += t.f0;
				l = t.f1;
			}
			
			out.collect(new Tuple2<Integer, Long>(i, l));
			
		}
	}
	
	public static class Tuple3SortedGroupReduce extends GroupReduceFunction<Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {
		private static final long serialVersionUID = 1L;


		@Override
		public void reduce(Iterator<Tuple3<Integer, Long, String>> values,
				Collector<Tuple3<Integer, Long, String>> out) throws Exception {
			
			while(values.hasNext()) {
				Tuple3<Integer, Long, String> t = values.next();
				
				if(t.f1 < 4)
					out.collect(t);
			}
			
		}
	}
	
	public static class Tuple5GroupReduce extends GroupReduceFunction<Tuple5<Integer, Long, Integer, String, Long>, Tuple5<Integer, Long, Integer, String, Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(
				Iterator<Tuple5<Integer, Long, Integer, String, Long>> values,
				Collector<Tuple5<Integer, Long, Integer, String, Long>> out)
				throws Exception {
			
			int i = 0;
			long l = 0l;
			long l2 = 0l;
			
			while(values.hasNext()) {
				Tuple5<Integer, Long, Integer, String, Long> t = values.next();
				i = t.f0;
				l += t.f1;
				l2 = t.f4;
			}
			
			out.collect(new Tuple5<Integer, Long, Integer, String, Long>(i, l, 0, "P-)", l2));
		}
	}
	
	public static class CustomTypeGroupReduce extends GroupReduceFunction<CustomType, CustomType> {
		private static final long serialVersionUID = 1L;
		

		@Override
		public void reduce(Iterator<CustomType> values,
				Collector<CustomType> out) throws Exception {
			
			CustomType o = new CustomType();
			
			while(values.hasNext()) {
				CustomType c = values.next();
				o.myInt = c.myInt;
				o.myLong += c.myLong;
				o.myString = "Hello!";

			}
			
			out.collect(o);
			
		}
	}
	
	public static class InputReturningTuple3GroupReduce extends GroupReduceFunction<Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterator<Tuple3<Integer, Long, String>> values,
				Collector<Tuple3<Integer, Long, String>> out) throws Exception {

			while(values.hasNext()) {
				Tuple3<Integer, Long, String> t = values.next();
				
				if(t.f0 < 4) {
					t.f2 = "Hi!";
					t.f0 += 10;
					out.collect(t);
					t.f0 += 10;
					t.f2 = "Hi again!";
					out.collect(t);
				}
			}
		}
	}
	
	public static class AllAddingTuple3GroupReduce extends GroupReduceFunction<Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void reduce(Iterator<Tuple3<Integer, Long, String>> values,
				Collector<Tuple3<Integer, Long, String>> out) throws Exception {

			int i = 0;
			long l = 0l;
			
			while(values.hasNext()) {
				Tuple3<Integer, Long, String> t = values.next();
				i += t.f0;
				l += t.f1;
			}
			
			out.collect(new Tuple3<Integer, Long, String>(i, l, "Hello World"));
		}
	}
	
	public static class AllAddingCustomTypeGroupReduce extends GroupReduceFunction<CustomType, CustomType> {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void reduce(Iterator<CustomType> values,
				Collector<CustomType> out) throws Exception {

			CustomType o = new CustomType();
			
			while(values.hasNext()) {
				CustomType c = values.next();
				o.myInt += c.myInt;
				o.myLong += c.myLong;
				o.myString = "Hello!";

			}
			
			out.collect(o);
		}
	}
	
	public static class BCTuple3GroupReduce extends GroupReduceFunction<Tuple3<Integer, Long, String>,Tuple3<Integer, Long, String>> {
		private static final long serialVersionUID = 1L;
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
		public void reduce(Iterator<Tuple3<Integer, Long, String>> values,
				Collector<Tuple3<Integer, Long, String>> out) throws Exception {
				
			int i = 0;
			long l = 0l;
			
			while(values.hasNext()) {
				Tuple3<Integer, Long, String> t = values.next();
				i += t.f0;
				l = t.f1;
			}
			
			out.collect(new Tuple3<Integer, Long, String>(i, l, this.f2Replace));
			
		}
	}
	
}
