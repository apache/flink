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
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.compiler.PactCompiler;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets.CrazyNested;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets.CustomType;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets.FromTuple;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets.FromTupleWithCTor;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets.POJO;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets.PojoContainingTupleAndWritable;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.Collector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

@SuppressWarnings("serial")
@RunWith(Parameterized.class)
public class GroupReduceITCase extends JavaProgramTestBase {
	
	private static int NUM_PROGRAMS = 26;
	
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
		expectedResult = GroupReduceProgs.runProgram(curProgId, resultPath, isCollectionExecution());
	}
	
	@Override
	protected void postSubmit() throws Exception {
		if (expectedResult != null) {
			compareResultsByLinesInMemory(expectedResult, resultPath);
		}
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
		
		public static String runProgram(int progId, String resultPath, boolean collectionExecution) throws Exception {

			switch (progId) {
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
							groupBy(4, 0).reduceGroup(new Tuple5GroupReduce());

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
					DataSet<Tuple3<Integer, Long, String>> reduceDs = ds.
							groupBy(1).sortGroup(2, Order.ASCENDING).reduceGroup(new Tuple3SortedGroupReduce());

					reduceDs.writeAsCsv(resultPath);
					env.execute();

					// return expected result
					return "1,1,Hi\n" +
							"5,2,Hello-Hello world\n" +
							"15,3,Hello world, how are you?-I am fine.-Luke Skywalker\n" +
							"34,4,Comment#1-Comment#2-Comment#3-Comment#4\n" +
							"65,5,Comment#5-Comment#6-Comment#7-Comment#8-Comment#9\n" +
							"111,6,Comment#10-Comment#11-Comment#12-Comment#13-Comment#14-Comment#15\n";

				}
				case 4: {
				/*
				 * check correctness of groupReduce on tuples with key extractor
				 */

					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

					DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
					DataSet<Tuple2<Integer, Long>> reduceDs = ds.
							groupBy(new KeySelector<Tuple3<Integer, Long, String>, Long>() {
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
				 * check correctness of all-groupreduce for tuples
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
				 * check correctness of all-groupreduce for custom types
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
				case 10: {
				
				/*
				 * check correctness of groupReduce on custom type with key extractor and combine
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
							}).reduceGroup(new CustomTypeGroupReduceWithCombine());

					reduceDs.writeAsText(resultPath);
					env.execute();

					// return expected result
					if (collectionExecution) {
						return null;

					} else {
						return "1,0,test1\n" +
								"2,3,test2\n" +
								"3,12,test3\n" +
								"4,30,test4\n" +
								"5,60,test5\n" +
								"6,105,test6\n";
					}
				}
				case 11: {
				
				/*
				 * check correctness of groupReduce on tuples with combine
				 */

					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
					env.setDegreeOfParallelism(2); // important because it determines how often the combiner is called

					DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
					DataSet<Tuple2<Integer, String>> reduceDs = ds.
							groupBy(1).reduceGroup(new Tuple3GroupReduceWithCombine());

					reduceDs.writeAsCsv(resultPath);
					env.execute();

					// return expected result
					if (collectionExecution) {
						return null;

					} else {
						return "1,test1\n" +
								"5,test2\n" +
								"15,test3\n" +
								"34,test4\n" +
								"65,test5\n" +
								"111,test6\n";
					}
				}
				// all-groupreduce with combine
				case 12: {
				
				/*
				 * check correctness of all-groupreduce for tuples with combine
				 */

					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

					DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env)
							.map(new IdentityMapper<Tuple3<Integer, Long, String>>()).setParallelism(4);

					Configuration cfg = new Configuration();
					cfg.setString(PactCompiler.HINT_SHIP_STRATEGY, PactCompiler.HINT_SHIP_STRATEGY_REPARTITION);
					DataSet<Tuple2<Integer, String>> reduceDs = ds.reduceGroup(new Tuple3AllGroupReduceWithCombine())
							.withParameters(cfg);

					reduceDs.writeAsCsv(resultPath);
					env.execute();

					// return expected result
					if (collectionExecution) {
						return null;
					} else {
						return "322,testtesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttest\n";
					}
				}
				case 13: {
				
				/*
				 * check correctness of groupReduce with descending group sort
				 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
					env.setDegreeOfParallelism(1);

					DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
					DataSet<Tuple3<Integer, Long, String>> reduceDs = ds.
							groupBy(1).sortGroup(2, Order.DESCENDING).reduceGroup(new Tuple3SortedGroupReduce());

					reduceDs.writeAsCsv(resultPath);
					env.execute();

					// return expected result
					return "1,1,Hi\n" +
							"5,2,Hello world-Hello\n" +
							"15,3,Luke Skywalker-I am fine.-Hello world, how are you?\n" +
							"34,4,Comment#4-Comment#3-Comment#2-Comment#1\n" +
							"65,5,Comment#9-Comment#8-Comment#7-Comment#6-Comment#5\n" +
							"111,6,Comment#15-Comment#14-Comment#13-Comment#12-Comment#11-Comment#10\n";

				}
				case 14: {
					/*
					 * check correctness of groupReduce on tuples with tuple-returning key selector
					 */

						final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

						DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = CollectionDataSets.get5TupleDataSet(env);
						DataSet<Tuple5<Integer, Long, Integer, String, Long>> reduceDs = ds.
								groupBy(
										new KeySelector<Tuple5<Integer,Long,Integer,String,Long>, Tuple2<Integer, Long>>() {
											private static final long serialVersionUID = 1L;
				
											@Override
											public Tuple2<Integer, Long> getKey(Tuple5<Integer,Long,Integer,String,Long> t) {
												return new Tuple2<Integer, Long>(t.f0, t.f4);
											}
										}).reduceGroup(new Tuple5GroupReduce());

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
				case 15: {
					/*
					 * check that input of combiner is also sorted for combinable groupReduce with group sorting
					 */

					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
					env.setDegreeOfParallelism(1);

					DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
					DataSet<Tuple3<Integer, Long, String>> reduceDs = ds.
							groupBy(1).sortGroup(0, Order.ASCENDING).reduceGroup(new OrderCheckingCombinableReduce());

					reduceDs.writeAsCsv(resultPath);
					env.execute();

					// return expected result
					return "1,1,Hi\n" +
							"2,2,Hello\n" +
							"4,3,Hello world, how are you?\n" +
							"7,4,Comment#1\n" +
							"11,5,Comment#5\n" +
							"16,6,Comment#10\n";
					
				}
				case 16: {
					/*
					 * Deep nesting test
					 * + null value in pojo
					 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
					
					DataSet<CrazyNested> ds = CollectionDataSets.getCrazyNestedDataSet(env);
					DataSet<Tuple2<String, Integer>> reduceDs = ds.groupBy("nest_Lvl1.nest_Lvl2.nest_Lvl3.nest_Lvl4.f1nal")
							.reduceGroup(new GroupReduceFunction<CollectionDataSets.CrazyNested, Tuple2<String, Integer>>() {
								private static final long serialVersionUID = 1L;

								@Override
								public void reduce(Iterable<CrazyNested> values,
										Collector<Tuple2<String, Integer>> out)
										throws Exception {
									int c = 0; String n = null;
									for(CrazyNested v : values) {
										c++; // haha
										n = v.nest_Lvl1.nest_Lvl2.nest_Lvl3.nest_Lvl4.f1nal;
									}
									out.collect(new Tuple2<String, Integer>(n,c));
								}});
					
					reduceDs.writeAsCsv(resultPath);
					env.execute();
					
					// return expected result
					return "aa,1\nbb,2\ncc,3\n";
				} 
				case 17: {
					/*
					 * Test Pojo extending from tuple WITH custom fields
					 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
					
					DataSet<FromTupleWithCTor> ds = CollectionDataSets.getPojoExtendingFromTuple(env);
					DataSet<Integer> reduceDs = ds.groupBy("special", "f2")
							.reduceGroup(new GroupReduceFunction<FromTupleWithCTor, Integer>() {
								private static final long serialVersionUID = 1L;
								@Override
								public void reduce(Iterable<FromTupleWithCTor> values,
										Collector<Integer> out)
										throws Exception {
									int c = 0;
									for(FromTuple v : values) {
										c++;
									}
									out.collect(c);
								}});
					
					reduceDs.writeAsText(resultPath);
					env.execute();
					
					// return expected result
					return "3\n2\n";
				} 
				case 18: {
					/*
					 * Test Pojo containing a Writable and Tuples
					 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
					
					DataSet<PojoContainingTupleAndWritable> ds = CollectionDataSets.getPojoContainingTupleAndWritable(env);
					DataSet<Integer> reduceDs = ds.groupBy("hadoopFan", "theTuple.*") // full tuple selection
							.reduceGroup(new GroupReduceFunction<PojoContainingTupleAndWritable, Integer>() {
								private static final long serialVersionUID = 1L;
								@Override
								public void reduce(Iterable<PojoContainingTupleAndWritable> values,
										Collector<Integer> out)
										throws Exception {
									int c = 0;
									for(PojoContainingTupleAndWritable v : values) {
										c++;
									}
									out.collect(c);
								}});
					
					reduceDs.writeAsText(resultPath);
					env.execute();
					
					// return expected result
					return "1\n5\n";
				} 
				case 19: {
					/*
					 * Test Tuple containing pojos and regular fields
					 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
					
					DataSet<Tuple3<Integer,CrazyNested, POJO>> ds = CollectionDataSets.getTupleContainingPojos(env);
					DataSet<Integer> reduceDs = ds.groupBy("f0", "f1.*") // nested full tuple selection
							.reduceGroup(new GroupReduceFunction<Tuple3<Integer,CrazyNested, POJO>, Integer>() {
								private static final long serialVersionUID = 1L;
								@Override
								public void reduce(Iterable<Tuple3<Integer,CrazyNested, POJO>> values,
										Collector<Integer> out)
										throws Exception {
									int c = 0;
									for(Tuple3<Integer,CrazyNested, POJO> v : values) {
										c++;
									}
									out.collect(c);
								}});
					
					reduceDs.writeAsText(resultPath);
					env.execute();
					
					// return expected result
					return "3\n1\n";
				}
				case 20: {
					/*
					 * Test string-based definition on group sort, based on test:
					 * check correctness of groupReduce with descending group sort
					 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
					env.setDegreeOfParallelism(1);

					DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
					DataSet<Tuple3<Integer, Long, String>> reduceDs = ds.
							groupBy(1).sortGroup("f2", Order.DESCENDING).reduceGroup(new Tuple3SortedGroupReduce());

					reduceDs.writeAsCsv(resultPath);
					env.execute();

					// return expected result
					return "1,1,Hi\n" +
							"5,2,Hello world-Hello\n" +
							"15,3,Luke Skywalker-I am fine.-Hello world, how are you?\n" +
							"34,4,Comment#4-Comment#3-Comment#2-Comment#1\n" +
							"65,5,Comment#9-Comment#8-Comment#7-Comment#6-Comment#5\n" +
							"111,6,Comment#15-Comment#14-Comment#13-Comment#12-Comment#11-Comment#10\n";

				}
				case 21: {
					/*
					 * Test int-based definition on group sort, for (full) nested Tuple
					 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
					env.setDegreeOfParallelism(1);

					DataSet<Tuple2<Tuple2<Integer, Integer>, String>> ds = CollectionDataSets.getGroupSortedNestedTupleDataSet(env);
					DataSet<String> reduceDs = ds.groupBy("f1").sortGroup(0, Order.DESCENDING).reduceGroup(new NestedTupleReducer());
					reduceDs.writeAsText(resultPath);
					env.execute();

					// return expected result
					return "a--(2,1)-(1,3)-(1,2)-\n" +
							"b--(2,2)-\n"+
							"c--(4,9)-(3,6)-(3,3)-\n";
				}
				case 22: {
					/*
					 * Test int-based definition on group sort, for (partial) nested Tuple ASC
					 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
					env.setDegreeOfParallelism(1);

					DataSet<Tuple2<Tuple2<Integer, Integer>, String>> ds = CollectionDataSets.getGroupSortedNestedTupleDataSet(env);
					// f0.f0 is first integer
					DataSet<String> reduceDs = ds.groupBy("f1")
							.sortGroup("f0.f0", Order.ASCENDING)
							.sortGroup("f0.f1", Order.ASCENDING)
							.reduceGroup(new NestedTupleReducer());
					reduceDs.writeAsText(resultPath);
					env.execute();
					
					// return expected result
					return "a--(1,2)-(1,3)-(2,1)-\n" +
							"b--(2,2)-\n"+
							"c--(3,3)-(3,6)-(4,9)-\n";
				}
				case 23: {
					/*
					 * Test string-based definition on group sort, for (partial) nested Tuple DESC
					 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
					env.setDegreeOfParallelism(1);

					DataSet<Tuple2<Tuple2<Integer, Integer>, String>> ds = CollectionDataSets.getGroupSortedNestedTupleDataSet(env);
					// f0.f0 is first integer
					DataSet<String> reduceDs = ds.groupBy("f1").sortGroup("f0.f0", Order.DESCENDING).reduceGroup(new NestedTupleReducer());
					reduceDs.writeAsText(resultPath);
					env.execute();
					
					// return expected result
					return "a--(2,1)-(1,3)-(1,2)-\n" +
							"b--(2,2)-\n"+
							"c--(4,9)-(3,3)-(3,6)-\n";
				}
				case 24: {
					/*
					 * Test string-based definition on group sort, for two grouping keys
					 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
					env.setDegreeOfParallelism(1);

					DataSet<Tuple2<Tuple2<Integer, Integer>, String>> ds = CollectionDataSets.getGroupSortedNestedTupleDataSet(env);
					// f0.f0 is first integer
					DataSet<String> reduceDs = ds.groupBy("f1").sortGroup("f0.f0", Order.DESCENDING).sortGroup("f0.f1", Order.DESCENDING).reduceGroup(new NestedTupleReducer());
					reduceDs.writeAsText(resultPath);
					env.execute();
					
					// return expected result
					return "a--(2,1)-(1,3)-(1,2)-\n" +
							"b--(2,2)-\n"+
							"c--(4,9)-(3,6)-(3,3)-\n";
				}
				case 25: {
					/*
					 * Test string-based definition on group sort, for two grouping keys with Pojos
					 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
					env.setDegreeOfParallelism(1);

					DataSet<PojoContainingTupleAndWritable> ds = CollectionDataSets.getGroupSortedPojoContainingTupleAndWritable(env);
					// f0.f0 is first integer
					DataSet<String> reduceDs = ds.groupBy("hadoopFan").sortGroup("theTuple.f0", Order.DESCENDING).sortGroup("theTuple.f1", Order.DESCENDING)
							.reduceGroup(new GroupReduceFunction<CollectionDataSets.PojoContainingTupleAndWritable, String>() {
								@Override
								public void reduce(
										Iterable<PojoContainingTupleAndWritable> values,
										Collector<String> out) throws Exception {
									boolean once = false;
									StringBuilder concat = new StringBuilder();
									for(PojoContainingTupleAndWritable value : values) {
										if(!once) {
											concat.append(value.hadoopFan.get());
											concat.append("---");
											once = true;
										}
										concat.append(value.theTuple);
										concat.append("-");
									}
									out.collect(concat.toString());
								}
					});
					reduceDs.writeAsText(resultPath);
					env.execute();
					
					// return expected result
					return "1---(10,100)-\n" +
							"2---(30,600)-(30,400)-(30,200)-(20,201)-(20,200)-\n";
				}
				case 26: {
					/*
					 * Test grouping with pojo containing multiple pojos (was a bug)
					 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
					env.setDegreeOfParallelism(1);

					DataSet<CollectionDataSets.PojoWithMultiplePojos> ds = CollectionDataSets.getPojoWithMultiplePojos(env);
					// f0.f0 is first integer
					DataSet<String> reduceDs = ds.groupBy("p2.a2")
							.reduceGroup(new GroupReduceFunction<CollectionDataSets.PojoWithMultiplePojos, String>() {
								@Override
								public void reduce(
										Iterable<CollectionDataSets.PojoWithMultiplePojos> values,
										Collector<String> out) throws Exception {
									StringBuilder concat = new StringBuilder();
									for(CollectionDataSets.PojoWithMultiplePojos value : values) {
										concat.append(value.p2.a2);
									}
									out.collect(concat.toString());
								}
							});
					reduceDs.writeAsText(resultPath);
					env.execute();

					// return expected result
					return "b\nccc\nee\n";
				}
				
				default: {
					throw new IllegalArgumentException("Invalid program id");
				}
			}
		}
	
	}
	
	
	public static class NestedTupleReducer implements GroupReduceFunction<Tuple2<Tuple2<Integer,Integer>,String>, String> {
		@Override
		public void reduce(
				Iterable<Tuple2<Tuple2<Integer, Integer>, String>> values,
				Collector<String> out)
				throws Exception {
			boolean once = false;
			StringBuilder concat = new StringBuilder();
			for(Tuple2<Tuple2<Integer, Integer>, String> value : values) {
				if(!once) {
					concat.append(value.f1).append("--");
					once = true;
				}
				concat.append(value.f0); // the tuple with the sorted groups
				concat.append("-");
			}
			out.collect(concat.toString());
		}
	}
	
	public static class Tuple3GroupReduce implements GroupReduceFunction<Tuple3<Integer, Long, String>, Tuple2<Integer, Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterable<Tuple3<Integer, Long, String>> values, Collector<Tuple2<Integer, Long>> out) {
			
			int i = 0;
			long l = 0l;
			
			for (Tuple3<Integer, Long, String> t : values) {
				i += t.f0;
				l = t.f1;
			}
			
			out.collect(new Tuple2<Integer, Long>(i, l));
			
		}
	}
	
	public static class Tuple3SortedGroupReduce implements GroupReduceFunction<Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {
		private static final long serialVersionUID = 1L;


		@Override
		public void reduce(Iterable<Tuple3<Integer, Long, String>> values, Collector<Tuple3<Integer, Long, String>> out) {
			int sum = 0;
			long key = 0;
			StringBuilder concat = new StringBuilder();
			
			for (Tuple3<Integer, Long, String> next : values) {
				sum += next.f0;
				key = next.f1;
				concat.append(next.f2).append("-");
			}
			
			if (concat.length() > 0) {
				concat.setLength(concat.length() - 1);
			}
			
			out.collect(new Tuple3<Integer, Long, String>(sum, key, concat.toString()));
		}
	}
	
	public static class Tuple5GroupReduce implements GroupReduceFunction<Tuple5<Integer, Long, Integer, String, Long>, Tuple5<Integer, Long, Integer, String, Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(
				Iterable<Tuple5<Integer, Long, Integer, String, Long>> values,
				Collector<Tuple5<Integer, Long, Integer, String, Long>> out)
		{
			int i = 0;
			long l = 0l;
			long l2 = 0l;
			
			for ( Tuple5<Integer, Long, Integer, String, Long> t : values ) {
				i = t.f0;
				l += t.f1;
				l2 = t.f4;
			}
			
			out.collect(new Tuple5<Integer, Long, Integer, String, Long>(i, l, 0, "P-)", l2));
		}
	}
	
	public static class CustomTypeGroupReduce implements GroupReduceFunction<CustomType, CustomType> {
		private static final long serialVersionUID = 1L;
		

		@Override
		public void reduce(Iterable<CustomType> values, Collector<CustomType> out) {
			final Iterator<CustomType> iter = values.iterator();
			
			CustomType o = new CustomType();
			CustomType c = iter.next();
			
			o.myString = "Hello!";
			o.myInt = c.myInt;
			o.myLong = c.myLong;
			
			while (iter.hasNext()) {
				CustomType next = iter.next();
				o.myLong += next.myLong;
			}
			
			out.collect(o);
			
		}
	}


	public static class InputReturningTuple3GroupReduce implements GroupReduceFunction<Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterable<Tuple3<Integer, Long, String>> values, Collector<Tuple3<Integer, Long, String>> out) {

			for ( Tuple3<Integer, Long, String> t : values ) {
				
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
	
	public static class AllAddingTuple3GroupReduce implements GroupReduceFunction<Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void reduce(Iterable<Tuple3<Integer, Long, String>> values, Collector<Tuple3<Integer, Long, String>> out) {

			int i = 0;
			long l = 0l;
			
			for ( Tuple3<Integer, Long, String> t : values ) {
				i += t.f0;
				l += t.f1;
			}
			
			out.collect(new Tuple3<Integer, Long, String>(i, l, "Hello World"));
		}
	}
	
	public static class AllAddingCustomTypeGroupReduce implements GroupReduceFunction<CustomType, CustomType> {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void reduce(Iterable<CustomType> values, Collector<CustomType> out) {

			CustomType o = new CustomType(0, 0, "Hello!");
			
			for (CustomType next : values) {
				o.myInt += next.myInt;
				o.myLong += next.myLong;
			}
			
			out.collect(o);
		}
	}
	
	public static class BCTuple3GroupReduce extends RichGroupReduceFunction<Tuple3<Integer, Long, String>,Tuple3<Integer, Long, String>> {
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
		public void reduce(Iterable<Tuple3<Integer, Long, String>> values, Collector<Tuple3<Integer, Long, String>> out) {
				
			int i = 0;
			long l = 0l;
			
			for ( Tuple3<Integer, Long, String> t : values ) {
				i += t.f0;
				l = t.f1;
			}
			
			out.collect(new Tuple3<Integer, Long, String>(i, l, this.f2Replace));
			
		}
	}
	
	@RichGroupReduceFunction.Combinable
	public static class Tuple3GroupReduceWithCombine extends RichGroupReduceFunction<Tuple3<Integer, Long, String>, Tuple2<Integer, String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void combine(Iterable<Tuple3<Integer, Long, String>> values, Collector<Tuple3<Integer, Long, String>> out) {

			Tuple3<Integer, Long, String> o = new Tuple3<Integer, Long, String>(0, 0l, "");

			for ( Tuple3<Integer, Long, String> t : values ) {
				o.f0 += t.f0;
				o.f1 = t.f1;
				o.f2 = "test"+o.f1;
			}

			out.collect(o);
		}

		@Override
		public void reduce(Iterable<Tuple3<Integer, Long, String>> values, Collector<Tuple2<Integer, String>> out) {

			int i = 0;
			String s = "";

			for ( Tuple3<Integer, Long, String> t : values ) {
				i += t.f0;
				s = t.f2;
			}

			out.collect(new Tuple2<Integer, String>(i, s));

		}
	}
	
	@RichGroupReduceFunction.Combinable
	public static class Tuple3AllGroupReduceWithCombine extends RichGroupReduceFunction<Tuple3<Integer, Long, String>, Tuple2<Integer, String>> {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void combine(Iterable<Tuple3<Integer, Long, String>> values, Collector<Tuple3<Integer, Long, String>> out) {
			
			Tuple3<Integer, Long, String> o = new Tuple3<Integer, Long, String>(0, 0l, "");
			
			for ( Tuple3<Integer, Long, String> t : values ) {
				o.f0 += t.f0;
				o.f1 += t.f1;
				o.f2 += "test";
			}
			
			out.collect(o);
		}

		@Override
		public void reduce(Iterable<Tuple3<Integer, Long, String>> values, Collector<Tuple2<Integer, String>> out) {
			
			int i = 0;
			String s = "";
			
			for ( Tuple3<Integer, Long, String> t : values ) {
				i += t.f0 + t.f1;
				s += t.f2;
			}
			
			out.collect(new Tuple2<Integer, String>(i, s));
			
		}
	}
	
	@RichGroupReduceFunction.Combinable
	public static class CustomTypeGroupReduceWithCombine extends RichGroupReduceFunction<CustomType, CustomType> {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void combine(Iterable<CustomType> values, Collector<CustomType> out) throws Exception {
			
			CustomType o = new CustomType();
			
			for ( CustomType c : values ) {
				o.myInt = c.myInt;
				o.myLong += c.myLong;
				o.myString = "test"+c.myInt;
			}
			
			out.collect(o);
		}

		@Override
		public void reduce(Iterable<CustomType> values, Collector<CustomType> out)  {
			
			CustomType o = new CustomType(0, 0, "");
			
			for ( CustomType c : values) {
				o.myInt = c.myInt;
				o.myLong += c.myLong;
				o.myString = c.myString;
			}
			
			out.collect(o);
			
		}
	}
	
	@RichGroupReduceFunction.Combinable
	public static class OrderCheckingCombinableReduce extends RichGroupReduceFunction<Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterable<Tuple3<Integer, Long, String>> values, Collector<Tuple3<Integer, Long, String>> out) throws Exception {
			Iterator<Tuple3<Integer,Long,String>> it = values.iterator();
			Tuple3<Integer,Long,String> t = it.next();
			
			int i = t.f0;
			out.collect(t);
			
			while(it.hasNext()) {
				t = it.next();
				if(i > t.f0 || t.f2.equals("INVALID-ORDER!")) {
					t.f2 = "INVALID-ORDER!";
					out.collect(t);
				}
			}		
		}
		
		@Override
		public void combine(Iterable<Tuple3<Integer, Long, String>> values, Collector<Tuple3<Integer, Long, String>> out) {	
			
			Iterator<Tuple3<Integer,Long,String>> it = values.iterator();
			Tuple3<Integer,Long,String> t = it.next();
			
			int i = t.f0;
			out.collect(t);
			
			while(it.hasNext()) {
				t = it.next();
				if(i > t.f0) {
					t.f2 = "INVALID-ORDER!";
					out.collect(t);
				}
			}

		}
		
		
	}
	
	public static final class IdentityMapper<T> extends RichMapFunction<T, T> {

		@Override
		public T map(T value) { return value; }
	}
}
