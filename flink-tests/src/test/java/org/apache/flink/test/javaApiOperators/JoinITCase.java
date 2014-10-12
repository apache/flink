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

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets.CustomType;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets.POJO;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.Collector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

@SuppressWarnings("serial")
@RunWith(Parameterized.class)
public class JoinITCase extends JavaProgramTestBase {
	
	private static int NUM_PROGRAMS = 22;
	
	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;
	
	public JoinITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		expectedResult = JoinProgs.runProgram(curProgId, resultPath);
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
	
	private static class JoinProgs {
		
		public static String runProgram(int progId, String resultPath) throws Exception {
			
			switch(progId) {
			case 1: {
				
				/*
				 * UDF Join on tuples with key field positions
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);
				DataSet<Tuple2<String, String>> joinDs = 
						ds1.join(ds2)
						.where(1)
						.equalTo(1)
						.with(new T3T5FlatJoin());
				
				joinDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "Hi,Hallo\n" +
						"Hello,Hallo Welt\n" +
						"Hello world,Hallo Welt\n";
				
			}
			case 2: {
				
				/*
				 * UDF Join on tuples with multiple key field positions
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);
				DataSet<Tuple2<String, String>> joinDs = 
						ds1.join(ds2)
						   .where(0,1)
						   .equalTo(0,4)
						   .with(new T3T5FlatJoin());
				
				joinDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "Hi,Hallo\n" +
						"Hello,Hallo Welt\n" +
						"Hello world,Hallo Welt wie gehts?\n" +
						"Hello world,ABC\n" +
						"I am fine.,HIJ\n" +
						"I am fine.,IJK\n";
				
			}
			case 3: {
				
				/*
				 * Default Join on tuples
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);
				DataSet<Tuple2<Tuple3<Integer, Long, String>,Tuple5<Integer, Long, Integer, String, Long>>> joinDs = 
						ds1.join(ds2)
						   .where(0)
						   .equalTo(2);
				
				joinDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "(1,1,Hi),(2,2,1,Hallo Welt,2)\n" +
						"(2,2,Hello),(2,3,2,Hallo Welt wie,1)\n" +
						"(3,2,Hello world),(3,4,3,Hallo Welt wie gehts?,2)\n";
			
			}
			case 4: {
				
				/*
				 * Join with Huge
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);
				DataSet<Tuple2<String, String>> joinDs = ds1.joinWithHuge(ds2)
															.where(1)
															.equalTo(1)
															.with(new T3T5FlatJoin());
				
				joinDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "Hi,Hallo\n" +
						"Hello,Hallo Welt\n" +
						"Hello world,Hallo Welt\n";
				
			}
			case 5: {
				
				/*
				 * Join with Tiny
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);
				DataSet<Tuple2<String, String>> joinDs = 
						ds1.joinWithTiny(ds2)
						   .where(1)
						   .equalTo(1)
						   .with(new T3T5FlatJoin());
				
				joinDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "Hi,Hallo\n" +
						"Hello,Hallo Welt\n" +
						"Hello world,Hallo Welt\n";
				
			}
			
			case 6: {
				
				/*
				 * Join that returns the left input object
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> joinDs = 
						ds1.join(ds2)
						   .where(1)
						   .equalTo(1)
						   .with(new LeftReturningJoin());
				
				joinDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,1,Hi\n" +
						"2,2,Hello\n" +
						"3,2,Hello world\n";
			}
			case 7: {
				
				/*
				 * Join that returns the right input object
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> joinDs = 
						ds1.join(ds2)
						   .where(1)
						   .equalTo(1)
						   .with(new RightReturningJoin());
				
				joinDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,1,0,Hallo,1\n" +
						"2,2,1,Hallo Welt,2\n" +
						"2,2,1,Hallo Welt,2\n";
			}
			case 8: {
				
				/*
				 * Join with broadcast set
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Integer> intDs = CollectionDataSets.getIntegerDataSet(env);
				
				DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.getSmall5TupleDataSet(env);
				DataSet<Tuple3<String, String, Integer>> joinDs = 
						ds1.join(ds2)
						   .where(1)
						   .equalTo(4)
						   .with(new T3T5BCJoin())
						   .withBroadcastSet(intDs, "ints");
				
				joinDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "Hi,Hallo,55\n" +
						"Hi,Hallo Welt wie,55\n" +
						"Hello,Hallo Welt,55\n" +
						"Hello world,Hallo Welt,55\n";
			}
			case 9: {
			
				/*
				 * Join on a tuple input with key field selector and a custom type input with key extractor
				 */

				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

				DataSet<CustomType> ds1 = CollectionDataSets.getSmallCustomTypeDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> ds2 = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple2<String, String>> joinDs =
						ds1.join(ds2)
						   .where(new KeySelector<CustomType, Integer>() {
									  @Override
									  public Integer getKey(CustomType value) {
										  return value.myInt;
									  }
								  }
						   )
						   .equalTo(0)
						   .with(new CustT3Join());

				joinDs.writeAsCsv(resultPath);
				env.execute();

				// return expected result
				return "Hi,Hi\n" +
						"Hello,Hello\n" +
						"Hello world,Hello\n";

				}
			case 10: {
				
				/*
				 * Project join on a tuple input 1
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);
				DataSet<Tuple6<String, Long, String, Integer, Long, Long>> joinDs = 
						ds1.join(ds2)
						   .where(1)
						   .equalTo(1)
						   .projectFirst(2,1)
						   .projectSecond(3)
						   .projectFirst(0)
						   .projectSecond(4,1)
						   .types(String.class, Long.class, String.class, Integer.class, Long.class, Long.class);
				
				joinDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "Hi,1,Hallo,1,1,1\n" +
						"Hello,2,Hallo Welt,2,2,2\n" +
						"Hello world,2,Hallo Welt,3,2,2\n";
				
			}
			case 11: {
				
				/*
				 * Project join on a tuple input 2
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);
				DataSet<Tuple6<String, String, Long, Long, Long, Integer>> joinDs = 
						ds1.join(ds2)
						   .where(1)
						   .equalTo(1)
						   .projectSecond(3)
						   .projectFirst(2,1)
						   .projectSecond(4,1)
						   .projectFirst(0)
						   .types(String.class, String.class, Long.class, Long.class, Long.class, Integer.class);
				
				joinDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "Hallo,Hi,1,1,1,1\n" +
						"Hallo Welt,Hello,2,2,2,2\n" +
						"Hallo Welt,Hello world,2,2,2,3\n";
			}
				
			case 12: {
				
				/*
				 * Join on a tuple input with key field selector and a custom type input with key extractor
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
				DataSet<CustomType> ds2 = CollectionDataSets.getCustomTypeDataSet(env);
				DataSet<Tuple2<String, String>> joinDs = 
						ds1.join(ds2)
						   .where(1).equalTo(new KeySelector<CustomType, Long>() {
									   @Override
									   public Long getKey(CustomType value) {
										   return value.myLong;
									   }
								   })
						   .with(new T3CustJoin());
				
				joinDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "Hi,Hello\n" +
						"Hello,Hello world\n" +
						"Hello world,Hello world\n";
						
			}
			
			case 13: {
				
				/*
				 * (Default) Join on two custom type inputs with key extractors
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<CustomType> ds1 = CollectionDataSets.getCustomTypeDataSet(env);
				DataSet<CustomType> ds2 = CollectionDataSets.getSmallCustomTypeDataSet(env);
				
				DataSet<Tuple2<CustomType, CustomType>> joinDs = 
					ds1.join(ds2)
					   .where(
							   new KeySelector<CustomType, Integer>() {
								   @Override
								   public Integer getKey(CustomType value) {
									   return value.myInt;
								   }
							   }
							  )
						.equalTo(
								new KeySelector<CustomType, Integer>() {
									   @Override
									   public Integer getKey(CustomType value) {
										   return value.myInt;
									   }
								   }
								);
																				
				joinDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,0,Hi,1,0,Hi\n" +
						"2,1,Hello,2,1,Hello\n" +
						"2,1,Hello,2,2,Hello world\n" +
						"2,2,Hello world,2,1,Hello\n" +
						"2,2,Hello world,2,2,Hello world\n";
	
			}
			case 14: {
				/*
				 * UDF Join on tuples with tuple-returning key selectors
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);
				DataSet<Tuple2<String, String>> joinDs = 
						ds1.join(ds2)
						   .where(new KeySelector<Tuple3<Integer,Long,String>, Tuple2<Integer, Long>>() {
								private static final long serialVersionUID = 1L;
								
								@Override
								public Tuple2<Integer, Long> getKey(Tuple3<Integer,Long,String> t) {
									return new Tuple2<Integer, Long>(t.f0, t.f1);
								}
							})
						   .equalTo(new KeySelector<Tuple5<Integer,Long,Integer,String,Long>, Tuple2<Integer, Long>>() {
								private static final long serialVersionUID = 1L;
								
								@Override
								public Tuple2<Integer, Long> getKey(Tuple5<Integer,Long,Integer,String,Long> t) {
									return new Tuple2<Integer, Long>(t.f0, t.f4);
								}
							})
						   .with(new T3T5FlatJoin());
				
				joinDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "Hi,Hallo\n" +
						"Hello,Hallo Welt\n" +
						"Hello world,Hallo Welt wie gehts?\n" +
						"Hello world,ABC\n" +
						"I am fine.,HIJ\n" +
						"I am fine.,IJK\n";
			}
			/**
			 *  Joins with POJOs
			 */
			case 15: {
				/*
				 * Join nested pojo against tuple (selected using a string)
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<POJO> ds1 = CollectionDataSets.getSmallPojoDataSet(env);
				DataSet<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> ds2 = CollectionDataSets.getSmallTuplebasedDataSet(env);
				DataSet<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long> >> joinDs = 
						ds1.join(ds2).where("nestedPojo.longNumber").equalTo("f6");
				
				joinDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One,10000)\n" +
					   "2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two,20000)\n" +
					   "3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n";
			}
			
			case 16: {
				/*
				 * Join nested pojo against tuple (selected as an integer)
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<POJO> ds1 = CollectionDataSets.getSmallPojoDataSet(env);
				DataSet<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> ds2 = CollectionDataSets.getSmallTuplebasedDataSet(env);
				DataSet<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long> >> joinDs = 
						ds1.join(ds2).where("nestedPojo.longNumber").equalTo(6); // <--- difference!
				
				joinDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One,10000)\n" +
					   "2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two,20000)\n" +
					   "3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n";
			}
			case 17: {
				/*
				 * selecting multiple fields using expression language
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<POJO> ds1 = CollectionDataSets.getSmallPojoDataSet(env);
				DataSet<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> ds2 = CollectionDataSets.getSmallTuplebasedDataSet(env);
				DataSet<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long> >> joinDs = 
						ds1.join(ds2).where("nestedPojo.longNumber", "number", "str").equalTo("f6","f0","f1");
				
				joinDs.writeAsCsv(resultPath);
				env.setDegreeOfParallelism(1);
				env.execute();
				
				// return expected result
				return "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One,10000)\n" +
					   "2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two,20000)\n" +
					   "3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n";
				
			}
			case 18: {
				/*
				 * nested into tuple
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<POJO> ds1 = CollectionDataSets.getSmallPojoDataSet(env);
				DataSet<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> ds2 = CollectionDataSets.getSmallTuplebasedDataSet(env);
				DataSet<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long> >> joinDs = 
						ds1.join(ds2).where("nestedPojo.longNumber", "number","nestedTupleWithCustom.f0").equalTo("f6","f0","f2");
				
				joinDs.writeAsCsv(resultPath);
				env.setDegreeOfParallelism(1);
				env.execute();
				
				// return expected result
				return "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One,10000)\n" +
					   "2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two,20000)\n" +
					   "3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n";
				
			}
			case 19: {
				/*
				 * nested into tuple into pojo
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<POJO> ds1 = CollectionDataSets.getSmallPojoDataSet(env);
				DataSet<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> ds2 = CollectionDataSets.getSmallTuplebasedDataSet(env);
				DataSet<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long> >> joinDs = 
						ds1.join(ds2).where("nestedTupleWithCustom.f0","nestedTupleWithCustom.f1.myInt","nestedTupleWithCustom.f1.myLong").equalTo("f2","f3","f4");
				
				joinDs.writeAsCsv(resultPath);
				env.setDegreeOfParallelism(1);
				env.execute();
				
				// return expected result
				return "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One,10000)\n" +
					   "2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two,20000)\n" +
					   "3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n";
				
			}
			case 20: {
				/*
				 * Non-POJO test to verify that full-tuple keys are working.
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple2<Tuple2<Integer, Integer>, String>> ds1 = CollectionDataSets.getSmallNestedTupleDataSet(env);
				DataSet<Tuple2<Tuple2<Integer, Integer>, String>> ds2 = CollectionDataSets.getSmallNestedTupleDataSet(env);
				DataSet<Tuple2<Tuple2<Tuple2<Integer, Integer>, String>, Tuple2<Tuple2<Integer, Integer>, String> >> joinDs = 
						ds1.join(ds2).where(0).equalTo("f0.f0", "f0.f1"); // key is now Tuple2<Integer, Integer>
				
				joinDs.writeAsCsv(resultPath);
				env.setDegreeOfParallelism(1);
				env.execute();
				
				// return expected result
				return "((1,1),one),((1,1),one)\n" +
					   "((2,2),two),((2,2),two)\n" +
					   "((3,3),three),((3,3),three)\n";
				
			}
			case 21: {
				/*
				 * Non-POJO test to verify "nested" tuple-element selection.
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple2<Tuple2<Integer, Integer>, String>> ds1 = CollectionDataSets.getSmallNestedTupleDataSet(env);
				DataSet<Tuple2<Tuple2<Integer, Integer>, String>> ds2 = CollectionDataSets.getSmallNestedTupleDataSet(env);
				DataSet<Tuple2<Tuple2<Tuple2<Integer, Integer>, String>, Tuple2<Tuple2<Integer, Integer>, String> >> joinDs = 
						ds1.join(ds2).where("f0.f0").equalTo("f0.f0"); // key is now Integer from Tuple2<Integer, Integer>
				
				joinDs.writeAsCsv(resultPath);
				env.setDegreeOfParallelism(1);
				env.execute();
				
				// return expected result
				return "((1,1),one),((1,1),one)\n" +
					   "((2,2),two),((2,2),two)\n" +
					   "((3,3),three),((3,3),three)\n";
				
			}
			case 22: {
				/*
				 * full pojo with full tuple
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<POJO> ds1 = CollectionDataSets.getSmallPojoDataSet(env);
				DataSet<Tuple7<Long, Integer, Integer, Long, String, Integer, String>> ds2 = CollectionDataSets.getSmallTuplebasedDataSetMatchingPojo(env);
				DataSet<Tuple2<POJO, Tuple7<Long, Integer, Integer, Long, String, Integer, String> >> joinDs = 
						ds1.join(ds2).where("*").equalTo("*");
				
				joinDs.writeAsCsv(resultPath);
				env.setDegreeOfParallelism(1);
				env.execute();
				
				// return expected result
				return "1 First (10,100,1000,One) 10000,(10000,10,100,1000,One,1,First)\n"+
						"2 Second (20,200,2000,Two) 20000,(20000,20,200,2000,Two,2,Second)\n"+
						"3 Third (30,300,3000,Three) 30000,(30000,30,300,3000,Three,3,Third)\n";
			}
			default: 
				throw new IllegalArgumentException("Invalid program id: "+progId);
			}
			
		}
	
	}
	
	public static class T3T5FlatJoin implements FlatJoinFunction<Tuple3<Integer, Long, String>, Tuple5<Integer, Long, Integer, String, Long>, Tuple2<String, String>> {

		@Override
		public void join(Tuple3<Integer, Long, String> first,
				Tuple5<Integer, Long, Integer, String, Long> second,
				Collector<Tuple2<String,String>> out)  {

			out.collect (new Tuple2<String,String> (first.f2, second.f3));
		}

	}
	
	public static class LeftReturningJoin implements JoinFunction<Tuple3<Integer, Long, String>, Tuple5<Integer, Long, Integer, String, Long>, Tuple3<Integer, Long, String>> {

		@Override
		public Tuple3<Integer, Long, String> join(Tuple3<Integer, Long, String> first,
												  Tuple5<Integer, Long, Integer, String, Long> second) {
			
			return first;
		}
	}
	
	public static class RightReturningJoin implements JoinFunction<Tuple3<Integer, Long, String>, Tuple5<Integer, Long, Integer, String, Long>, Tuple5<Integer, Long, Integer, String, Long>> {

		@Override
		public Tuple5<Integer, Long, Integer, String, Long> join(Tuple3<Integer, Long, String> first,
																 Tuple5<Integer, Long, Integer, String, Long> second) {
			
			return second;
		}
	}
		
	public static class T3T5BCJoin extends RichFlatJoinFunction<Tuple3<Integer, Long, String>, Tuple5<Integer, Long, Integer, String, Long>, Tuple3<String, String, Integer>> {

		private int broadcast;
		
		@Override
		public void open(Configuration config) {
			
			Collection<Integer> ints = this.getRuntimeContext().getBroadcastVariable("ints");
			int sum = 0;
			for(Integer i : ints) {
				sum += i;
			}
			broadcast = sum;
			
		}

		/*
		@Override
		public Tuple3<String, String, Integer> join(
				Tuple3<Integer, Long, String> first,
				Tuple5<Integer, Long, Integer, String, Long> second) {

			return new Tuple3<String, String, Integer>(first.f2, second.f3, broadcast);
		}
		*/

		@Override
		public void join(Tuple3<Integer, Long, String> first, Tuple5<Integer, Long, Integer, String, Long> second, Collector<Tuple3<String, String, Integer>> out) throws Exception {
			out.collect(new Tuple3<String, String, Integer> (first.f2, second.f3, broadcast));
		}
	}
	
	public static class T3CustJoin implements JoinFunction<Tuple3<Integer, Long, String>, CustomType, Tuple2<String, String>> {

		@Override
		public Tuple2<String, String> join(Tuple3<Integer, Long, String> first,
										   CustomType second) {

			return new Tuple2<String, String>(first.f2, second.myString);
		}
	}
	
	public static class CustT3Join implements JoinFunction<CustomType, Tuple3<Integer, Long, String>, Tuple2<String, String>> {

		@Override
		public Tuple2<String, String> join(CustomType first, Tuple3<Integer, Long, String> second) {

			return new Tuple2<String, String>(first.myString, second.f2);
		}
	}
}
