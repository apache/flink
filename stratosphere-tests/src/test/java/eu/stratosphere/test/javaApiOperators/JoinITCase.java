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
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.KeySelector;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple5;
import eu.stratosphere.api.java.tuple.Tuple6;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.javaApiOperators.util.CollectionDataSets;
import eu.stratosphere.test.javaApiOperators.util.CollectionDataSets.CustomType;
import eu.stratosphere.test.util.JavaProgramTestBase;

@SuppressWarnings("serial")
@RunWith(Parameterized.class)
public class JoinITCase extends JavaProgramTestBase {
	
	private static int NUM_PROGRAMS = 13;
	
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
						.with(new T3T5Join());
				
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
						   .with(new T3T5Join());
				
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
						   .where(1)
						   .equalTo(1);															
				
				joinDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "(1, 1, Hi),(1, 1, 0, Hallo, 1)\n" +
						"(2, 2, Hello),(2, 2, 1, Hallo Welt, 2)\n" +
						"(3, 2, Hello world),(2, 2, 1, Hallo Welt, 2)\n";
			
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
															.with(new T3T5Join());
				
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
						   .with(new T3T5Join());
				
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
				@SuppressWarnings("serial")
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
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
			
		}
	
	}
	
	public static class T3T5Join extends JoinFunction<Tuple3<Integer, Long, String>, Tuple5<Integer, Long, Integer, String, Long>, Tuple2<String, String>> {

		@Override
		public Tuple2<String, String> join(Tuple3<Integer, Long, String> first,
				Tuple5<Integer, Long, Integer, String, Long> second)  {
			
			return new Tuple2<String,String>(first.f2, second.f3);
		}
		
	}
	
	public static class LeftReturningJoin extends JoinFunction<Tuple3<Integer, Long, String>, Tuple5<Integer, Long, Integer, String, Long>, Tuple3<Integer, Long, String>> {

		@Override
		public Tuple3<Integer, Long, String> join(Tuple3<Integer, Long, String> first,
				Tuple5<Integer, Long, Integer, String, Long> second) {
			
			return first;
		}
	}
	
	public static class RightReturningJoin extends JoinFunction<Tuple3<Integer, Long, String>, Tuple5<Integer, Long, Integer, String, Long>, Tuple5<Integer, Long, Integer, String, Long>> {

		@Override
		public Tuple5<Integer, Long, Integer, String, Long> join(Tuple3<Integer, Long, String> first,
				Tuple5<Integer, Long, Integer, String, Long> second) {
			
			return second;
		}
	}
		
	public static class T3T5BCJoin extends JoinFunction<Tuple3<Integer, Long, String>, Tuple5<Integer, Long, Integer, String, Long>, Tuple3<String, String, Integer>> {

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

		@Override
		public Tuple3<String, String, Integer> join(
				Tuple3<Integer, Long, String> first,
				Tuple5<Integer, Long, Integer, String, Long> second) {

			return new Tuple3<String, String, Integer>(first.f2, second.f3, broadcast);
		}
	}
	
	public static class T3CustJoin extends JoinFunction<Tuple3<Integer, Long, String>, CustomType, Tuple2<String, String>> {

		@Override
		public Tuple2<String, String> join(Tuple3<Integer, Long, String> first,
				CustomType second) {

			return new Tuple2<String, String>(first.f2, second.myString);
		}
	}
	
	public static class CustT3Join extends JoinFunction<CustomType, Tuple3<Integer, Long, String>, Tuple2<String, String>> {

		@Override
		public Tuple2<String, String> join(CustomType first, Tuple3<Integer, Long, String> second) {

			return new Tuple2<String, String>(first.myString, second.f2);
		}
	}
}
