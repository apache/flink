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
import eu.stratosphere.api.java.functions.CrossFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple5;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.javaApiOperators.util.CollectionDataSets;
import eu.stratosphere.test.javaApiOperators.util.CollectionDataSets.CustomType;
import eu.stratosphere.test.util.JavaProgramTestBase;

@RunWith(Parameterized.class)
public class CrossITCase extends JavaProgramTestBase {
	
	private static int NUM_PROGRAMS = 6;
	
	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;
	
	public CrossITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		expectedResult = CrossProgs.runProgram(curProgId, resultPath);
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
	
	private static class CrossProgs {
		
		public static String runProgram(int progId, String resultPath) throws Exception {
			
			switch(progId) {
			case 1: {
				
				/*
				 * check correctness of cross on two tuple inputs 
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = CollectionDataSets.getSmall5TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.getSmall5TupleDataSet(env);
				DataSet<Tuple2<Integer, String>> crossDs = ds.cross(ds2).with(new Tuple5Cross());
				
				crossDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "0,HalloHallo\n" +
						"1,HalloHallo Welt\n" +
						"2,HalloHallo Welt wie\n" +
						"1,Hallo WeltHallo\n" +
						"2,Hallo WeltHallo Welt\n" +
						"3,Hallo WeltHallo Welt wie\n" +
						"2,Hallo Welt wieHallo\n" +
						"3,Hallo Welt wieHallo Welt\n" +
						"4,Hallo Welt wieHallo Welt wie\n";
			}
			case 2: {
				
				/*
				 * check correctness of cross if UDF returns left input object
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.getSmall3TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.getSmall5TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> coGroupDs = ds.cross(ds2).with(new Tuple3ReturnLeft());
				
				coGroupDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,1,Hi\n" +
						"1,1,Hi\n" +
						"1,1,Hi\n" +
						"2,2,Hello\n" +
						"2,2,Hello\n" +
						"2,2,Hello\n" +
						"3,2,Hello world\n" +
						"3,2,Hello world\n" +
						"3,2,Hello world\n";
				
			}
			case 3: {
				
				/*
				 * check correctness of cross if UDF returns right input object
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.getSmall3TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.getSmall5TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> coGroupDs = ds.cross(ds2).with(new Tuple5ReturnRight());
				
				coGroupDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,1,0,Hallo,1\n" +
						"1,1,0,Hallo,1\n" +
						"1,1,0,Hallo,1\n" +
						"2,2,1,Hallo Welt,2\n" +
						"2,2,1,Hallo Welt,2\n" +
						"2,2,1,Hallo Welt,2\n" +
						"2,3,2,Hallo Welt wie,1\n" +
						"2,3,2,Hallo Welt wie,1\n" +
						"2,3,2,Hallo Welt wie,1\n";
				
			}
			case 4: {
				
				/*
				 * check correctness of cross with broadcast set
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Integer> intDs = CollectionDataSets.getIntegerDataSet(env);
				
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = CollectionDataSets.getSmall5TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.getSmall5TupleDataSet(env);
				DataSet<Tuple3<Integer, Integer, Integer>> coGroupDs = ds.cross(ds2).with(new Tuple5CrossBC()).withBroadcastSet(intDs, "ints");
				
				coGroupDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "2,0,55\n" +
						"3,0,55\n" +
						"3,0,55\n" +
						"3,0,55\n" +
						"4,1,55\n" +
						"4,2,55\n" +
						"3,0,55\n" +
						"4,2,55\n" +
						"4,4,55\n";
			}
			case 5: {
				
				/*
				 * check correctness of crossWithHuge (only correctness of result -> should be the same as with normal cross)
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = CollectionDataSets.getSmall5TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.getSmall5TupleDataSet(env);
				DataSet<Tuple2<Integer, String>> crossDs = ds.crossWithHuge(ds2).with(new Tuple5Cross());
				
				crossDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "0,HalloHallo\n" +
						"1,HalloHallo Welt\n" +
						"2,HalloHallo Welt wie\n" +
						"1,Hallo WeltHallo\n" +
						"2,Hallo WeltHallo Welt\n" +
						"3,Hallo WeltHallo Welt wie\n" +
						"2,Hallo Welt wieHallo\n" +
						"3,Hallo Welt wieHallo Welt\n" +
						"4,Hallo Welt wieHallo Welt wie\n";
				
			}
			case 6: {
				
				/*
				 * check correctness of crossWithTiny (only correctness of result -> should be the same as with normal cross)
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = CollectionDataSets.getSmall5TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.getSmall5TupleDataSet(env);
				DataSet<Tuple2<Integer, String>> crossDs = ds.crossWithTiny(ds2).with(new Tuple5Cross());
				
				crossDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "0,HalloHallo\n" +
						"1,HalloHallo Welt\n" +
						"2,HalloHallo Welt wie\n" +
						"1,Hallo WeltHallo\n" +
						"2,Hallo WeltHallo Welt\n" +
						"3,Hallo WeltHallo Welt wie\n" +
						"2,Hallo Welt wieHallo\n" +
						"3,Hallo Welt wieHallo Welt\n" +
						"4,Hallo Welt wieHallo Welt wie\n";
				
			}
			// TODO Currently not working because AvroSerializer does not implement copy()
//			case 7: {
//				
//				/*
//				 * check correctness of cross on two custom type inputs
//				 */
//				
//				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//				
//				DataSet<CustomType> ds = CollectionDataSets.getSmallCustomTypeDataSet(env);
//				DataSet<CustomType> ds2 = CollectionDataSets.getSmallCustomTypeDataSet(env);
//				DataSet<CustomType> crossDs = ds.cross(ds2).with(new CustomTypeCross());
//				
//				crossDs.writeAsText(resultPath);
//				env.execute();
//				
//				// return expected result
//				return "1,0,HalloHallo\n" +
//						"2,1,HalloHallo Welt\n" +
//						"2,2,HalloHallo Welt wie\n" +
//						"2,1,Hallo WeltHallo\n" +
//						"4,2,Hallo WeltHallo Welt\n" +
//						"4,3,Hallo WeltHallo Welt wie\n" +
//						"2,2,Hallo Welt wieHallo\n" +
//						"4,3,Hallo Welt wieHallo Welt\n" +
//						",44,Hallo Welt wieHallo Welt wie\n";
//			}
			// TODO Currently not working because AvroSerializer does not implement copy()
//			case 8: {
//				
//				/*
//				 * check correctness of cross a tuple input and a custom type input
//				 */
//				
//				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//				
//				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = CollectionDataSets.get5TupleDataSet(env);
//				DataSet<CustomType> ds2 = CollectionDataSets.getCustomTypeDataSet(env);
//				DataSet<Tuple3<Integer, Long, String>> coGroupDs = ds.cross(ds2).with(new MixedCross());
//				
//				coGroupDs.writeAsCsv(resultPath);
//				env.execute();
//				
//				// return expected result
//				return "2,0,HalloHi\n" +
//						"3,0,HalloHello\n" +
//						"3,0,HalloHello world\n" +
//						"3,0,Hallo WeltHi\n" +
//						"4,1,Hallo WeltHello\n" +
//						"4,2,Hallo WeltHello world\n" +
//						"3,0,Hallo Welt wieHi\n" +
//						"4,2,Hallo Welt wieHello\n" +
//						"4,4,Hallo Welt wieHello world\n";
//				
//			}
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
			
		}
	
	}
	
	public static class Tuple5Cross extends CrossFunction<Tuple5<Integer, Long, Integer, String, Long>, Tuple5<Integer, Long, Integer, String, Long>, Tuple2<Integer, String>> {

		private static final long serialVersionUID = 1L;

		
		@Override
		public Tuple2<Integer, String> cross(
				Tuple5<Integer, Long, Integer, String, Long> first,
				Tuple5<Integer, Long, Integer, String, Long> second)
				throws Exception {
			
				return new Tuple2<Integer, String>(first.f2+second.f2, first.f3+second.f3);
		}

	}
	
	public static class CustomTypeCross extends CrossFunction<CustomType, CustomType, CustomType> {

		private static final long serialVersionUID = 1L;

		@Override
		public CustomType cross(CustomType first, CustomType second)
				throws Exception {
			
			return new CustomType(first.myInt * second.myInt, first.myLong + second.myLong, first.myString + second.myString);
		}
		
	}
	
	public static class MixedCross extends CrossFunction<Tuple5<Integer, Long, Integer, String, Long>, CustomType, Tuple3<Integer, Long, String>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple3<Integer, Long, String> cross(
				Tuple5<Integer, Long, Integer, String, Long> first,
				CustomType second) throws Exception {

			return new Tuple3<Integer, Long, String>(first.f0 + second.myInt, first.f2 * second.myLong, first.f3 + second.myString);
		}
		
	}
	
	
	public static class Tuple3ReturnLeft extends CrossFunction<Tuple3<Integer, Long, String>, Tuple5<Integer, Long, Integer, String, Long>, Tuple3<Integer, Long, String>> {
		
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple3<Integer, Long, String> cross(
				Tuple3<Integer, Long, String> first,
				Tuple5<Integer, Long, Integer, String, Long> second) throws Exception {

			return first;
		}
	}
	
	public static class Tuple5ReturnRight extends CrossFunction<Tuple3<Integer, Long, String>, Tuple5<Integer, Long, Integer, String, Long>, Tuple5<Integer, Long, Integer, String, Long>> {
		
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple5<Integer, Long, Integer, String, Long> cross(
				Tuple3<Integer, Long, String> first,
				Tuple5<Integer, Long, Integer, String, Long> second)
				throws Exception {
			
			return second;
		}


	}
	
	public static class Tuple5CrossBC extends CrossFunction<Tuple5<Integer, Long, Integer, String, Long>, Tuple5<Integer, Long, Integer, String, Long>, Tuple3<Integer, Integer, Integer>> {

		private static final long serialVersionUID = 1L;
		
		private int broadcast = 42;
		
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
		public Tuple3<Integer, Integer, Integer> cross(
				Tuple5<Integer, Long, Integer, String, Long> first,
				Tuple5<Integer, Long, Integer, String, Long> second)
				throws Exception {

			return new Tuple3<Integer, Integer, Integer>(first.f0 + second.f0, first.f2 * second.f2, broadcast);
		}
	}
}
