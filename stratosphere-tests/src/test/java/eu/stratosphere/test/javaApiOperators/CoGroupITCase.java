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

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.CoGroupFunction;
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
public class CoGroupITCase extends JavaProgramTestBase {
	
	private static int NUM_PROGRAMS = 7;
	
	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;
	
	public CoGroupITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		expectedResult = CoGroupProgs.runProgram(curProgId, resultPath);
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
	
	private static class CoGroupProgs {
		
		public static String runProgram(int progId, String resultPath) throws Exception {
			
			switch(progId) {
			case 1: {
				
				/*
				 * CoGroup on tuples with key field selector
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = CollectionDataSets.get5TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);
				DataSet<Tuple2<Integer, Integer>> coGroupDs = ds.coGroup(ds2).where(0).equalTo(0).with(new Tuple5CoGroup());
				
				coGroupDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,0\n" +
						"2,6\n" +
						"3,24\n" +
						"4,60\n" +
						"5,120\n";
			}
			case 2: {
				
				/*
				 * CoGroup on two custom type inputs with key extractors
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<CustomType> ds = CollectionDataSets.getCustomTypeDataSet(env);
				DataSet<CustomType> ds2 = CollectionDataSets.getCustomTypeDataSet(env);
				DataSet<CustomType> coGroupDs = ds.coGroup(ds2).where(new KeySelector<CustomType, Integer>() {
									private static final long serialVersionUID = 1L;
									@Override
									public Integer getKey(CustomType in) {
										return in.myInt;
									}
								}).equalTo(new KeySelector<CustomType, Integer>() {
									private static final long serialVersionUID = 1L;
									@Override
									public Integer getKey(CustomType in) {
										return in.myInt;
									}
								}).with(new CustomTypeCoGroup());
				
				coGroupDs.writeAsText(resultPath);
				env.execute();
				
				// return expected result
				return "1,0,test\n" +
						"2,6,test\n" +
						"3,24,test\n" +
						"4,60,test\n" +
						"5,120,test\n" +
						"6,210,test\n";
			}
			case 3: {
				
				/*
				 * check correctness of cogroup if UDF returns left input objects
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> ds2 = CollectionDataSets.get3TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> coGroupDs = ds.coGroup(ds2).where(0).equalTo(0).with(new Tuple3ReturnLeft());
				
				coGroupDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,1,Hi\n" +
						"2,2,Hello\n" +
						"3,2,Hello world\n" +
						"4,3,Hello world, how are you?\n" +
						"5,3,I am fine.\n";
				
			}
			case 4: {
				
				/*
				 * check correctness of cogroup if UDF returns right input objects
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = CollectionDataSets.get5TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> coGroupDs = ds.coGroup(ds2).where(0).equalTo(0).with(new Tuple5ReturnRight());
				
				coGroupDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,1,0,Hallo,1\n" +
						"2,2,1,Hallo Welt,2\n" +
						"2,3,2,Hallo Welt wie,1\n" +
						"3,4,3,Hallo Welt wie gehts?,2\n" +
						"3,5,4,ABC,2\n" +
						"3,6,5,BCD,3\n";
				
			}
			case 5: {
				
				/*
				 * Reduce with broadcast set
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Integer> intDs = CollectionDataSets.getIntegerDataSet(env);
				
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = CollectionDataSets.get5TupleDataSet(env);
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);
				DataSet<Tuple3<Integer, Integer, Integer>> coGroupDs = ds.coGroup(ds2).where(0).equalTo(0).with(new Tuple5CoGroupBC()).withBroadcastSet(intDs, "ints");
				
				coGroupDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,0,55\n" +
						"2,6,55\n" +
						"3,24,55\n" +
						"4,60,55\n" +
						"5,120,55\n";
			}
			case 6: {
				
				/*
				 * CoGroup on a tuple input with key field selector and a custom type input with key extractor
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = CollectionDataSets.get5TupleDataSet(env);
				DataSet<CustomType> ds2 = CollectionDataSets.getCustomTypeDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> coGroupDs = ds.coGroup(ds2).where(0).equalTo(new KeySelector<CustomType, Integer>() {
									private static final long serialVersionUID = 1L;
									@Override
									public Integer getKey(CustomType in) {
										return in.myInt;
									}
								}).with(new MixedCoGroup());
				
				coGroupDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "1,0,test\n" +
						"2,6,test\n" +
						"3,24,test\n" +
						"4,60,test\n" +
						"5,120,test\n" +
						"6,105,test\n";
				
			}
			case 7: {
				
				/*
				 * CoGroup on a tuple input with key field selector and a custom type input with key extractor
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = CollectionDataSets.get5TupleDataSet(env);
				DataSet<CustomType> ds2 = CollectionDataSets.getCustomTypeDataSet(env);
				DataSet<CustomType> coGroupDs = ds2.coGroup(ds).where(new KeySelector<CustomType, Integer>() {
									private static final long serialVersionUID = 1L;
									@Override
									public Integer getKey(CustomType in) {
										return in.myInt;
									}
								}).equalTo(0).with(new MixedCoGroup2());
				
				coGroupDs.writeAsText(resultPath);
				env.execute();
				
				// return expected result
				return "1,0,test\n" +
						"2,6,test\n" +
						"3,24,test\n" +
						"4,60,test\n" +
						"5,120,test\n" +
						"6,105,test\n";
				
			}
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
			
		}
	
	}
	
	public static class Tuple5CoGroup extends CoGroupFunction<Tuple5<Integer, Long, Integer, String, Long>, Tuple5<Integer, Long, Integer, String, Long>, Tuple2<Integer, Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void coGroup(
				Iterator<Tuple5<Integer, Long, Integer, String, Long>> first,
				Iterator<Tuple5<Integer, Long, Integer, String, Long>> second,
				Collector<Tuple2<Integer, Integer>> out) throws Exception {
			
			int sum = 0;
			int id = 0;
			
			while(first.hasNext()) {
				Tuple5<Integer, Long, Integer, String, Long> element = first.next();
				sum += element.f2;
				id = element.f0;
			}
			
			while(second.hasNext()) {
				Tuple5<Integer, Long, Integer, String, Long> element = second.next();
				sum += element.f2;
				id = element.f0;
			}
			
			out.collect(new Tuple2<Integer, Integer>(id, sum));
		}
	}
	
	public static class CustomTypeCoGroup extends CoGroupFunction<CustomType, CustomType, CustomType> {

		private static final long serialVersionUID = 1L;

		@Override
		public void coGroup(Iterator<CustomType> first,
				Iterator<CustomType> second, Collector<CustomType> out)
				throws Exception {
			
			CustomType o = new CustomType(0,0,"test");
			
			while(first.hasNext()) {
				CustomType element = first.next();
				o.myInt = element.myInt;
				o.myLong += element.myLong;
			}
			
			while(second.hasNext()) {
				CustomType element = second.next();
				o.myInt = element.myInt;
				o.myLong += element.myLong;
			}
			
			out.collect(o);
		}
		
	}
	
	public static class MixedCoGroup extends CoGroupFunction<Tuple5<Integer, Long, Integer, String, Long>, CustomType, Tuple3<Integer, Long, String>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void coGroup(
				Iterator<Tuple5<Integer, Long, Integer, String, Long>> first,
				Iterator<CustomType> second,
				Collector<Tuple3<Integer, Long, String>> out) throws Exception {
			
			long sum = 0;
			int id = 0;
			
			while(first.hasNext()) {
				Tuple5<Integer, Long, Integer, String, Long> element = first.next();
				sum += element.f2;
				id = element.f0;
			}
			
			while(second.hasNext()) {
				CustomType element = second.next();
				id = element.myInt;
				sum += element.myLong;
			}
			
			out.collect(new Tuple3<Integer, Long, String>(id, sum, "test"));
		}
		
	}
	
	public static class MixedCoGroup2 extends CoGroupFunction<CustomType, Tuple5<Integer, Long, Integer, String, Long>, CustomType> {

		private static final long serialVersionUID = 1L;

		@Override
		public void coGroup(Iterator<CustomType> first,
				Iterator<Tuple5<Integer, Long, Integer, String, Long>> second,
				Collector<CustomType> out) throws Exception {
			
			CustomType o = new CustomType(0,0,"test");
			
			while(first.hasNext()) {
				CustomType element = first.next();
				o.myInt = element.myInt;
				o.myLong += element.myLong;
			}
			
			while(second.hasNext()) {
				Tuple5<Integer, Long, Integer, String, Long> element = second.next();
				o.myInt = element.f0;
				o.myLong += element.f2;
			}
			
			out.collect(o);
			
		}
		
	}
	
	public static class Tuple3ReturnLeft extends CoGroupFunction<Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {
		
		private static final long serialVersionUID = 1L;

		@Override
		public void coGroup(Iterator<Tuple3<Integer, Long, String>> first,
				Iterator<Tuple3<Integer, Long, String>> second,
				Collector<Tuple3<Integer, Long, String>> out) throws Exception {

			while(first.hasNext()) {
				Tuple3<Integer, Long, String> element = first.next();
				if(element.f0 < 6)
					out.collect(element);
			}
		}
	}
	
	public static class Tuple5ReturnRight extends CoGroupFunction<Tuple5<Integer, Long, Integer, String, Long>, Tuple5<Integer, Long, Integer, String, Long>, Tuple5<Integer, Long, Integer, String, Long>> {
		
		private static final long serialVersionUID = 1L;

		@Override
		public void coGroup(
				Iterator<Tuple5<Integer, Long, Integer, String, Long>> first,
				Iterator<Tuple5<Integer, Long, Integer, String, Long>> second,
				Collector<Tuple5<Integer, Long, Integer, String, Long>> out)
				throws Exception {

			while(second.hasNext()) {
				Tuple5<Integer, Long, Integer, String, Long> element = second.next();
				if(element.f0 < 4)
					out.collect(element);
			}
			
		}


	}
	
	public static class Tuple5CoGroupBC extends CoGroupFunction<Tuple5<Integer, Long, Integer, String, Long>, Tuple5<Integer, Long, Integer, String, Long>, Tuple3<Integer, Integer, Integer>> {

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
		public void coGroup(
				Iterator<Tuple5<Integer, Long, Integer, String, Long>> first,
				Iterator<Tuple5<Integer, Long, Integer, String, Long>> second,
				Collector<Tuple3<Integer, Integer, Integer>> out) throws Exception {
			
			int sum = 0;
			int id = 0;
			
			while(first.hasNext()) {
				Tuple5<Integer, Long, Integer, String, Long> element = first.next();
				sum += element.f2;
				id = element.f0;
			}
			
			while(second.hasNext()) {
				Tuple5<Integer, Long, Integer, String, Long> element = second.next();
				sum += element.f2;
				id = element.f0;
			}
			
			out.collect(new Tuple3<Integer, Integer, Integer>(id, sum, broadcast));
		}
	}
}
