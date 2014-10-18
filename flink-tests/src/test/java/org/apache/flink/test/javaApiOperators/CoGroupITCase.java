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
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets.CustomType;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets.POJO;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class CoGroupITCase extends JavaProgramTestBase {
	
	private static int NUM_PROGRAMS = 13;
	
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
				DataSet<Tuple3<Integer, Long, String>> coGroupDs = ds.coGroup(ds2).where(2).equalTo(new KeySelector<CustomType, Integer>() {
									private static final long serialVersionUID = 1L;
									@Override
									public Integer getKey(CustomType in) {
										return in.myInt;
									}
								}).with(new MixedCoGroup());
				
				coGroupDs.writeAsCsv(resultPath);
				env.execute();
				
				// return expected result
				return "0,1,test\n" +
						"1,2,test\n" +
						"2,5,test\n" +
						"3,15,test\n" +
						"4,33,test\n" +
						"5,63,test\n" +
						"6,109,test\n" +
						"7,4,test\n" + 
						"8,4,test\n" + 
						"9,4,test\n" + 
						"10,5,test\n" + 
						"11,5,test\n" + 
						"12,5,test\n" + 
						"13,5,test\n" +
						"14,5,test\n"; 
						
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
								}).equalTo(2).with(new MixedCoGroup2());
				
				coGroupDs.writeAsText(resultPath);
				env.execute();
				
				// return expected result
				return "0,1,test\n" +
						"1,2,test\n" +
						"2,5,test\n" +
						"3,15,test\n" +
						"4,33,test\n" +
						"5,63,test\n" +
						"6,109,test\n" +
						"7,4,test\n" + 
						"8,4,test\n" + 
						"9,4,test\n" + 
						"10,5,test\n" + 
						"11,5,test\n" + 
						"12,5,test\n" + 
						"13,5,test\n" +
						"14,5,test\n"; 
				
			}
			case 8: {
				/*
				 * CoGroup with multiple key fields
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds1 = CollectionDataSets.get5TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> ds2 = CollectionDataSets.get3TupleDataSet(env);
				
				DataSet<Tuple3<Integer, Long, String>> coGrouped = ds1.coGroup(ds2).
						where(0,4).equalTo(0,1).with(new Tuple5Tuple3CoGroup());
				
				coGrouped.writeAsCsv(resultPath);
				env.execute();
				
				return "1,1,Hallo\n" +
						"2,2,Hallo Welt\n" +
						"3,2,Hallo Welt wie gehts?\n" +
						"3,2,ABC\n" +
						"5,3,HIJ\n" +
						"5,3,IJK\n";
			}
			case 9: {
				/*
				 * CoGroup with multiple key fields
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds1 = CollectionDataSets.get5TupleDataSet(env);
				DataSet<Tuple3<Integer, Long, String>> ds2 = CollectionDataSets.get3TupleDataSet(env);
				
				DataSet<Tuple3<Integer, Long, String>> coGrouped = ds1.coGroup(ds2).
						where(new KeySelector<Tuple5<Integer,Long,Integer,String,Long>, Tuple2<Integer, Long>>() {
							private static final long serialVersionUID = 1L;
							
							@Override
							public Tuple2<Integer, Long> getKey(Tuple5<Integer,Long,Integer,String,Long> t) {
								return new Tuple2<Integer, Long>(t.f0, t.f4);
							}
						}).
						equalTo(new KeySelector<Tuple3<Integer,Long,String>, Tuple2<Integer, Long>>() {
							private static final long serialVersionUID = 1L;
							
							@Override
							public Tuple2<Integer, Long> getKey(Tuple3<Integer,Long,String> t) {
								return new Tuple2<Integer, Long>(t.f0, t.f1);
							}
						}).with(new Tuple5Tuple3CoGroup());
				
				coGrouped.writeAsCsv(resultPath);
				env.execute();
				
				return "1,1,Hallo\n" +
						"2,2,Hallo Welt\n" +
						"3,2,Hallo Welt wie gehts?\n" +
						"3,2,ABC\n" +
						"5,3,HIJ\n" +
						"5,3,IJK\n";
			}
			case 10: {
				/*
				 * CoGroup on two custom type inputs using expression keys
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<CustomType> ds = CollectionDataSets.getCustomTypeDataSet(env);
				DataSet<CustomType> ds2 = CollectionDataSets.getCustomTypeDataSet(env);
				DataSet<CustomType> coGroupDs = ds.coGroup(ds2).where("myInt").equalTo("myInt").with(new CustomTypeCoGroup());
				
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
			case 11: {
				/*
				 * CoGroup on two custom type inputs using expression keys
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<POJO> ds = CollectionDataSets.getSmallPojoDataSet(env);
				DataSet<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> ds2 = CollectionDataSets.getSmallTuplebasedDataSet(env);
				DataSet<CustomType> coGroupDs = ds.coGroup(ds2)
						.where("nestedPojo.longNumber").equalTo(6).with(new CoGroupFunction<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>, CustomType>() {
						private static final long serialVersionUID = 1L;

						@Override
						public void coGroup(
								Iterable<POJO> first,
								Iterable<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> second,
								Collector<CustomType> out) throws Exception {
							for(POJO p : first) {
								for(Tuple7<Integer, String, Integer, Integer, Long, String, Long> t: second) {
									Assert.assertTrue(p.nestedPojo.longNumber == t.f6);
									out.collect(new CustomType(-1, p.nestedPojo.longNumber, "Flink"));
								}
							}
						}
				});
				coGroupDs.writeAsText(resultPath);
				env.execute();
				
				// return expected result
				return 	"-1,20000,Flink\n" +
						"-1,10000,Flink\n" +
						"-1,30000,Flink\n";
			}
			case 12: {
				/*
				 * CoGroup field-selector (expression keys) + key selector function
				 * The key selector is unnecessary complicated (Tuple1) ;)
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<POJO> ds = CollectionDataSets.getSmallPojoDataSet(env);
				DataSet<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> ds2 = CollectionDataSets.getSmallTuplebasedDataSet(env);
				DataSet<CustomType> coGroupDs = ds.coGroup(ds2)
						.where(new KeySelector<POJO, Tuple1<Long>>() {
							private static final long serialVersionUID = 1L;

							@Override
							public Tuple1<Long> getKey(POJO value)
									throws Exception {
								return new Tuple1<Long>(value.nestedPojo.longNumber);
							}
						}).equalTo(6).with(new CoGroupFunction<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>, CustomType>() {
							private static final long serialVersionUID = 1L;

						@Override
						public void coGroup(
								Iterable<POJO> first,
								Iterable<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> second,
								Collector<CustomType> out) throws Exception {
							for(POJO p : first) {
								for(Tuple7<Integer, String, Integer, Integer, Long, String, Long> t: second) {
									Assert.assertTrue(p.nestedPojo.longNumber == t.f6);
									out.collect(new CustomType(-1, p.nestedPojo.longNumber, "Flink"));
								}
							}
						}
				});
				coGroupDs.writeAsText(resultPath);
				env.execute();
				
				// return expected result
				return 	"-1,20000,Flink\n" +
						"-1,10000,Flink\n" +
						"-1,30000,Flink\n";
			}
			case 13: {
				/*
				 * CoGroup field-selector (expression keys) + key selector function
				 * The key selector is simple here
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<POJO> ds = CollectionDataSets.getSmallPojoDataSet(env);
				DataSet<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> ds2 = CollectionDataSets.getSmallTuplebasedDataSet(env);
				DataSet<CustomType> coGroupDs = ds.coGroup(ds2)
						.where(new KeySelector<POJO, Long>() {
							private static final long serialVersionUID = 1L;

							@Override
							public Long getKey(POJO value)
									throws Exception {
								return value.nestedPojo.longNumber;
							}
						}).equalTo(6).with(new CoGroupFunction<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>, CustomType>() {
							private static final long serialVersionUID = 1L;

						@Override
						public void coGroup(
								Iterable<POJO> first,
								Iterable<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> second,
								Collector<CustomType> out) throws Exception {
							for(POJO p : first) {
								for(Tuple7<Integer, String, Integer, Integer, Long, String, Long> t: second) {
									Assert.assertTrue(p.nestedPojo.longNumber == t.f6);
									out.collect(new CustomType(-1, p.nestedPojo.longNumber, "Flink"));
								}
							}
						}
				});
				coGroupDs.writeAsText(resultPath);
				env.execute();
				
				// return expected result
				return 	"-1,20000,Flink\n" +
						"-1,10000,Flink\n" +
						"-1,30000,Flink\n";
			}
			
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
			
		}
	
	}
	
	public static class Tuple5CoGroup implements CoGroupFunction<Tuple5<Integer, Long, Integer, String, Long>, Tuple5<Integer, Long, Integer, String, Long>, Tuple2<Integer, Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void coGroup(
				Iterable<Tuple5<Integer, Long, Integer, String, Long>> first,
				Iterable<Tuple5<Integer, Long, Integer, String, Long>> second,
				Collector<Tuple2<Integer, Integer>> out)
		{
			int sum = 0;
			int id = 0;
			
			for ( Tuple5<Integer, Long, Integer, String, Long> element : first ) {
				sum += element.f2;
				id = element.f0;
			}
			
			for ( Tuple5<Integer, Long, Integer, String, Long> element : second ) {
				sum += element.f2;
				id = element.f0;
			}
			
			out.collect(new Tuple2<Integer, Integer>(id, sum));
		}
	}
	
	public static class CustomTypeCoGroup implements CoGroupFunction<CustomType, CustomType, CustomType> {

		private static final long serialVersionUID = 1L;

		@Override
		public void coGroup(Iterable<CustomType> first, Iterable<CustomType> second, Collector<CustomType> out) {
			
			CustomType o = new CustomType(0,0,"test");
			
			for ( CustomType element : first ) {
				o.myInt = element.myInt;
				o.myLong += element.myLong;
			}
			
			for ( CustomType element : second ) {
				o.myInt = element.myInt;
				o.myLong += element.myLong;
			}
			
			out.collect(o);
		}
	}
	
	public static class MixedCoGroup implements CoGroupFunction<Tuple5<Integer, Long, Integer, String, Long>, CustomType, Tuple3<Integer, Long, String>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void coGroup(
				Iterable<Tuple5<Integer, Long, Integer, String, Long>> first,
				Iterable<CustomType> second,
				Collector<Tuple3<Integer, Long, String>> out) throws Exception {
			
			long sum = 0;
			int id = 0;
			
			for ( Tuple5<Integer, Long, Integer, String, Long> element : first ) {
				sum += element.f0;
				id = element.f2;
			}
			
			for (CustomType element : second) {
				id = element.myInt;
				sum += element.myLong;
			}
			
			out.collect(new Tuple3<Integer, Long, String>(id, sum, "test"));
		}
		
	}
	
	public static class MixedCoGroup2 implements CoGroupFunction<CustomType, Tuple5<Integer, Long, Integer, String, Long>, CustomType> {

		private static final long serialVersionUID = 1L;

		@Override
		public void coGroup(Iterable<CustomType> first,
				Iterable<Tuple5<Integer, Long, Integer, String, Long>> second,
				Collector<CustomType> out)
		{
			CustomType o = new CustomType(0,0,"test");
			
			for (CustomType element : first) {
				o.myInt = element.myInt;
				o.myLong += element.myLong;
			}
			
			for (Tuple5<Integer, Long, Integer, String, Long> element : second) {
				o.myInt = element.f2;
				o.myLong += element.f0;
			}
			
			out.collect(o);
			
		}
		
	}
	
	public static class Tuple3ReturnLeft implements CoGroupFunction<Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {
		
		private static final long serialVersionUID = 1L;

		@Override
		public void coGroup(Iterable<Tuple3<Integer, Long, String>> first,
				Iterable<Tuple3<Integer, Long, String>> second,
				Collector<Tuple3<Integer, Long, String>> out)
		{
			for (Tuple3<Integer, Long, String> element : first) {
				if(element.f0 < 6) {
					out.collect(element);
				}
			}
		}
	}
	
	public static class Tuple5ReturnRight implements CoGroupFunction<Tuple5<Integer, Long, Integer, String, Long>, Tuple5<Integer, Long, Integer, String, Long>, Tuple5<Integer, Long, Integer, String, Long>> {
		
		private static final long serialVersionUID = 1L;

		@Override
		public void coGroup(
				Iterable<Tuple5<Integer, Long, Integer, String, Long>> first,
				Iterable<Tuple5<Integer, Long, Integer, String, Long>> second,
				Collector<Tuple5<Integer, Long, Integer, String, Long>> out)
		{
			for (Tuple5<Integer, Long, Integer, String, Long> element : second) {
				if(element.f0 < 4) {
					out.collect(element);
				}
			}
		}
	}
	
	public static class Tuple5CoGroupBC extends RichCoGroupFunction<Tuple5<Integer, Long, Integer, String, Long>, Tuple5<Integer, Long, Integer, String, Long>, Tuple3<Integer, Integer, Integer>> {

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
				Iterable<Tuple5<Integer, Long, Integer, String, Long>> first,
				Iterable<Tuple5<Integer, Long, Integer, String, Long>> second,
				Collector<Tuple3<Integer, Integer, Integer>> out)
		{
			int sum = 0;
			int id = 0;
			
			for (Tuple5<Integer, Long, Integer, String, Long> element : first) {
				sum += element.f2;
				id = element.f0;
			}
			
			for (Tuple5<Integer, Long, Integer, String, Long> element : second) {
				sum += element.f2;
				id = element.f0;
			}
			
			out.collect(new Tuple3<Integer, Integer, Integer>(id, sum, broadcast));
		}
	}
	
	public static class Tuple5Tuple3CoGroup implements CoGroupFunction<Tuple5<Integer, Long, Integer, String, Long>, Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {
		
		private static final long serialVersionUID = 1L;

		@Override
		public void coGroup(Iterable<Tuple5<Integer, Long, Integer, String, Long>> first,
				Iterable<Tuple3<Integer, Long, String>> second,
				Collector<Tuple3<Integer, Long, String>> out)
		{
			List<String> strs = new ArrayList<String>();
			
			for (Tuple5<Integer, Long, Integer, String, Long> t : first) {
				strs.add(t.f3);
			}
			
			for(Tuple3<Integer, Long, String> t : second) {
				for(String s : strs) {
					out.collect(new Tuple3<Integer, Long, String>(t.f0, t.f1, s));
				}
			}
		}
	}
}
