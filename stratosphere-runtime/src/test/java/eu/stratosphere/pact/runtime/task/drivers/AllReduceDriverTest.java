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

package eu.stratosphere.pact.runtime.task.drivers;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.api.common.functions.GenericReduce;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.pact.runtime.task.AllReduceDriver;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.test.util.DiscardingOutputCollector;
import eu.stratosphere.pact.runtime.util.EmptyMutableObjectIterator;
import eu.stratosphere.pact.runtime.util.RegularToMutableObjectIterator;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.TypeInformation;
import eu.stratosphere.util.MutableObjectIterator;

@SuppressWarnings("serial")
public class AllReduceDriverTest {

	@Test
	public void testAllReduceDriverImmutableEmpty() {
		try {
			TestTaskContext<GenericReduce<Tuple2<String, Integer>>, Tuple2<String, Integer>> context =
					new TestTaskContext<GenericReduce<Tuple2<String,Integer>>, Tuple2<String,Integer>>();
			
			List<Tuple2<String, Integer>> data = DriverTestData.createReduceImmutableData();
			TypeInformation<Tuple2<String, Integer>> typeInfo = TypeExtractor.getForObject(data.get(0));
			MutableObjectIterator<Tuple2<String, Integer>> input = EmptyMutableObjectIterator.get();
			context.setDriverStrategy(DriverStrategy.ALL_REDUCE);
			
			context.setInput1(input, typeInfo.createSerializer());
			context.setCollector(new DiscardingOutputCollector<Tuple2<String, Integer>>());
			
			AllReduceDriver<Tuple2<String, Integer>> driver = new AllReduceDriver<Tuple2<String,Integer>>();
			driver.setup(context);
			driver.prepare();
			driver.run();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testAllReduceDriverImmutable() {
		try {
			{
				TestTaskContext<GenericReduce<Tuple2<String, Integer>>, Tuple2<String, Integer>> context =
						new TestTaskContext<GenericReduce<Tuple2<String,Integer>>, Tuple2<String,Integer>>();
				
				List<Tuple2<String, Integer>> data = DriverTestData.createReduceImmutableData();
				TypeInformation<Tuple2<String, Integer>> typeInfo = TypeExtractor.getForObject(data.get(0));
				MutableObjectIterator<Tuple2<String, Integer>> input = new RegularToMutableObjectIterator<Tuple2<String, Integer>>(data.iterator(), typeInfo.createSerializer());
				
				GatheringCollector<Tuple2<String, Integer>> result = new GatheringCollector<Tuple2<String,Integer>>(typeInfo.createSerializer());
				
				context.setDriverStrategy(DriverStrategy.ALL_REDUCE);
				context.setInput1(input, typeInfo.createSerializer());
				context.setCollector(result);
				context.setUdf(new ConcatSumFirstReducer());
				
				AllReduceDriver<Tuple2<String, Integer>> driver = new AllReduceDriver<Tuple2<String,Integer>>();
				driver.setup(context);
				driver.prepare();
				driver.run();
				
				Tuple2<String,Integer> res = result.getList().get(0);
				
				char[] foundString = res.f0.toCharArray();
				Arrays.sort(foundString);
				
				char[] expectedString = "abcddeeeffff".toCharArray();
				Arrays.sort(expectedString);
				
				Assert.assertArrayEquals(expectedString, foundString);
				Assert.assertEquals(78, res.f1.intValue());
			}
			
			{
				TestTaskContext<GenericReduce<Tuple2<String, Integer>>, Tuple2<String, Integer>> context =
						new TestTaskContext<GenericReduce<Tuple2<String,Integer>>, Tuple2<String,Integer>>();
				
				List<Tuple2<String, Integer>> data = DriverTestData.createReduceImmutableData();
				TypeInformation<Tuple2<String, Integer>> typeInfo = TypeExtractor.getForObject(data.get(0));
				MutableObjectIterator<Tuple2<String, Integer>> input = new RegularToMutableObjectIterator<Tuple2<String, Integer>>(data.iterator(), typeInfo.createSerializer());
				
				GatheringCollector<Tuple2<String, Integer>> result = new GatheringCollector<Tuple2<String,Integer>>(typeInfo.createSerializer());
				
				context.setDriverStrategy(DriverStrategy.ALL_REDUCE);
				context.setInput1(input, typeInfo.createSerializer());
				context.setCollector(result);
				context.setUdf(new ConcatSumSecondReducer());
				
				AllReduceDriver<Tuple2<String, Integer>> driver = new AllReduceDriver<Tuple2<String,Integer>>();
				driver.setup(context);
				driver.prepare();
				driver.run();
				
				Tuple2<String,Integer> res = result.getList().get(0);
				
				char[] foundString = res.f0.toCharArray();
				Arrays.sort(foundString);
				
				char[] expectedString = "abcddeeeffff".toCharArray();
				Arrays.sort(expectedString);
				
				Assert.assertArrayEquals(expectedString, foundString);
				Assert.assertEquals(78, res.f1.intValue());
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testAllReduceDriverMutable() {
		try {
			{
				TestTaskContext<GenericReduce<Tuple2<StringValue, IntValue>>, Tuple2<StringValue, IntValue>> context =
						new TestTaskContext<GenericReduce<Tuple2<StringValue, IntValue>>, Tuple2<StringValue, IntValue>>();
				
				List<Tuple2<StringValue, IntValue>> data = DriverTestData.createReduceMutableData();
				TypeInformation<Tuple2<StringValue, IntValue>> typeInfo = TypeExtractor.getForObject(data.get(0));
				MutableObjectIterator<Tuple2<StringValue, IntValue>> input = new RegularToMutableObjectIterator<Tuple2<StringValue, IntValue>>(data.iterator(), typeInfo.createSerializer());
				
				GatheringCollector<Tuple2<StringValue, IntValue>> result = new GatheringCollector<Tuple2<StringValue, IntValue>>(typeInfo.createSerializer());
				
				context.setDriverStrategy(DriverStrategy.ALL_REDUCE);
				context.setInput1(input, typeInfo.createSerializer());
				context.setCollector(result);
				context.setUdf(new ConcatSumFirstMutableReducer());
				
				AllReduceDriver<Tuple2<StringValue, IntValue>> driver = new AllReduceDriver<Tuple2<StringValue, IntValue>>();
				driver.setup(context);
				driver.prepare();
				driver.run();
				
				Tuple2<StringValue, IntValue> res = result.getList().get(0);
				
				char[] foundString = res.f0.getValue().toCharArray();
				Arrays.sort(foundString);
				
				char[] expectedString = "abcddeeeffff".toCharArray();
				Arrays.sort(expectedString);
				
				Assert.assertArrayEquals(expectedString, foundString);
				Assert.assertEquals(78, res.f1.getValue());
			}
			{
				TestTaskContext<GenericReduce<Tuple2<StringValue, IntValue>>, Tuple2<StringValue, IntValue>> context =
						new TestTaskContext<GenericReduce<Tuple2<StringValue, IntValue>>, Tuple2<StringValue, IntValue>>();
				
				List<Tuple2<StringValue, IntValue>> data = DriverTestData.createReduceMutableData();
				TypeInformation<Tuple2<StringValue, IntValue>> typeInfo = TypeExtractor.getForObject(data.get(0));
				MutableObjectIterator<Tuple2<StringValue, IntValue>> input = new RegularToMutableObjectIterator<Tuple2<StringValue, IntValue>>(data.iterator(), typeInfo.createSerializer());
				
				GatheringCollector<Tuple2<StringValue, IntValue>> result = new GatheringCollector<Tuple2<StringValue, IntValue>>(typeInfo.createSerializer());
				
				context.setDriverStrategy(DriverStrategy.ALL_REDUCE);
				context.setInput1(input, typeInfo.createSerializer());
				context.setCollector(result);
				context.setUdf(new ConcatSumSecondMutableReducer());
				
				AllReduceDriver<Tuple2<StringValue, IntValue>> driver = new AllReduceDriver<Tuple2<StringValue, IntValue>>();
				driver.setup(context);
				driver.prepare();
				driver.run();
				
				Tuple2<StringValue, IntValue> res = result.getList().get(0);
				
				char[] foundString = res.f0.getValue().toCharArray();
				Arrays.sort(foundString);
				
				char[] expectedString = "abcddeeeffff".toCharArray();
				Arrays.sort(expectedString);
				
				Assert.assertArrayEquals(expectedString, foundString);
				Assert.assertEquals(78, res.f1.getValue());
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	// --------------------------------------------------------------------------------------------
	//  Test UDFs
	// --------------------------------------------------------------------------------------------
	
	public static final class ConcatSumFirstReducer extends ReduceFunction<Tuple2<String, Integer>> {

		@Override
		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
			value1.f0 = value1.f0 + value2.f0;
			value1.f1 = value1.f1 + value2.f1;
			return value1;
		}
	}
	
	public static final class ConcatSumSecondReducer extends ReduceFunction<Tuple2<String, Integer>> {
		
		@Override
		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
			value2.f0 = value1.f0 + value2.f0;
			value2.f1 = value1.f1 + value2.f1;
			return value2;
		}
	}
	
	public static final class ConcatSumFirstMutableReducer extends ReduceFunction<Tuple2<StringValue, IntValue>> {

		@Override
		public Tuple2<StringValue, IntValue> reduce(Tuple2<StringValue, IntValue> value1, Tuple2<StringValue, IntValue> value2) {
			value1.f0.setValue(value1.f0.getValue() + value2.f0.getValue());
			value1.f1.setValue(value1.f1.getValue() + value2.f1.getValue());
			return value1;
		}
	}
	
	public static final class ConcatSumSecondMutableReducer extends ReduceFunction<Tuple2<StringValue, IntValue>> {

		@Override
		public Tuple2<StringValue, IntValue> reduce(Tuple2<StringValue, IntValue> value1, Tuple2<StringValue, IntValue> value2) {
			value2.f0.setValue(value1.f0.getValue() + value2.f0.getValue());
			value2.f1.setValue(value1.f1.getValue() + value2.f1.getValue());
			return value2;
		}
	}
}
