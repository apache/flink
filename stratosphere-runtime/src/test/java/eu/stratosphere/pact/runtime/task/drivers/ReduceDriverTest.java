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

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.api.common.functions.GenericReduce;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.task.ReduceDriver;
import eu.stratosphere.pact.runtime.util.EmptyMutableObjectIterator;
import eu.stratosphere.pact.runtime.util.RegularToMutableObjectIterator;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.MutableObjectIterator;

@SuppressWarnings("serial")
public class ReduceDriverTest {

	@Test
	public void testReduceDriverImmutableEmpty() {
		try {
			TestTaskContext<GenericReduce<Tuple2<String, Integer>>, Tuple2<String, Integer>> context =
					new TestTaskContext<GenericReduce<Tuple2<String,Integer>>, Tuple2<String,Integer>>();
			
			List<Tuple2<String, Integer>> data = DriverTestData.createReduceImmutableData();
			TupleTypeInfo<Tuple2<String, Integer>> typeInfo = (TupleTypeInfo<Tuple2<String, Integer>>) TypeExtractor.getForObject(data.get(0));
			MutableObjectIterator<Tuple2<String, Integer>> input = EmptyMutableObjectIterator.get();
			context.setDriverStrategy(DriverStrategy.SORTED_REDUCE);
			TypeComparator<Tuple2<String, Integer>> comparator = typeInfo.createComparator(new int[]{0}, new boolean[] {true});
			
			GatheringCollector<Tuple2<String, Integer>> result = new GatheringCollector<Tuple2<String,Integer>>(typeInfo.createSerializer());
			
			context.setInput1(input, typeInfo.createSerializer());
			context.setComparator1(comparator);
			context.setCollector(result);
			
			ReduceDriver<Tuple2<String, Integer>> driver = new ReduceDriver<Tuple2<String,Integer>>();
			driver.setup(context);
			driver.prepare();
			driver.run();
			
			Assert.assertEquals(0, result.getList().size());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testReduceDriverImmutable() {
		try {
			{
				TestTaskContext<GenericReduce<Tuple2<String, Integer>>, Tuple2<String, Integer>> context =
						new TestTaskContext<GenericReduce<Tuple2<String,Integer>>, Tuple2<String,Integer>>();
				
				List<Tuple2<String, Integer>> data = DriverTestData.createReduceImmutableData();
				TupleTypeInfo<Tuple2<String, Integer>> typeInfo = (TupleTypeInfo<Tuple2<String, Integer>>) TypeExtractor.getForObject(data.get(0));
				MutableObjectIterator<Tuple2<String, Integer>> input = new RegularToMutableObjectIterator<Tuple2<String, Integer>>(data.iterator(), typeInfo.createSerializer());
				TypeComparator<Tuple2<String, Integer>> comparator = typeInfo.createComparator(new int[]{0}, new boolean[] {true});
				
				GatheringCollector<Tuple2<String, Integer>> result = new GatheringCollector<Tuple2<String,Integer>>(typeInfo.createSerializer());
				
				context.setDriverStrategy(DriverStrategy.SORTED_REDUCE);
				context.setInput1(input, typeInfo.createSerializer());
				context.setComparator1(comparator);
				context.setCollector(result);
				context.setUdf(new ConcatSumFirstReducer());
				
				ReduceDriver<Tuple2<String, Integer>> driver = new ReduceDriver<Tuple2<String,Integer>>();
				driver.setup(context);
				driver.prepare();
				driver.run();
				
				Object[] res = result.getList().toArray();
				Object[] expected = DriverTestData.createReduceImmutableDataGroupedResult().toArray();
				
				DriverTestData.compareTupleArrays(expected, res);
			}
			
			{
				TestTaskContext<GenericReduce<Tuple2<String, Integer>>, Tuple2<String, Integer>> context =
						new TestTaskContext<GenericReduce<Tuple2<String,Integer>>, Tuple2<String,Integer>>();
				
				List<Tuple2<String, Integer>> data = DriverTestData.createReduceImmutableData();
				TupleTypeInfo<Tuple2<String, Integer>> typeInfo = (TupleTypeInfo<Tuple2<String, Integer>>) TypeExtractor.getForObject(data.get(0));
				MutableObjectIterator<Tuple2<String, Integer>> input = new RegularToMutableObjectIterator<Tuple2<String, Integer>>(data.iterator(), typeInfo.createSerializer());
				TypeComparator<Tuple2<String, Integer>> comparator = typeInfo.createComparator(new int[]{0}, new boolean[] {true});
				
				GatheringCollector<Tuple2<String, Integer>> result = new GatheringCollector<Tuple2<String,Integer>>(typeInfo.createSerializer());
				
				context.setDriverStrategy(DriverStrategy.SORTED_REDUCE);
				context.setInput1(input, typeInfo.createSerializer());
				context.setComparator1(comparator);
				context.setCollector(result);
				context.setUdf(new ConcatSumSecondReducer());
				
				ReduceDriver<Tuple2<String, Integer>> driver = new ReduceDriver<Tuple2<String,Integer>>();
				driver.setup(context);
				driver.prepare();
				driver.run();
				
				Object[] res = result.getList().toArray();
				Object[] expected = DriverTestData.createReduceImmutableDataGroupedResult().toArray();
				
				DriverTestData.compareTupleArrays(expected, res);
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testReduceDriverMutable() {
		try {
			{
				TestTaskContext<GenericReduce<Tuple2<StringValue, IntValue>>, Tuple2<StringValue, IntValue>> context =
						new TestTaskContext<GenericReduce<Tuple2<StringValue, IntValue>>, Tuple2<StringValue, IntValue>>();
				
				List<Tuple2<StringValue, IntValue>> data = DriverTestData.createReduceMutableData();
				TupleTypeInfo<Tuple2<StringValue, IntValue>> typeInfo = (TupleTypeInfo<Tuple2<StringValue, IntValue>>) TypeExtractor.getForObject(data.get(0));
				MutableObjectIterator<Tuple2<StringValue, IntValue>> input = new RegularToMutableObjectIterator<Tuple2<StringValue, IntValue>>(data.iterator(), typeInfo.createSerializer());
				TypeComparator<Tuple2<StringValue, IntValue>> comparator = typeInfo.createComparator(new int[]{0}, new boolean[] {true});
				
				GatheringCollector<Tuple2<StringValue, IntValue>> result = new GatheringCollector<Tuple2<StringValue, IntValue>>(typeInfo.createSerializer());
				
				context.setDriverStrategy(DriverStrategy.SORTED_REDUCE);
				context.setInput1(input, typeInfo.createSerializer());
				context.setComparator1(comparator);
				context.setCollector(result);
				context.setUdf(new ConcatSumFirstMutableReducer());
				
				ReduceDriver<Tuple2<StringValue, IntValue>> driver = new ReduceDriver<Tuple2<StringValue, IntValue>>();
				driver.setup(context);
				driver.prepare();
				driver.run();
				
				Object[] res = result.getList().toArray();
				Object[] expected = DriverTestData.createReduceMutableDataGroupedResult().toArray();
				
				DriverTestData.compareTupleArrays(expected, res);
			}
			{
				TestTaskContext<GenericReduce<Tuple2<StringValue, IntValue>>, Tuple2<StringValue, IntValue>> context =
						new TestTaskContext<GenericReduce<Tuple2<StringValue, IntValue>>, Tuple2<StringValue, IntValue>>();
				
				List<Tuple2<StringValue, IntValue>> data = DriverTestData.createReduceMutableData();
				TupleTypeInfo<Tuple2<StringValue, IntValue>> typeInfo = (TupleTypeInfo<Tuple2<StringValue, IntValue>>) TypeExtractor.getForObject(data.get(0));
				MutableObjectIterator<Tuple2<StringValue, IntValue>> input = new RegularToMutableObjectIterator<Tuple2<StringValue, IntValue>>(data.iterator(), typeInfo.createSerializer());
				TypeComparator<Tuple2<StringValue, IntValue>> comparator = typeInfo.createComparator(new int[]{0}, new boolean[] {true});
				
				GatheringCollector<Tuple2<StringValue, IntValue>> result = new GatheringCollector<Tuple2<StringValue, IntValue>>(typeInfo.createSerializer());
				
				context.setDriverStrategy(DriverStrategy.SORTED_REDUCE);
				context.setInput1(input, typeInfo.createSerializer());
				context.setComparator1(comparator);
				context.setCollector(result);
				context.setUdf(new ConcatSumSecondMutableReducer());
				
				ReduceDriver<Tuple2<StringValue, IntValue>> driver = new ReduceDriver<Tuple2<StringValue, IntValue>>();
				driver.setup(context);
				driver.prepare();
				driver.run();
				
				Object[] res = result.getList().toArray();
				Object[] expected = DriverTestData.createReduceMutableDataGroupedResult().toArray();
				
				DriverTestData.compareTupleArrays(expected, res);
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
