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

import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.api.common.functions.GenericGroupReduce;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.task.GroupReduceDriver;
import eu.stratosphere.pact.runtime.test.util.DiscardingOutputCollector;
import eu.stratosphere.pact.runtime.util.EmptyMutableObjectIterator;
import eu.stratosphere.pact.runtime.util.RegularToMutableObjectIterator;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.MutableObjectIterator;

@SuppressWarnings("serial")
public class GroupReduceDriverTest {

	@Test
	public void testAllReduceDriverImmutableEmpty() {
		try {
			TestTaskContext<GenericGroupReduce<Tuple2<String, Integer>, Tuple2<String, Integer>>, Tuple2<String, Integer>> context =
					new TestTaskContext<GenericGroupReduce<Tuple2<String, Integer>, Tuple2<String, Integer>>, Tuple2<String,Integer>>();
			
			List<Tuple2<String, Integer>> data = DriverTestData.createReduceImmutableData();
			TupleTypeInfo<Tuple2<String, Integer>> typeInfo = (TupleTypeInfo<Tuple2<String, Integer>>) TypeExtractor.getForObject(data.get(0));
			MutableObjectIterator<Tuple2<String, Integer>> input = EmptyMutableObjectIterator.get();
			TypeComparator<Tuple2<String, Integer>> comparator = typeInfo.createComparator(new int[]{0}, new boolean[] {true});
			context.setDriverStrategy(DriverStrategy.SORTED_GROUP);
			
			context.setInput1(input, typeInfo.createSerializer());
			context.setComparator1(comparator);
			context.setCollector(new DiscardingOutputCollector<Tuple2<String, Integer>>());
			
			GroupReduceDriver<Tuple2<String, Integer>, Tuple2<String, Integer>> driver = new GroupReduceDriver<Tuple2<String, Integer>, Tuple2<String, Integer>>();
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
			TestTaskContext<GenericGroupReduce<Tuple2<String, Integer>, Tuple2<String, Integer>>, Tuple2<String, Integer>> context =
					new TestTaskContext<GenericGroupReduce<Tuple2<String, Integer>, Tuple2<String, Integer>>, Tuple2<String,Integer>>();
			
			List<Tuple2<String, Integer>> data = DriverTestData.createReduceImmutableData();
			TupleTypeInfo<Tuple2<String, Integer>> typeInfo = (TupleTypeInfo<Tuple2<String, Integer>>) TypeExtractor.getForObject(data.get(0));
			MutableObjectIterator<Tuple2<String, Integer>> input = new RegularToMutableObjectIterator<Tuple2<String, Integer>>(data.iterator(), typeInfo.createSerializer());
			TypeComparator<Tuple2<String, Integer>> comparator = typeInfo.createComparator(new int[]{0}, new boolean[] {true});
			
			GatheringCollector<Tuple2<String, Integer>> result = new GatheringCollector<Tuple2<String,Integer>>(typeInfo.createSerializer());
			
			context.setDriverStrategy(DriverStrategy.SORTED_GROUP);
			context.setInput1(input, typeInfo.createSerializer());
			context.setCollector(result);
			context.setComparator1(comparator);
			context.setUdf(new ConcatSumReducer());
			
			GroupReduceDriver<Tuple2<String, Integer>, Tuple2<String, Integer>> driver = new GroupReduceDriver<Tuple2<String, Integer>, Tuple2<String, Integer>>();
			driver.setup(context);
			driver.prepare();
			driver.run();
			
			Object[] res = result.getList().toArray();
			Object[] expected = DriverTestData.createReduceImmutableDataGroupedResult().toArray();
			
			DriverTestData.compareTupleArrays(expected, res);
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
			TestTaskContext<GenericGroupReduce<Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>>, Tuple2<StringValue, IntValue>> context =
					new TestTaskContext<GenericGroupReduce<Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>>, Tuple2<StringValue, IntValue>>();
			
			List<Tuple2<StringValue, IntValue>> data = DriverTestData.createReduceMutableData();
			TupleTypeInfo<Tuple2<StringValue, IntValue>> typeInfo = (TupleTypeInfo<Tuple2<StringValue, IntValue>>) TypeExtractor.getForObject(data.get(0));
			MutableObjectIterator<Tuple2<StringValue, IntValue>> input = new RegularToMutableObjectIterator<Tuple2<StringValue, IntValue>>(data.iterator(), typeInfo.createSerializer());
			TypeComparator<Tuple2<StringValue, IntValue>> comparator = typeInfo.createComparator(new int[]{0}, new boolean[] {true});
			
			GatheringCollector<Tuple2<StringValue, IntValue>> result = new GatheringCollector<Tuple2<StringValue, IntValue>>(typeInfo.createSerializer());
			
			context.setDriverStrategy(DriverStrategy.SORTED_GROUP);
			context.setInput1(input, typeInfo.createSerializer());
			context.setComparator1(comparator);
			context.setCollector(result);
			context.setUdf(new ConcatSumMutableReducer());
			
			GroupReduceDriver<Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>> driver = new GroupReduceDriver<Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>>();
			driver.setup(context);
			driver.prepare();
			driver.run();
			
			Object[] res = result.getList().toArray();
			Object[] expected = DriverTestData.createReduceMutableDataGroupedResult().toArray();
			
			DriverTestData.compareTupleArrays(expected, res);
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
	
	public static final class ConcatSumReducer extends GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

		@Override
		public void reduce(Iterator<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out) {
			Tuple2<String, Integer> current = values.next();
			
			while (values.hasNext()) {
				Tuple2<String, Integer> next = values.next();
				next.f0 = current.f0 + next.f0;
				next.f1 = current.f1 + next.f1;
				current = next;
			}
			
			out.collect(current);
		}
	}
	
	public static final class ConcatSumMutableReducer extends GroupReduceFunction<Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>> {

		@Override
		public void reduce(Iterator<Tuple2<StringValue, IntValue>> values, Collector<Tuple2<StringValue, IntValue>> out) {
			Tuple2<StringValue, IntValue> current = values.next();
			
			while (values.hasNext()) {
				Tuple2<StringValue, IntValue> next = values.next();
				next.f0.append(current.f0);
				next.f1.setValue(current.f1.getValue() + next.f1.getValue());
				current = next;
			}
			
			out.collect(current);
		}
	}
}
