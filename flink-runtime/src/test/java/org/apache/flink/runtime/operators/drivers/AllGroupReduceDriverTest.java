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

package org.apache.flink.runtime.operators.drivers;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.operators.AllGroupReduceDriver;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.testutils.DiscardingOutputCollector;
import org.apache.flink.runtime.util.EmptyMutableObjectIterator;
import org.apache.flink.runtime.util.RegularToMutableObjectIterator;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("serial")
public class AllGroupReduceDriverTest {

	@Test
	public void testAllReduceDriverImmutableEmpty() {
		try {
			TestTaskContext<GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>, Tuple2<String, Integer>> context =
					new TestTaskContext<GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>, Tuple2<String,Integer>>();
			
			List<Tuple2<String, Integer>> data = DriverTestData.createReduceImmutableData();
			TypeInformation<Tuple2<String, Integer>> typeInfo = TypeExtractor.getForObject(data.get(0));
			MutableObjectIterator<Tuple2<String, Integer>> input = EmptyMutableObjectIterator.get();
			context.setDriverStrategy(DriverStrategy.ALL_GROUP_REDUCE);
			
			context.setInput1(input, typeInfo.createSerializer(new ExecutionConfig()));
			context.setCollector(new DiscardingOutputCollector<Tuple2<String, Integer>>());
			
			AllGroupReduceDriver<Tuple2<String, Integer>, Tuple2<String, Integer>> driver = new AllGroupReduceDriver<Tuple2<String, Integer>, Tuple2<String, Integer>>();
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
			TestTaskContext<GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>, Tuple2<String, Integer>> context =
					new TestTaskContext<GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>, Tuple2<String,Integer>>();
			
			List<Tuple2<String, Integer>> data = DriverTestData.createReduceImmutableData();
			TypeInformation<Tuple2<String, Integer>> typeInfo = TypeExtractor.getForObject(data.get(0));
			MutableObjectIterator<Tuple2<String, Integer>> input = new RegularToMutableObjectIterator<Tuple2<String, Integer>>(data.iterator(), typeInfo.createSerializer(new ExecutionConfig()));
			
			GatheringCollector<Tuple2<String, Integer>> result = new GatheringCollector<Tuple2<String,Integer>>(typeInfo.createSerializer(new ExecutionConfig()));
			
			context.setDriverStrategy(DriverStrategy.ALL_GROUP_REDUCE);
			context.setInput1(input, typeInfo.createSerializer(new ExecutionConfig()));
			context.setCollector(result);
			context.setUdf(new ConcatSumReducer());
			
			AllGroupReduceDriver<Tuple2<String, Integer>, Tuple2<String, Integer>> driver = new AllGroupReduceDriver<Tuple2<String, Integer>, Tuple2<String, Integer>>();
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
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testAllReduceDriverMutable() {
		try {
			TestTaskContext<GroupReduceFunction<Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>>, Tuple2<StringValue, IntValue>> context =
					new TestTaskContext<GroupReduceFunction<Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>>, Tuple2<StringValue, IntValue>>();
			
			List<Tuple2<StringValue, IntValue>> data = DriverTestData.createReduceMutableData();
			TypeInformation<Tuple2<StringValue, IntValue>> typeInfo = TypeExtractor.getForObject(data.get(0));
			MutableObjectIterator<Tuple2<StringValue, IntValue>> input = new RegularToMutableObjectIterator<Tuple2<StringValue, IntValue>>(data.iterator(), typeInfo.createSerializer(new ExecutionConfig()));
			
			GatheringCollector<Tuple2<StringValue, IntValue>> result = new GatheringCollector<Tuple2<StringValue, IntValue>>(typeInfo.createSerializer(new ExecutionConfig()));
			
			context.setDriverStrategy(DriverStrategy.ALL_GROUP_REDUCE);
			context.setInput1(input, typeInfo.createSerializer(new ExecutionConfig()));
			context.setCollector(result);
			context.setUdf(new ConcatSumMutableReducer());
			
			AllGroupReduceDriver<Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>> driver = new AllGroupReduceDriver<Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>>();
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
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	// --------------------------------------------------------------------------------------------
	//  Test UDFs
	// --------------------------------------------------------------------------------------------
	
	public static final class ConcatSumReducer extends RichGroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

		@Override
		public void reduce(Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out) {
			Tuple2<String, Integer> current = new Tuple2<String, Integer>("", 0);
			
			for (Tuple2<String, Integer> next : values) {
				next.f0 = current.f0 + next.f0;
				next.f1 = current.f1 + next.f1;
				current = next;
			}
			
			out.collect(current);
		}
	}
	
	public static final class ConcatSumMutableReducer extends RichGroupReduceFunction<Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>> {

		@Override
		public void reduce(Iterable<Tuple2<StringValue, IntValue>> values, Collector<Tuple2<StringValue, IntValue>> out) {
			Tuple2<StringValue, IntValue> current = new Tuple2<StringValue, IntValue>(new StringValue(""), new IntValue(0));
			
			for (Tuple2<StringValue, IntValue> next : values) {
				next.f0.append(current.f0);
				next.f1.setValue(current.f1.getValue() + next.f1.getValue());
				current = next;
			}
			
			out.collect(current);
		}
	}
}
