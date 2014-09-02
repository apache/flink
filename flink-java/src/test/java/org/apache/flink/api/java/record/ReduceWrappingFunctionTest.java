/**
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

package org.apache.flink.api.java.record;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.functions.FlatCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.java.record.functions.FunctionAnnotation.ConstantFields;
import org.apache.flink.api.java.record.functions.ReduceFunction;
import org.apache.flink.api.java.record.operators.ReduceOperator;
import org.apache.flink.api.java.record.operators.ReduceOperator.Combinable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;
import org.junit.Test;

@SuppressWarnings("serial")
public class ReduceWrappingFunctionTest {

	@SuppressWarnings("unchecked")
	@Test
	public void testWrappedReduceObject() {
		try {
			AtomicInteger methodCounter = new AtomicInteger();
			
			ReduceOperator reduceOp = ReduceOperator.builder(new TestReduceFunction(methodCounter)).build();
			
			RichFunction reducer = (RichFunction) reduceOp.getUserCodeWrapper().getUserCodeObject();
			
			// test the method invocations
			reducer.close();
			reducer.open(new Configuration());
			assertEquals(2, methodCounter.get());
			
			// prepare the reduce / combine tests
			final List<Record> target = new ArrayList<Record>();
			Collector<Record> collector = new Collector<Record>() {
				@Override
				public void collect(Record record) {
					target.add(record);
				}
				@Override
				public void close() {}
			};
			
			List<Record> source = new ArrayList<Record>();
			source.add(new Record(new IntValue(42), new LongValue(11)));
			source.add(new Record(new IntValue(13), new LongValue(17)));
			
			// test reduce
			((GroupReduceFunction<Record, Record>) reducer).reduce(source, collector);
			assertEquals(2, target.size());
			assertEquals(new IntValue(42), target.get(0).getField(0, IntValue.class));
			assertEquals(new LongValue(11), target.get(0).getField(1, LongValue.class));
			assertEquals(new IntValue(13), target.get(1).getField(0, IntValue.class));
			assertEquals(new LongValue(17), target.get(1).getField(1, LongValue.class));
			target.clear();
			
			// test combine
			((FlatCombineFunction<Record>) reducer).combine(source, collector);
			assertEquals(2, target.size());
			assertEquals(new IntValue(42), target.get(0).getField(0, IntValue.class));
			assertEquals(new LongValue(11), target.get(0).getField(1, LongValue.class));
			assertEquals(new IntValue(13), target.get(1).getField(0, IntValue.class));
			assertEquals(new LongValue(17), target.get(1).getField(1, LongValue.class));
			target.clear();
			
			// test the serialization
			SerializationUtils.clone((java.io.Serializable) reducer);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testWrappedReduceClass() {
		try {
			ReduceOperator reduceOp = ReduceOperator.builder(TestReduceFunction.class).build();
			
			UserCodeWrapper<GroupReduceFunction<Record, Record>> udf = reduceOp.getUserCodeWrapper();
			UserCodeWrapper<GroupReduceFunction<Record, Record>> copy = SerializationUtils.clone(udf);
			GroupReduceFunction<Record, Record> reducer = copy.getUserCodeObject();
			
			// prepare the reduce / combine tests
			final List<Record> target = new ArrayList<Record>();
			Collector<Record> collector = new Collector<Record>() {
				@Override
				public void collect(Record record) {
					target.add(record);
				}
				@Override
				public void close() {}
			};
			
			List<Record> source = new ArrayList<Record>();
			source.add(new Record(new IntValue(42), new LongValue(11)));
			source.add(new Record(new IntValue(13), new LongValue(17)));
			
			// test reduce
			reducer.reduce(source, collector);
			assertEquals(2, target.size());
			assertEquals(new IntValue(42), target.get(0).getField(0, IntValue.class));
			assertEquals(new LongValue(11), target.get(0).getField(1, LongValue.class));
			assertEquals(new IntValue(13), target.get(1).getField(0, IntValue.class));
			assertEquals(new LongValue(17), target.get(1).getField(1, LongValue.class));
			target.clear();
			
			// test combine
			((FlatCombineFunction<Record>) reducer).combine(source, collector);
			assertEquals(2, target.size());
			assertEquals(new IntValue(42), target.get(0).getField(0, IntValue.class));
			assertEquals(new LongValue(11), target.get(0).getField(1, LongValue.class));
			assertEquals(new IntValue(13), target.get(1).getField(0, IntValue.class));
			assertEquals(new LongValue(17), target.get(1).getField(1, LongValue.class));
			target.clear();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testExtractSemantics() {
		try {
			{
				ReduceOperator reduceOp = ReduceOperator.builder(new TestReduceFunction()).build();
				
				SingleInputSemanticProperties props = reduceOp.getSemanticProperties();
				FieldSet fw2 = props.getForwardedField(2);
				FieldSet fw4 = props.getForwardedField(4);
				
				assertNotNull(fw2);
				assertNotNull(fw4);
				assertEquals(1, fw2.size());
				assertEquals(1, fw4.size());
				assertTrue(fw2.contains(2));
				assertTrue(fw4.contains(4));
			}
			{
				ReduceOperator reduceOp = ReduceOperator.builder(TestReduceFunction.class).build();
				
				SingleInputSemanticProperties props = reduceOp.getSemanticProperties();
				FieldSet fw2 = props.getForwardedField(2);
				FieldSet fw4 = props.getForwardedField(4);
				
				assertNotNull(fw2);
				assertNotNull(fw4);
				assertEquals(1, fw2.size());
				assertEquals(1, fw4.size());
				assertTrue(fw2.contains(2));
				assertTrue(fw4.contains(4));
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCombinable() {
		try {
			{
				ReduceOperator reduceOp = ReduceOperator.builder(new TestReduceFunction()).build();
				assertTrue(reduceOp.isCombinable());
			}
			{
				ReduceOperator reduceOp = ReduceOperator.builder(TestReduceFunction.class).build();
				assertTrue(reduceOp.isCombinable());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Combinable
	@ConstantFields({2, 4})
	public static class TestReduceFunction extends ReduceFunction {
		
		private final AtomicInteger methodCounter;
		
		private TestReduceFunction(AtomicInteger methodCounter) {
			this.methodCounter= methodCounter;
		}
		
		public TestReduceFunction() {
			methodCounter = new AtomicInteger();
		}
		
		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out) throws Exception {
			while (records.hasNext()) {
				out.collect(records.next());
			}
		}
		
		@Override
		public void close() throws Exception {
			methodCounter.incrementAndGet();
			super.close();
		}
		
		@Override
		public void open(Configuration parameters) throws Exception {
			methodCounter.incrementAndGet();
			super.open(parameters);
		}
	};
}
