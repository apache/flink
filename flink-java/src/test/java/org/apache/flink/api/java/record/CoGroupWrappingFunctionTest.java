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

package org.apache.flink.api.java.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.operators.DualInputSemanticProperties;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.java.record.functions.CoGroupFunction;
import org.apache.flink.api.java.record.functions.FunctionAnnotation.ConstantFieldsFirst;
import org.apache.flink.api.java.record.functions.FunctionAnnotation.ConstantFieldsSecond;
import org.apache.flink.api.java.record.operators.CoGroupOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;
import org.junit.Test;

@SuppressWarnings({ "serial", "deprecation" })
public class CoGroupWrappingFunctionTest {

	@SuppressWarnings("unchecked")
	@Test
	public void testWrappedCoGroupObject() {
		try {
			AtomicInteger methodCounter = new AtomicInteger();
			
			CoGroupOperator coGroupOp = CoGroupOperator.builder(new TestCoGroupFunction(methodCounter), LongValue.class, 1, 2).build();
			
			RichFunction cogrouper = (RichFunction) coGroupOp.getUserCodeWrapper().getUserCodeObject();
			
			// test the method invocations
			cogrouper.close();
			cogrouper.open(new Configuration());
			assertEquals(2, methodCounter.get());
			
			// prepare the coGroup
			final List<Record> target = new ArrayList<Record>();
			Collector<Record> collector = new Collector<Record>() {
				@Override
				public void collect(Record record) {
					target.add(record);
				}
				@Override
				public void close() {}
			};
			
			List<Record> source1 = new ArrayList<Record>();
			source1.add(new Record(new IntValue(42)));
			source1.add(new Record(new IntValue(13)));
			
			List<Record> source2 = new ArrayList<Record>();
			source2.add(new Record(new LongValue(11)));
			source2.add(new Record(new LongValue(17)));
			
			// test coGroup
			((org.apache.flink.api.common.functions.CoGroupFunction<Record, Record, Record>) cogrouper).coGroup(source1, source2, collector);
			assertEquals(4, target.size());
			assertEquals(new IntValue(42), target.get(0).getField(0, IntValue.class));
			assertEquals(new IntValue(13), target.get(1).getField(0, IntValue.class));
			assertEquals(new LongValue(11), target.get(2).getField(0, LongValue.class));
			assertEquals(new LongValue(17), target.get(3).getField(0, LongValue.class));
			target.clear();
			
			// test the serialization
			SerializationUtils.clone((java.io.Serializable) cogrouper);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testWrappedCoGroupClass() {
		try {
			CoGroupOperator coGroupOp = CoGroupOperator.builder(TestCoGroupFunction.class, LongValue.class, 1, 2).build();
			
			UserCodeWrapper<org.apache.flink.api.common.functions.CoGroupFunction<Record, Record, Record>> udf = coGroupOp.getUserCodeWrapper();
			UserCodeWrapper<org.apache.flink.api.common.functions.CoGroupFunction<Record, Record, Record>> copy = SerializationUtils.clone(udf);
			org.apache.flink.api.common.functions.CoGroupFunction<Record, Record, Record> cogrouper = copy.getUserCodeObject();
			
			// prepare the coGpuü
			final List<Record> target = new ArrayList<Record>();
			Collector<Record> collector = new Collector<Record>() {
				@Override
				public void collect(Record record) {
					target.add(record);
				}
				@Override
				public void close() {}
			};
			
			List<Record> source1 = new ArrayList<Record>();
			source1.add(new Record(new IntValue(42)));
			source1.add(new Record(new IntValue(13)));
			
			List<Record> source2 = new ArrayList<Record>();
			source2.add(new Record(new LongValue(11)));
			source2.add(new Record(new LongValue(17)));
			
			// test coGroup
			cogrouper.coGroup(source1, source2, collector);
			assertEquals(4, target.size());
			assertEquals(new IntValue(42), target.get(0).getField(0, IntValue.class));
			assertEquals(new IntValue(13), target.get(1).getField(0, IntValue.class));
			assertEquals(new LongValue(11), target.get(2).getField(0, LongValue.class));
			assertEquals(new LongValue(17), target.get(3).getField(0, LongValue.class));
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
				CoGroupOperator coGroupOp = CoGroupOperator.builder(new TestCoGroupFunction(), LongValue.class, 1, 2).build();
				
				DualInputSemanticProperties props = coGroupOp.getSemanticProperties();
				FieldSet fw2 = props.getForwardingTargetFields(0, 2);
				FieldSet fw4 = props.getForwardingTargetFields(1, 4);
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
	
	// --------------------------------------------------------------------------------------------
	
	@ConstantFieldsFirst(2)
	@ConstantFieldsSecond(4)
	public static class TestCoGroupFunction extends CoGroupFunction {
		
		private final AtomicInteger methodCounter;
		
		private TestCoGroupFunction(AtomicInteger methodCounter) {
			this.methodCounter= methodCounter;
		}
		
		public TestCoGroupFunction() {
			methodCounter = new AtomicInteger();
		}
		
		@Override
		public void coGroup(Iterator<Record> records1, Iterator<Record> records2, Collector<Record> out) throws Exception {
			while (records1.hasNext()) {
				out.collect(records1.next());
			}
			while (records2.hasNext()) {
				out.collect(records2.next());
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
