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

package org.apache.flink.api.java.operators.translation;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.CoGroupOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Tests for translation of co-group sort.
 */
@SuppressWarnings({"serial", "unchecked"})
public class CoGroupSortTranslationTest implements java.io.Serializable {

	@Test
	public void testGroupSortTuples() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

			DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<Long, Long>(0L, 0L));
			DataSet<Tuple3<Long, Long, Long>> input2 = env.fromElements(new Tuple3<Long, Long, Long>(0L, 0L, 0L));

			input1.coGroup(input2)
				.where(1).equalTo(2)
				.sortFirstGroup(0, Order.DESCENDING)
				.sortSecondGroup(1, Order.ASCENDING).sortSecondGroup(0, Order.DESCENDING)

				.with(new CoGroupFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Long>, Long>() {
					@Override
					public void coGroup(Iterable<Tuple2<Long, Long>> first, Iterable<Tuple3<Long, Long, Long>> second,
							Collector<Long> out) {}
				})

				.output(new DiscardingOutputFormat<Long>());

			Plan p = env.createProgramPlan();

			GenericDataSinkBase<?> sink = p.getDataSinks().iterator().next();
			CoGroupOperatorBase<?, ?, ?, ?> coGroup = (CoGroupOperatorBase<?, ?, ?, ?>) sink.getInput();

			assertNotNull(coGroup.getGroupOrderForInputOne());
			assertNotNull(coGroup.getGroupOrderForInputTwo());

			assertEquals(1, coGroup.getGroupOrderForInputOne().getNumberOfFields());
			assertEquals(0, coGroup.getGroupOrderForInputOne().getFieldNumber(0).intValue());
			assertEquals(Order.DESCENDING, coGroup.getGroupOrderForInputOne().getOrder(0));

			assertEquals(2, coGroup.getGroupOrderForInputTwo().getNumberOfFields());
			assertEquals(1, coGroup.getGroupOrderForInputTwo().getFieldNumber(0).intValue());
			assertEquals(0, coGroup.getGroupOrderForInputTwo().getFieldNumber(1).intValue());
			assertEquals(Order.ASCENDING, coGroup.getGroupOrderForInputTwo().getOrder(0));
			assertEquals(Order.DESCENDING, coGroup.getGroupOrderForInputTwo().getOrder(1));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSortTuplesAndPojos() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

			DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<Long, Long>(0L, 0L));
			DataSet<TestPoJo> input2 = env.fromElements(new TestPoJo());

			input1.coGroup(input2)
				.where(1).equalTo("b")
				.sortFirstGroup(0, Order.DESCENDING)
				.sortSecondGroup("c", Order.ASCENDING).sortSecondGroup("a", Order.DESCENDING)

				.with(new CoGroupFunction<Tuple2<Long, Long>, TestPoJo, Long>() {
					@Override
					public void coGroup(Iterable<Tuple2<Long, Long>> first, Iterable<TestPoJo> second, Collector<Long> out) {}
				})

				.output(new DiscardingOutputFormat<Long>());

			Plan p = env.createProgramPlan();

			GenericDataSinkBase<?> sink = p.getDataSinks().iterator().next();
			CoGroupOperatorBase<?, ?, ?, ?> coGroup = (CoGroupOperatorBase<?, ?, ?, ?>) sink.getInput();

			assertNotNull(coGroup.getGroupOrderForInputOne());
			assertNotNull(coGroup.getGroupOrderForInputTwo());

			assertEquals(1, coGroup.getGroupOrderForInputOne().getNumberOfFields());
			assertEquals(0, coGroup.getGroupOrderForInputOne().getFieldNumber(0).intValue());
			assertEquals(Order.DESCENDING, coGroup.getGroupOrderForInputOne().getOrder(0));

			assertEquals(2, coGroup.getGroupOrderForInputTwo().getNumberOfFields());
			assertEquals(2, coGroup.getGroupOrderForInputTwo().getFieldNumber(0).intValue());
			assertEquals(0, coGroup.getGroupOrderForInputTwo().getFieldNumber(1).intValue());
			assertEquals(Order.ASCENDING, coGroup.getGroupOrderForInputTwo().getOrder(0));
			assertEquals(Order.DESCENDING, coGroup.getGroupOrderForInputTwo().getOrder(1));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Sample test pojo.
	 */
	public static class TestPoJo {
		public long a;
		public long b;
		public long c;
	}
}
