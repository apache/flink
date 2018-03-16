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

package org.apache.flink.test.operators;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import org.junit.Assert;

/**
 * Integration tests for {@link CoGroupFunction}.
 */
@SuppressWarnings({"serial", "unchecked"})
public class CoGroupGroupSortITCase extends JavaProgramTestBase {

	@Override
	protected void testProgram() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Long, Long>> input1 = env.fromElements(
				new Tuple2<Long, Long>(0L, 5L),
				new Tuple2<Long, Long>(0L, 4L),
				new Tuple2<Long, Long>(0L, 3L),
				new Tuple2<Long, Long>(0L, 2L),
				new Tuple2<Long, Long>(0L, 1L),
				new Tuple2<Long, Long>(1L, 10L),
				new Tuple2<Long, Long>(1L, 8L),
				new Tuple2<Long, Long>(1L, 9L),
				new Tuple2<Long, Long>(1L, 7L));

		DataSet<TestPojo> input2 = env.fromElements(
				new TestPojo(0L, 10L, 3L),
				new TestPojo(0L, 8L, 3L),
				new TestPojo(0L, 10L, 1L),
				new TestPojo(0L, 9L, 0L),
				new TestPojo(0L, 8L, 2L),
				new TestPojo(0L, 8L, 4L),
				new TestPojo(1L, 10L, 3L),
				new TestPojo(1L, 8L, 3L),
				new TestPojo(1L, 10L, 1L),
				new TestPojo(1L, 9L, 0L),
				new TestPojo(1L, 8L, 2L),
				new TestPojo(1L, 8L, 4L));

		input1.coGroup(input2)
		.where(1).equalTo("b")
		.sortFirstGroup(0, Order.DESCENDING)
		.sortSecondGroup("c", Order.ASCENDING).sortSecondGroup("a", Order.DESCENDING)

		.with(new ValidatingCoGroup())
		.output(new DiscardingOutputFormat<NullValue>());

		env.execute();
	}

	private static class ValidatingCoGroup implements CoGroupFunction<Tuple2<Long, Long>, TestPojo, NullValue> {

		@Override
		public void coGroup(Iterable<Tuple2<Long, Long>> first, Iterable<TestPojo> second, Collector<NullValue> out) throws Exception {
			// validate the tuple input, field 1, descending
			{
				long lastValue = Long.MAX_VALUE;

				for (Tuple2<Long, Long> t : first) {
					long current = t.f1;
					Assert.assertTrue(current <= lastValue);
					lastValue = current;
				}
			}

			// validate the pojo input
			{
				TestPojo lastValue = new TestPojo(Long.MAX_VALUE, 0, Long.MIN_VALUE);

				for (TestPojo current : second) {
					Assert.assertTrue(current.c >= lastValue.c);
					Assert.assertTrue(current.c != lastValue.c || current.a <= lastValue.a);

					lastValue = current;
				}
			}

		}
	}

	/**
	 * Test POJO.
	 */
	public static class TestPojo implements Cloneable {
		public long a;
		public long b;
		public long c;

		public TestPojo() {}

		public TestPojo(long a, long b, long c) {
			this.a = a;
			this.b = b;
			this.c = c;
		}
	}
}
