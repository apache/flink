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

package org.apache.flink.api.common.operators.base;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.api.common.operators.BinaryOperatorInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.builder.Tuple2Builder;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

@SuppressWarnings("serial")
public class CoGroupOperatorCollectionTest implements Serializable {

	@Test
	public void testExecuteOnCollection() {
		try {
			List<Tuple2<String, Integer>> input1 = Arrays.asList(
					new Tuple2Builder<String, Integer>()
							.add("foo", 1)
							.add("foobar", 1)
							.add("foo", 1)
							.add("bar", 1)
							.add("foo", 1)
							.add("foo", 1)
							.build()
			);

			List<Tuple2<String, Integer>> input2 = Arrays.asList(
					new Tuple2Builder<String, Integer>()
							.add("foo", 1)
							.add("foo", 1)
							.add("bar", 1)
							.add("foo", 1)
							.add("barfoo", 1)
							.add("foo", 1)
							.build()
			);

			ExecutionConfig executionConfig = new ExecutionConfig();
			final HashMap<String, Accumulator<?, ?>> accumulators = new HashMap<String, Accumulator<?, ?>>();
			final HashMap<String, Future<Path>> cpTasks = new HashMap<>();
			final TaskInfo taskInfo = new TaskInfo("Test UDF", 0, 4, 0);
			final RuntimeContext ctx = new RuntimeUDFContext(taskInfo, null, executionConfig, cpTasks, accumulators);

			{
				SumCoGroup udf1 = new SumCoGroup();
				SumCoGroup udf2 = new SumCoGroup();

				executionConfig.disableObjectReuse();
				List<Tuple2<String, Integer>> resultSafe = getCoGroupOperator(udf1)
						.executeOnCollections(input1, input2, ctx, executionConfig);
				executionConfig.enableObjectReuse();
				List<Tuple2<String, Integer>> resultRegular = getCoGroupOperator(udf2)
						.executeOnCollections(input1, input2, ctx, executionConfig);

				Assert.assertTrue(udf1.isClosed);
				Assert.assertTrue(udf2.isClosed);
				
				Set<Tuple2<String, Integer>> expected = new HashSet<Tuple2<String, Integer>>(
						Arrays.asList(new Tuple2Builder<String, Integer>()
										.add("foo", 8)
										.add("bar", 2)
										.add("foobar", 1)
										.add("barfoo", 1)
										.build()
						)
				);

				Assert.assertEquals(expected, new HashSet<Tuple2<String, Integer>>(resultSafe));
				Assert.assertEquals(expected, new HashSet<Tuple2<String, Integer>>(resultRegular));
			}

			{
				executionConfig.disableObjectReuse();
				List<Tuple2<String, Integer>> resultSafe = getCoGroupOperator(new SumCoGroup())
						.executeOnCollections(Collections.<Tuple2<String, Integer>>emptyList(),
								Collections.<Tuple2<String, Integer>>emptyList(), ctx, executionConfig);

				executionConfig.enableObjectReuse();
				List<Tuple2<String, Integer>> resultRegular = getCoGroupOperator(new SumCoGroup())
						.executeOnCollections(Collections.<Tuple2<String, Integer>>emptyList(),
								Collections.<Tuple2<String, Integer>>emptyList(), ctx, executionConfig);

				Assert.assertEquals(0, resultSafe.size());
				Assert.assertEquals(0, resultRegular.size());
			}
		} catch (Throwable t) {
			t.printStackTrace();
			Assert.fail(t.getMessage());
		}
	}

	private class SumCoGroup extends RichCoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> {

		private boolean isOpened = false;
		private boolean isClosed = false;

		@Override
		public void open(Configuration parameters) throws Exception {
			isOpened = true;

			RuntimeContext ctx = getRuntimeContext();
			Assert.assertEquals("Test UDF", ctx.getTaskName());
			Assert.assertEquals(4, ctx.getNumberOfParallelSubtasks());
			Assert.assertEquals(0, ctx.getIndexOfThisSubtask());
		}

		@Override
		public void coGroup(
				Iterable<Tuple2<String, Integer>> first,
				Iterable<Tuple2<String, Integer>> second,
				Collector<Tuple2<String, Integer>> out) throws Exception {

			Assert.assertTrue(isOpened);
			Assert.assertFalse(isClosed);

			String f0 = null;
			int sumF1 = 0;

			for (Tuple2<String, Integer> input : first) {
				f0 = (f0 == null) ? input.f0 : f0;
				sumF1 += input.f1;
			}

			for (Tuple2<String, Integer> input : second) {
				f0 = (f0 == null) ? input.f0 : f0;
				sumF1 += input.f1;
			}

			out.collect(Tuple2.of(f0, sumF1));
		}

		@Override
		public void close() throws Exception {
			isClosed = true;
		}
	}

	private CoGroupOperatorBase<Tuple2<String, Integer>, Tuple2<String, Integer>,
			Tuple2<String, Integer>, CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>,
			Tuple2<String, Integer>>> getCoGroupOperator(
			RichCoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> udf) {

		return new CoGroupOperatorBase<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>,
				CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>>(
				udf,
				new BinaryOperatorInformation<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>(
						TypeInfoParser.<Tuple2<String, Integer>>parse("Tuple2<String, Integer>"),
						TypeInfoParser.<Tuple2<String, Integer>>parse("Tuple2<String, Integer>"),
						TypeInfoParser.<Tuple2<String, Integer>>parse("Tuple2<String, Integer>")
				),
				new int[]{0},
				new int[]{0},
				"coGroup on Collections"
		);
	}
}
