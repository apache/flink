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
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.api.common.operators.BinaryOperatorInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for {@link InnerJoinOperatorBase}.
 */
@SuppressWarnings({ "unchecked", "serial" })
public class InnerJoinOperatorBaseTest implements Serializable {

	@Test
	public void testTupleBaseJoiner(){
		final FlatJoinFunction<Tuple3<String, Double, Integer>, Tuple2<Integer, String>, Tuple2<Double, String>> joiner =
					new FlatJoinFunction<Tuple3<String, Double, Integer>, Tuple2<Integer, String>, Tuple2<Double, String>>() {
			@Override
			public void join(Tuple3<String, Double, Integer> first, Tuple2<Integer, String> second, Collector<Tuple2<Double, String>> out) {

				assertEquals(first.f0, second.f1);
				assertEquals(first.f2, second.f0);

				out.collect(new Tuple2<>(first.f1, second.f0.toString()));
			}
		};

		final TupleTypeInfo<Tuple3<String, Double, Integer>> leftTypeInfo = TupleTypeInfo.getBasicTupleTypeInfo
				(String.class, Double.class, Integer.class);
		final TupleTypeInfo<Tuple2<Integer, String>> rightTypeInfo = TupleTypeInfo.getBasicTupleTypeInfo(Integer.class,
				String.class);
		final TupleTypeInfo<Tuple2<Double, String>> outTypeInfo = TupleTypeInfo.getBasicTupleTypeInfo(Double.class,
				String.class);

		final int[] leftKeys = new int[]{0, 2};
		final int[] rightKeys = new int[]{1, 0};

		final String taskName = "Collection based tuple joiner";

		final BinaryOperatorInformation<Tuple3<String, Double, Integer>, Tuple2<Integer, String>, Tuple2<Double,
				String>> binaryOpInfo = new BinaryOperatorInformation<Tuple3<String, Double, Integer>, Tuple2<Integer,
				String>, Tuple2<Double, String>>(leftTypeInfo, rightTypeInfo, outTypeInfo);

		final InnerJoinOperatorBase<Tuple3<String, Double, Integer>, Tuple2<Integer,
						String>, Tuple2<Double, String>, FlatJoinFunction<Tuple3<String, Double, Integer>, Tuple2<Integer,
						String>, Tuple2<Double, String>>> base = new InnerJoinOperatorBase<Tuple3<String, Double, Integer>,
										Tuple2<Integer, String>, Tuple2<Double, String>, FlatJoinFunction<Tuple3<String, Double, Integer>,
										Tuple2<Integer, String>, Tuple2<Double, String>>>(joiner, binaryOpInfo, leftKeys, rightKeys, taskName);

		final List<Tuple3<String, Double, Integer> > inputData1 = new ArrayList<Tuple3<String, Double,
				Integer>>(Arrays.asList(
				new Tuple3<>("foo", 42.0, 1),
				new Tuple3<>("bar", 1.0, 2),
				new Tuple3<>("bar", 2.0, 3),
				new Tuple3<>("foobar", 3.0, 4),
				new Tuple3<>("bar", 3.0, 3)
		));

		final List<Tuple2<Integer, String>> inputData2 = new ArrayList<Tuple2<Integer, String>>(Arrays.asList(
				new Tuple2<>(3, "bar"),
				new Tuple2<>(4, "foobar"),
				new Tuple2<>(2, "foo")
		));
		final Set<Tuple2<Double, String>> expected = new HashSet<Tuple2<Double, String>>(Arrays.asList(
				new Tuple2<>(2.0, "3"),
				new Tuple2<>(3.0, "3"),
				new Tuple2<>(3.0, "4")
		));

		try {
			final TaskInfo taskInfo = new TaskInfo("op", 1, 0, 1, 0);
			ExecutionConfig executionConfig = new ExecutionConfig();

			executionConfig.disableObjectReuse();
			List<Tuple2<Double, String>> resultSafe = base.executeOnCollections(inputData1, inputData2,
					new RuntimeUDFContext(taskInfo, null, executionConfig,
							new HashMap<String, Future<Path>>(),
							new HashMap<String, Accumulator<?, ?>>(),
							new UnregisteredMetricsGroup()),
					executionConfig);

			executionConfig.enableObjectReuse();
			List<Tuple2<Double, String>> resultRegular = base.executeOnCollections(inputData1, inputData2,
					new RuntimeUDFContext(taskInfo, null, executionConfig,
							new HashMap<String, Future<Path>>(),
							new HashMap<String, Accumulator<?, ?>>(),
							new UnregisteredMetricsGroup()),
					executionConfig);

			assertEquals(expected, new HashSet<>(resultSafe));
			assertEquals(expected, new HashSet<>(resultRegular));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
