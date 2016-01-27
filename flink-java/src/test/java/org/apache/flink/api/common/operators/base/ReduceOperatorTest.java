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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

@SuppressWarnings({"serial", "unchecked"})
public class ReduceOperatorTest implements java.io.Serializable {

	@Test
	public void testReduceCollection() {
		try {
			final ReduceFunction<Tuple2<String, Integer>> reducer = new
					ReduceFunction<Tuple2<String, Integer>>() {

				@Override
				public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1,
				                                      Tuple2<String, Integer> value2) throws
						Exception {
					return new Tuple2<String, Integer>(value1.f0, value1.f1 + value2.f1);
				}
			};

			ReduceOperatorBase<Tuple2<String, Integer>, ReduceFunction<Tuple2<String,
					Integer>>> op = new ReduceOperatorBase<Tuple2<String, Integer>,
					ReduceFunction<Tuple2<String, Integer>>>(reducer,
					new UnaryOperatorInformation<Tuple2<String, Integer>, Tuple2<String,
							Integer>>(TypeInfoParser.<Tuple2<String,
							Integer>>parse("Tuple2<String, Integer>"),
							TypeInfoParser.<Tuple2<String, Integer>>parse("Tuple2<String, " +
									"Integer>")), new int[]{0}, "TestReducer");

			List<Tuple2<String, Integer>> input = new ArrayList<Tuple2<String,
					Integer>>(asList(new Tuple2<String, Integer>("foo", 1), new Tuple2<String,
					Integer>("foo", 3), new Tuple2<String, Integer>("bar", 2), new Tuple2<String,
					Integer>("bar", 4)));

			ExecutionConfig executionConfig = new ExecutionConfig();
			executionConfig.disableObjectReuse();
			List<Tuple2<String, Integer>> resultMutableSafe = op.executeOnCollections(input, null, executionConfig);
			executionConfig.enableObjectReuse();
			List<Tuple2<String, Integer>> resultRegular = op.executeOnCollections(input, null, executionConfig);
			
			Set<Tuple2<String, Integer>> resultSetMutableSafe = new HashSet<Tuple2<String, Integer>>(resultMutableSafe);
			Set<Tuple2<String, Integer>> resultSetRegular = new HashSet<Tuple2<String, Integer>>(resultRegular);

			Set<Tuple2<String, Integer>> expectedResult = new HashSet<Tuple2<String,
					Integer>>(asList(new Tuple2<String, Integer>("foo", 4), new Tuple2<String,
					Integer>("bar", 6)));

			assertEquals(expectedResult, resultSetMutableSafe);
			assertEquals(expectedResult, resultSetRegular);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testReduceCollectionWithRuntimeContext() {
		try {
			final String taskName = "Test Task";
			final AtomicBoolean opened = new AtomicBoolean();
			final AtomicBoolean closed = new AtomicBoolean();

			final ReduceFunction<Tuple2<String, Integer>> reducer = new
					RichReduceFunction<Tuple2<String, Integer>>() {
				@Override
				public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1,
				                                      Tuple2<String, Integer> value2) throws
						Exception {
					return new Tuple2<String, Integer>(value1.f0, value1.f1 + value2.f1);
				}

				@Override
				public void open(Configuration parameters) throws Exception {
					opened.set(true);
					RuntimeContext ctx = getRuntimeContext();
					assertEquals(0, ctx.getIndexOfThisSubtask());
					assertEquals(1, ctx.getNumberOfParallelSubtasks());
					assertEquals(taskName, ctx.getTaskName());
				}

				@Override
				public void close() throws Exception {
					closed.set(true);
				}
			};

			ReduceOperatorBase<Tuple2<String, Integer>, ReduceFunction<Tuple2<String,
					Integer>>> op = new ReduceOperatorBase<Tuple2<String, Integer>,
					ReduceFunction<Tuple2<String, Integer>>>(reducer,
					new UnaryOperatorInformation<Tuple2<String, Integer>, Tuple2<String,
							Integer>>(TypeInfoParser.<Tuple2<String,
							Integer>>parse("Tuple2<String, Integer>"),
							TypeInfoParser.<Tuple2<String, Integer>>parse("Tuple2<String, " +
									"Integer>")), new int[]{0}, "TestReducer");

			List<Tuple2<String, Integer>> input = new ArrayList<Tuple2<String,
					Integer>>(asList(new Tuple2<String, Integer>("foo", 1), new Tuple2<String,
					Integer>("foo", 3), new Tuple2<String, Integer>("bar", 2), new Tuple2<String,
					Integer>("bar", 4)));

			final TaskInfo taskInfo = new TaskInfo(taskName, 0, 1, 0);

			ExecutionConfig executionConfig = new ExecutionConfig();
			executionConfig.disableObjectReuse();
			List<Tuple2<String, Integer>> resultMutableSafe = op.executeOnCollections(input, new RuntimeUDFContext(taskInfo, null, executionConfig, new HashMap<String, Future<Path>>(), new HashMap<String, Accumulator<?, ?>>()), executionConfig);
			executionConfig.enableObjectReuse();
			List<Tuple2<String, Integer>> resultRegular = op.executeOnCollections(input, new RuntimeUDFContext(taskInfo, null, executionConfig, new HashMap<String, Future<Path>>(), new HashMap<String, Accumulator<?, ?>>()), executionConfig);

			Set<Tuple2<String, Integer>> resultSetMutableSafe = new HashSet<Tuple2<String, Integer>>(resultMutableSafe);
			Set<Tuple2<String, Integer>> resultSetRegular = new HashSet<Tuple2<String, Integer>>(resultRegular);

			Set<Tuple2<String, Integer>> expectedResult = new HashSet<Tuple2<String,
					Integer>>(asList(new Tuple2<String, Integer>("foo", 4), new Tuple2<String,
					Integer>("bar", 6)));

			assertEquals(expectedResult, resultSetMutableSafe);
			assertEquals(expectedResult, resultSetRegular);

			assertTrue(opened.get());
			assertTrue(closed.get());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
