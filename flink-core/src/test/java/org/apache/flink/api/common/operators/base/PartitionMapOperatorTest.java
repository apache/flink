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

import static org.junit.Assert.*;
import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;

import org.junit.Test;

@SuppressWarnings("serial")
public class PartitionMapOperatorTest implements java.io.Serializable {

	@Test
	public void testMapPartitionWithRuntimeContext() {
		try {
			final String taskName = "Test Task";
			final AtomicBoolean opened = new AtomicBoolean();
			final AtomicBoolean closed = new AtomicBoolean();
			
			final MapPartitionFunction<String, Integer> parser = new RichMapPartitionFunction<String, Integer>() {
				
				@Override
				public void open(Configuration parameters) throws Exception {
					opened.set(true);
					RuntimeContext ctx = getRuntimeContext();
					assertEquals(0, ctx.getIndexOfThisSubtask());
					assertEquals(1, ctx.getNumberOfParallelSubtasks());
					assertEquals(taskName, ctx.getTaskName());
				}
				
				@Override
				public void mapPartition(Iterable<String> values, Collector<Integer> out) {
					for (String s : values) {
						out.collect(Integer.parseInt(s));
					}
				}
				
				@Override
				public void close() throws Exception {
					closed.set(true);
				}
			};
			
			MapPartitionOperatorBase<String, Integer, MapPartitionFunction<String, Integer>> op = 
					new MapPartitionOperatorBase<String, Integer, MapPartitionFunction<String,Integer>>(
					parser, new UnaryOperatorInformation<String, Integer>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO), taskName);
			
			List<String> input = new ArrayList<String>(asList("1", "2", "3", "4", "5", "6"));

			final TaskInfo taskInfo = new TaskInfo(taskName, 1, 0, 1, 0);

			ExecutionConfig executionConfig = new ExecutionConfig();
			executionConfig.disableObjectReuse();
			
			List<Integer> resultMutableSafe = op.executeOnCollections(input,
					new RuntimeUDFContext(taskInfo, null, executionConfig,
							new HashMap<String, Future<Path>>(),
							new HashMap<String, Accumulator<?, ?>>(),
							new UnregisteredMetricsGroup()),
					executionConfig);
			
			executionConfig.enableObjectReuse();
			List<Integer> resultRegular = op.executeOnCollections(input,
					new RuntimeUDFContext(taskInfo, null, executionConfig,
							new HashMap<String, Future<Path>>(),
							new HashMap<String, Accumulator<?, ?>>(),
							new UnregisteredMetricsGroup()),
					executionConfig);
			
			assertEquals(asList(1, 2, 3, 4, 5, 6), resultMutableSafe);
			assertEquals(asList(1, 2, 3, 4, 5, 6), resultRegular);
			
			assertTrue(opened.get());
			assertTrue(closed.get());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
