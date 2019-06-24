/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.api;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.Operator;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.functions.BroadcastStateBootstrapFunction;
import org.apache.flink.state.api.functions.StateBootstrapFunction;
import org.apache.flink.state.api.output.TaggedOperatorSubtaskState;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.state.api.runtime.metadata.SavepointMetadata;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

/**
 * Tests for bootstrap transformations.
 */
public class BootstrapTransformationTest extends AbstractTestBase {

	@Test
	public void testBroadcastStateTransformationParallelism() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(10);

		DataSet<Integer> input = env.fromElements(0);

		BootstrapTransformation<Integer> transformation = OperatorTransformation
			.bootstrapWith(input)
			.transform(new ExampleBroadcastStateBootstrapFunction());

		int maxParallelism = transformation.getMaxParallelism(new SavepointMetadata(4, Collections.emptyList()));
		DataSet<TaggedOperatorSubtaskState> result = transformation.writeOperatorSubtaskStates(
			OperatorIDGenerator.fromUid("uid"),
			new MemoryStateBackend(),
			new Path(),
			maxParallelism
		);

		Assert.assertEquals("Broadcast transformations should always be run at parallelism 1",
			1,
			getParallelism(result));
	}

	@Test
	public void testDefaultParallelismRespectedWhenLessThanMaxParallelism() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);

		DataSource<Integer> input = env.fromElements(0);

		BootstrapTransformation<Integer> transformation = OperatorTransformation
			.bootstrapWith(input)
			.transform(new ExampleStateBootstrapFunction());

		int maxParallelism = transformation.getMaxParallelism(new SavepointMetadata(10, Collections.emptyList()));
		DataSet<TaggedOperatorSubtaskState> result = transformation.writeOperatorSubtaskStates(
			OperatorIDGenerator.fromUid("uid"),
			new MemoryStateBackend(),
			new Path(),
			maxParallelism
		);

		Assert.assertEquals(
			"The parallelism of a data set should not change when less than the max parallelism of the savepoint",
			ExecutionConfig.PARALLELISM_DEFAULT,
			getParallelism(result));
	}

	@Test
	public void testMaxParallelismRespected() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(10);

		DataSource<Integer> input = env.fromElements(0);

		BootstrapTransformation<Integer> transformation = OperatorTransformation
			.bootstrapWith(input)
			.transform(new ExampleStateBootstrapFunction());

		int maxParallelism = transformation.getMaxParallelism(new SavepointMetadata(4, Collections.emptyList()));
		DataSet<TaggedOperatorSubtaskState> result = transformation.writeOperatorSubtaskStates(
			OperatorIDGenerator.fromUid("uid"),
			new MemoryStateBackend(),
			new Path(),
			maxParallelism
		);

		Assert.assertEquals(
			"The parallelism of a data set should be constrained my the savepoint max parallelism",
			4,
			getParallelism(result));
	}

	@Test
	public void testOperatorSpecificMaxParallelismRespected() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);

		DataSource<Integer> input = env.fromElements(0);

		BootstrapTransformation<Integer> transformation = OperatorTransformation
			.bootstrapWith(input)
			.setMaxParallelism(1)
			.transform(new ExampleStateBootstrapFunction());

		int maxParallelism = transformation.getMaxParallelism(new SavepointMetadata(4, Collections.emptyList()));
		DataSet<TaggedOperatorSubtaskState> result = transformation.writeOperatorSubtaskStates(
			OperatorIDGenerator.fromUid("uid"),
			new MemoryStateBackend(),
			new Path(),
			maxParallelism
		);

		Assert.assertEquals("The parallelism of a data set should be constrained my the savepoint max parallelism", 1, getParallelism(result));
	}

	private static <T> int getParallelism(DataSet<T> dataSet) {
		//All concrete implementations of DataSet are operators so this should always be safe.
		return ((Operator) dataSet).getParallelism();
	}

	private static class ExampleBroadcastStateBootstrapFunction extends BroadcastStateBootstrapFunction<Integer> {

		@Override
		public void processElement(Integer value, Context ctx) throws Exception {
		}
	}

	private static class ExampleStateBootstrapFunction extends StateBootstrapFunction<Integer> {

		@Override
		public void processElement(Integer value, Context ctx) throws Exception {
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
		}
	}
}
