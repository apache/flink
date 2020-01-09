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

package org.apache.flink.state.api.output;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;
import org.apache.flink.state.api.output.operators.KeyedStateBootstrapOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test writing keyed bootstrap state.
 */
public class KeyedStateBootstrapOperatorTest {

	private static final ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<Long>("state", Types.LONG);

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void testNonTimerStatesRestorableByNonProcessesOperator() throws Exception {
		Path path = new Path(folder.newFolder().toURI());

		OperatorSubtaskState state;
		KeyedStateBootstrapOperator<Long, Long> bootstrapOperator = new KeyedStateBootstrapOperator<>(0L, path, new SimpleBootstrapFunction());
		try (KeyedOneInputStreamOperatorTestHarness<Long, Long, TaggedOperatorSubtaskState> harness = getHarness(bootstrapOperator)) {
			harness.open();

			harness.processElement(1L, 0L);
			harness.processElement(2L, 0L);
			harness.processElement(3L, 0L);
			bootstrapOperator.endInput();

			state = harness.extractOutputValues().get(0).state;
		}

		StreamMap<Long, Long> mapOperator = new StreamMap<>(new StreamingFunction());
		try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Long> harness = getHarness(mapOperator)) {

			harness.initializeState(state);
			harness.open();

			harness.processElement(1L, 0L);
			harness.processElement(2L, 0L);
			harness.processElement(3L, 0L);

			Assert.assertThat(harness.extractOutputValues(), Matchers.containsInAnyOrder(1L, 2L, 3L));

			harness.snapshot(0L, 0L);
		}
	}

	private <T> KeyedOneInputStreamOperatorTestHarness<Long, Long, T> getHarness(OneInputStreamOperator<Long, T> bootstrapOperator) throws Exception {
		KeyedOneInputStreamOperatorTestHarness<Long, Long, T> harness = new KeyedOneInputStreamOperatorTestHarness<>(
			bootstrapOperator, id -> id, Types.LONG, 128, 1, 0);

		harness.setStateBackend(new RocksDBStateBackend(folder.newFolder().toURI()));
		return harness;
	}

	private static class SimpleBootstrapFunction extends KeyedStateBootstrapFunction<Long, Long> {

		private ValueState<Long> state;

		@Override
		public void open(Configuration parameters) throws Exception {
			state = getRuntimeContext().getState(descriptor);
		}

		@Override
		public void processElement(Long value, Context ctx) throws Exception {
			state.update(value);
		}
	}

	private static class StreamingFunction extends RichMapFunction<Long, Long> {

		private ValueState<Long> state;

		@Override
		public void open(Configuration parameters) throws Exception {
			state = getRuntimeContext().getState(descriptor);
		}

		@Override
		public Long map(Long value) throws Exception {
			return state.value();
		}
	}
}
