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

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.CoreMatchers.containsString;

/**
 * Tests for {@link AbstractStreamOperatorTestHarness}.
 */
public class AbstractStreamOperatorTestHarnessTest extends TestLogger {
	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void testInitializeAfterOpenning() throws Throwable {
		expectedException.expect(IllegalStateException.class);
		expectedException.expectMessage(containsString("TestHarness has already been initialized."));

		AbstractStreamOperatorTestHarness<Integer> result;
		result =
			new AbstractStreamOperatorTestHarness<>(
				new AbstractStreamOperator<Integer>() {
				},
				1,
				1,
				0);
		result.setup();
		result.open();
		result.initializeState(new OperatorSubtaskState());
	}

	@Test
	public void testSetTtlTimeProvider() throws Exception {
		AbstractStreamOperator<Integer> operator = new AbstractStreamOperator<Integer>() {};
		try (AbstractStreamOperatorTestHarness<Integer> result = new AbstractStreamOperatorTestHarness<>(
				operator,
				1,
				1,
				0)) {

			result.config.setStateKeySerializer(IntSerializer.INSTANCE);

			Time timeToLive = Time.hours(1);
			result.initializeState(new OperatorSubtaskState());
			result.open();

			ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("test", IntSerializer.INSTANCE);
			stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(timeToLive).build());
			KeyedStateBackend<Integer> keyedStateBackend = operator.getKeyedStateBackend();
			ValueState<Integer> state = keyedStateBackend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);

			int expectedValue = 42;
			keyedStateBackend.setCurrentKey(1);
			result.setStateTtlProcessingTime(0L);
			state.update(expectedValue);
			Assert.assertEquals(expectedValue, (int) state.value());
			result.setStateTtlProcessingTime(timeToLive.toMilliseconds() + 1);
			Assert.assertNull(state.value());
		}
	}
}
