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

package org.apache.flink.queryablestate.client.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertEquals;

/**
 * Tests the {@link ImmutableAggregatingStateTest}.
 */
public class ImmutableAggregatingStateTest {

	private final AggregatingStateDescriptor<Long, String, String> aggrStateDesc =
			new AggregatingStateDescriptor<>(
					"test",
					new SumAggr(),
					String.class);

	private AggregatingState<Long, String> aggrState;

	@Before
	public void setUp() throws Exception {
		if (!aggrStateDesc.isSerializerInitialized()) {
			aggrStateDesc.initializeSerializerUnlessSet(new ExecutionConfig());
		}

		final String initValue = "42";

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		aggrStateDesc.getSerializer().serialize(initValue, new DataOutputViewStreamWrapper(out));

		aggrState = ImmutableAggregatingState.createState(
				aggrStateDesc,
				out.toByteArray()
		);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUpdate() throws Exception {
		String value = aggrState.get();
		assertEquals("42", value);

		aggrState.add(54L);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testClear() throws Exception {
		String value = aggrState.get();
		assertEquals("42", value);

		aggrState.clear();
	}

	/**
	 * Test {@link AggregateFunction} concatenating the already stored string with the long passed as argument.
	 */
	private static class SumAggr implements AggregateFunction<Long, String, String> {

		private static final long serialVersionUID = -6249227626701264599L;

		@Override
		public String createAccumulator() {
			return "";
		}

		@Override
		public String add(Long value, String accumulator) {
			accumulator += ", " + value;
			return accumulator;
		}

		@Override
		public String getResult(String accumulator) {
			return accumulator;
		}

		@Override
		public String merge(String a, String b) {
			return a + ", " + b;
		}
	}
}
