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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

/**
 * Tests the {@link ImmutableReducingState}.
 */
public class ImmutableReducingStateTest {

	private final ReducingStateDescriptor<Long> reducingStateDesc =
			new ReducingStateDescriptor<>("test", new SumReduce(), BasicTypeInfo.LONG_TYPE_INFO);

	private ImmutableReducingState<Long> reduceState;

	@Before
	public void setUp() throws Exception {
		if (!reducingStateDesc.isSerializerInitialized()) {
			reducingStateDesc.initializeSerializerUnlessSet(new ExecutionConfig());
		}

		reduceState = ImmutableReducingState.createState(
				reducingStateDesc,
				ByteBuffer.allocate(Long.BYTES).putLong(42L).array()
		);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUpdate() {
		long value = reduceState.get();
		assertEquals(42L, value);

		reduceState.add(54L);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testClear() {
		long value = reduceState.get();
		assertEquals(42L, value);

		reduceState.clear();
	}

	/**
	 * Test {@link ReduceFunction} summing up its two arguments.
	 */
	private static class SumReduce implements ReduceFunction<Long> {

		private static final long serialVersionUID = 6041237513913189144L;

		@Override
		public Long reduce(Long value1, Long value2) throws Exception {
			return value1 + value2;
		}
	}
}
