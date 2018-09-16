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
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertEquals;

/**
 * Tests the {@link ImmutableFoldingState}.
 */
public class ImmutableFoldingStateTest {

	private final FoldingStateDescriptor<Long, String> foldingStateDesc =
			new FoldingStateDescriptor<>(
					"test",
					"0",
					new SumFold(),
					StringSerializer.INSTANCE);

	private FoldingState<Long, String> foldingState;

	@Before
	public void setUp() throws Exception {
		if (!foldingStateDesc.isSerializerInitialized()) {
			foldingStateDesc.initializeSerializerUnlessSet(new ExecutionConfig());
		}

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		StringSerializer.INSTANCE.serialize("42", new DataOutputViewStreamWrapper(out));

		foldingState = ImmutableFoldingState.createState(
				foldingStateDesc,
				out.toByteArray()
		);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUpdate() throws Exception {
		String value = foldingState.get();
		assertEquals("42", value);

		foldingState.add(54L);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testClear() throws Exception {
		String value = foldingState.get();
		assertEquals("42", value);

		foldingState.clear();
	}

	/**
	 * Test {@link FoldFunction} concatenating the already stored string with the long passed as argument.
	 */
	private static class SumFold implements FoldFunction<Long, String> {

		private static final long serialVersionUID = -6249227626701264599L;

		@Override
		public String fold(String accumulator, Long value) throws Exception {
			long acc = Long.valueOf(accumulator);
			acc += value;
			return Long.toString(acc);
		}
	}
}
