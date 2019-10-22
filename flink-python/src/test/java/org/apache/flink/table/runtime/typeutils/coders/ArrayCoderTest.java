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

package org.apache.flink.table.runtime.typeutils.coders;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;

/**
 * Tests for {@link ArrayCoder}.
 */
public class ArrayCoderTest {

	/**
	 * Test for one dimensional array for {@link ArrayCoder}.
	 */
	public static class OneDimensionalArrayCoderTest extends CoderTestBase<Long[]> {
		@Override
		protected Coder<Long[]> createCoder() {
			return ArrayCoder.of(VarLongCoder.of(), Long.class);
		}

		@Override
		protected Long[][] getTestData() {
			return new Long[][]{{1L, 2L, 3L}, {2L, 3L, 4L}};
		}
	}

	/**
	 * Test for two dimensional array for {@link ArrayCoder}.
	 */
	public static class TwoDimensionalArrayCoderTest extends CoderTestBase<Integer[][]> {
		@Override
		protected Coder<Integer[][]> createCoder() {
			return ArrayCoder.of(ArrayCoder.of(VarIntCoder.of(), Integer.class), Integer[].class);
		}

		@Override
		protected Integer[][][] getTestData() {
			return new Integer[][][]{{{1, 2, 3}, {2, 3, 4}}, {{3, 4, 5}, {4, 5, 6}}};
		}
	}
}
