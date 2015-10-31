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
package org.apache.flink.api.common.typeutils.base.array;

import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.junit.Assert;

public class ShortPrimitiveArrayComparatorTest extends PrimitiveArrayComparatorTestBase<short[]> {
	public ShortPrimitiveArrayComparatorTest() {
		super(PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO);
	}

	@Override
	protected void deepEquals(String message, short[] should, short[] is) {
		Assert.assertArrayEquals(message, should, is);
	}

	@Override
	protected short[][] getSortedTestData() {
		return new short[][]{
			new short[]{-1, 0},
			new short[]{0, -1},
			new short[]{0, 0},
			new short[]{0, 1},
			new short[]{0, 1, 2},
			new short[]{2}
		};
	}
}
