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

public class BooleanPrimitiveArrayComparatorTest extends PrimitiveArrayComparatorTestBase<boolean[]> {
	public BooleanPrimitiveArrayComparatorTest() {
		super(PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO);
	}

	@Override
	protected void deepEquals(String message, boolean[] should, boolean[] is) {
		Assert.assertTrue(should.length == is.length);
		for(int x=0; x< should.length; x++) {
			Assert.assertEquals(should[x], is[x]);
		}
	}

	@Override
	protected boolean[][] getSortedTestData() {
		return new boolean[][]{
			new boolean[]{false, false},
			new boolean[]{false, true},
			new boolean[]{false, true, true},
			new boolean[]{true},
		};
	}
}
