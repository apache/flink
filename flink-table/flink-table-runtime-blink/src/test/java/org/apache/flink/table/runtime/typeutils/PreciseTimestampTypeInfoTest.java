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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeutils.TypeInformationTestBase;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link PreciseTimestampTypeInfo}.
 */
public class PreciseTimestampTypeInfoTest extends TypeInformationTestBase<PreciseTimestampTypeInfo> {
	@Override
	protected PreciseTimestampTypeInfo[] getTestData() {
		return new PreciseTimestampTypeInfo[]{
			new PreciseTimestampTypeInfo(9),
			new PreciseTimestampTypeInfo(6),
			new PreciseTimestampTypeInfo(3),
			new PreciseTimestampTypeInfo(0)
		};
	}

	@Test
	public void testEquality() {
		PreciseTimestampTypeInfo ti1 = new PreciseTimestampTypeInfo(9);
		PreciseTimestampTypeInfo ti2 = new PreciseTimestampTypeInfo(9);
		PreciseTimestampTypeInfo ti3 = new PreciseTimestampTypeInfo(3);

		Assert.assertEquals(ti1, ti2);
		Assert.assertEquals(ti1.hashCode(), ti2.hashCode());
		Assert.assertNotEquals(ti1, ti3);
	}
}
