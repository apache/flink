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
 * Test for {@link TimestampTypeInfo}.
 */
public class TimestampTypeInfoTest extends TypeInformationTestBase<TimestampTypeInfo> {
	@Override
	protected TimestampTypeInfo[] getTestData() {
		return new TimestampTypeInfo[]{
			new TimestampTypeInfo(9),
			new TimestampTypeInfo(6),
			new TimestampTypeInfo(3),
			new TimestampTypeInfo(0)
		};
	}

	@Test
	public void testEquality() {
		TimestampTypeInfo ti1 = new TimestampTypeInfo(9);
		TimestampTypeInfo ti2 = new TimestampTypeInfo(9);
		TimestampTypeInfo ti3 = new TimestampTypeInfo(3);

		Assert.assertEquals(ti1, ti2);
		Assert.assertEquals(ti1.hashCode(), ti2.hashCode());
		Assert.assertNotEquals(ti1, ti3);
	}
}
