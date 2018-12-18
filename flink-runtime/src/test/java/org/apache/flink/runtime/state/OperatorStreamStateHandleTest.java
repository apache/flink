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

package org.apache.flink.runtime.state;

import org.junit.Assert;
import org.junit.Test;

public class OperatorStreamStateHandleTest {

	@Test
	public void testFixedEnumOrder() {

		// Ensure the order / ordinal of all values of enum 'mode' are fixed, as this is used for serialization
		Assert.assertEquals(0, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE.ordinal());
		Assert.assertEquals(1, OperatorStateHandle.Mode.UNION.ordinal());
		Assert.assertEquals(2, OperatorStateHandle.Mode.BROADCAST.ordinal());

		// Ensure all enum values are registered and fixed forever by this test
		Assert.assertEquals(3, OperatorStateHandle.Mode.values().length);

		// Byte is used to encode enum value on serialization
		Assert.assertTrue(OperatorStateHandle.Mode.values().length <= Byte.MAX_VALUE);
	}
}
