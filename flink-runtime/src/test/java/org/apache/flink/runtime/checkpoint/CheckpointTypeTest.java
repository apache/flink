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

package org.apache.flink.runtime.checkpoint;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CheckpointTypeTest {

	/**
	 * This test validates that the order of enumeration constants is not changed, because the
	 * ordinal of that enum is used in serialization.
	 *
	 * <p>It is still possible to edit both the ordinal and this test, but the test adds
	 * a level of safety, and should make developers stumble over this when attempting
	 * to adjust the enumeration.
	 */
	@Test
	public void testOrdinalsAreConstant() {
		assertEquals(0, CheckpointType.CHECKPOINT.ordinal());
		assertEquals(1, CheckpointType.SAVEPOINT.ordinal());
		assertEquals(2, CheckpointType.SYNC_SAVEPOINT.ordinal());
	}
}
