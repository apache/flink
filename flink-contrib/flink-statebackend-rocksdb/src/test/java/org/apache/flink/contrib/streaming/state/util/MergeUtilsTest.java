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

package org.apache.flink.contrib.streaming.state.util;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Tests for MergeUtils.
 */
public class MergeUtilsTest {
	@Test
	public void testMerge() {
		List<byte[]> list = Arrays.asList(
				new byte[4],
				new byte[1],
				new byte[2]);

		byte[] expected = new byte[9];
		expected[4] = MergeUtils.DELIMITER;
		expected[6] = MergeUtils.DELIMITER;

		assertTrue(Arrays.equals(expected, MergeUtils.merge(list)));

		// Empty list
		list = Arrays.asList();

		assertTrue(Arrays.equals(null, MergeUtils.merge(list)));

		// Singleton list
		list = Arrays.asList(new byte[1]);

		assertTrue(Arrays.equals(new byte[1], MergeUtils.merge(list)));
	}
}
