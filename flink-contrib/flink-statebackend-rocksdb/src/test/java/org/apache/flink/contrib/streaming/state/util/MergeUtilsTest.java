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
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Tests for MergeUtils.
 */
public class MergeUtilsTest {

	@Test
	public void testMergeMulti() {
		List<byte[]> list = Arrays.asList(
			new byte[]{0, 1, 2, 3},
			new byte[]{4},
			new byte[]{5, 6});

		byte[] expected = new byte[]{0, 1, 2, 3, MergeUtils.DELIMITER, 4, MergeUtils.DELIMITER, 5, 6};
		assertTrue(Arrays.equals(expected, MergeUtils.merge(list)));
	}

	@Test
	public void testMergeEmptyList() {
		// Empty list
		assertTrue(Arrays.equals(null, MergeUtils.merge(Collections.emptyList())));
	}

	@Test
	public void testMergeSingleton() {
		// Singleton list
		byte[] singletonData = new byte[] {0x42};
		assertTrue(Arrays.equals(singletonData, MergeUtils.merge(Arrays.asList(singletonData))));
	}
}
