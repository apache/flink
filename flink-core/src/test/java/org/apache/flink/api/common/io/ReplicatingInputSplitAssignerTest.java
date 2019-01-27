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

package org.apache.flink.api.common.io;

import org.apache.flink.core.io.InputSplit;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for ReplicatingInputSplitAssigner.
 */
public class ReplicatingInputSplitAssignerTest {

	@Test
	public void testInputSplitsAssigned() {
		List<InputSplit> inputSplits = new ArrayList<>(10);
		for (int i = 0; i < 10; i++) {
			inputSplits.add(new TestingInputSplit(i));
		}
		ReplicatingInputSplitAssigner assigner = new ReplicatingInputSplitAssigner(inputSplits);
		assertEquals(0, assigner.getNextInputSplit("", 1).getSplitNumber());

		List<InputSplit> assigned = inputSplits.subList(0, 5);
		assigner.inputSplitsAssigned(3, assigned);
		assertEquals(5, assigner.getNextInputSplit("", 3).getSplitNumber());
	}

	private static class TestingInputSplit implements InputSplit {
		private final int splitNumber;

		public TestingInputSplit(int splitNumber) {
			this.splitNumber = splitNumber;
		}

		@Override
		public int getSplitNumber() {
			return splitNumber;
		}
	}
}
