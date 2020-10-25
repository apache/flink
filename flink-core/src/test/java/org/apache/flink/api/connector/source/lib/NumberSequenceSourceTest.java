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

package org.apache.flink.api.connector.source.lib;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.mocks.TestingReaderContext;
import org.apache.flink.api.connector.source.mocks.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.fail;

/**
 * Tests for the {@link NumberSequenceSource}.
 */
public class NumberSequenceSourceTest {

	@Test
	public void testReaderCheckpoints() throws Exception {
		final long from = 177;
		final long mid = 333;
		final long to = 563;
		final long elementsPerCycle = (to - from) / 3;

		final TestingReaderOutput<Long> out = new TestingReaderOutput<>();

		SourceReader<Long, NumberSequenceSource.NumberSequenceSplit> reader = createReader();
		reader.addSplits(Arrays.asList(
				new NumberSequenceSource.NumberSequenceSplit("split-1", from, mid),
				new NumberSequenceSource.NumberSequenceSplit("split-2", mid + 1, to)));

		long remainingInCycle = elementsPerCycle;
		while (reader.pollNext(out) != InputStatus.END_OF_INPUT) {
			if (--remainingInCycle <= 0) {
				remainingInCycle = elementsPerCycle;
				// checkpoint
				List<NumberSequenceSource.NumberSequenceSplit> splits = reader.snapshotState(1L);

				// re-create and restore
				reader = createReader();
				reader.addSplits(splits);
			}
		}

		final List<Long> result = out.getEmittedRecords();
		validateSequence(result, from, to);
	}

	private static void validateSequence(final List<Long> sequence, final long from, final long to) {
		if (sequence.size() != to - from + 1) {
			failSequence(sequence, from, to);
		}

		long nextExpected = from;
		for (Long next : sequence) {
			if (next != nextExpected++) {
				failSequence(sequence, from, to);
			}
		}
	}

	private static void failSequence(final List<Long> sequence, final long from, final long to) {
		fail(String.format("Expected: A sequence [%d, %d], but found: sequence (size %d) : %s",
				from, to, sequence.size(), sequence));
	}

	private static SourceReader<Long, NumberSequenceSource.NumberSequenceSplit> createReader() {
		// the arguments passed in the source constructor matter only to the enumerator
		return new NumberSequenceSource(0L, 0L).createReader(new TestingReaderContext());
	}
}
