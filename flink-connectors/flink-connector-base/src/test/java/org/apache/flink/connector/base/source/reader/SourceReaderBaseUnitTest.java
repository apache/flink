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

package org.apache.flink.connector.base.source.reader;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.mocks.PassThroughRecordEmitter;
import org.apache.flink.connector.base.source.reader.mocks.TestingReaderContext;
import org.apache.flink.connector.base.source.reader.mocks.TestingReaderOutput;
import org.apache.flink.connector.base.source.reader.mocks.TestingRecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.mocks.TestingSourceSplit;
import org.apache.flink.connector.base.source.reader.mocks.TestingSplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.base.source.reader.synchronization.FutureNotifier;

import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Targeted unit tests for the {@link SourceReaderBase}.
 */
public class SourceReaderBaseUnitTest {

	@Test
	public void recordsWithSplitsNotRecycledWhenRecordsLeft() throws Exception {
		final TestingRecordsWithSplitIds<String> records = new TestingRecordsWithSplitIds<>("test-split", "value1", "value2");
		final SourceReader<?, ?> reader = createReaderAndAwaitAvailable("test-split", records);

		reader.pollNext(new TestingReaderOutput<>());

		assertFalse(records.isRecycled());
	}

	@Test
	public void testRecordsWithSplitsRecycledWhenEmpty() throws Exception {
		final TestingRecordsWithSplitIds<String> records = new TestingRecordsWithSplitIds<>("test-split", "value1", "value2");
		final SourceReader<?, ?> reader = createReaderAndAwaitAvailable("test-split", records);

		// poll thrice: twice to get all records, one more to trigger recycle and moving to the next split
		reader.pollNext(new TestingReaderOutput<>());
		reader.pollNext(new TestingReaderOutput<>());
		reader.pollNext(new TestingReaderOutput<>());

		assertTrue(records.isRecycled());
	}

	// ------------------------------------------------------------------------
	//  Testing Setup Helpers
	// ------------------------------------------------------------------------

	private static <E> SourceReader<E, ?> createReaderAndAwaitAvailable(
			final String splitId,
			final RecordsWithSplitIds<E> records) throws Exception {

		final FutureNotifier futureNotifier = new FutureNotifier();
		final FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue =
			new FutureCompletingBlockingQueue<>(futureNotifier);

		final SourceReader<E, TestingSourceSplit> reader = new SingleThreadMultiplexSourceReaderBase<>(
				futureNotifier,
				elementsQueue,
				() -> new TestingSplitReader<E, TestingSourceSplit>(records),
				new PassThroughRecordEmitter<E, TestingSourceSplit>(),
				new Configuration(),
				new TestingReaderContext()) {

			@Override
			protected void onSplitFinished(Collection<String> finishedSplitIds) {
			}

			@Override
			protected TestingSourceSplit initializedState(TestingSourceSplit split) {
				return split;
			}

			@Override
			protected TestingSourceSplit toSplitType(String splitId, TestingSourceSplit splitState) {
				return splitState;
			}
		};

		reader.start();

		final List<TestingSourceSplit> splits = Collections.singletonList(new TestingSourceSplit(splitId));
		reader.addSplits(splits);

		reader.isAvailable().get();

		return reader;
	}
}
