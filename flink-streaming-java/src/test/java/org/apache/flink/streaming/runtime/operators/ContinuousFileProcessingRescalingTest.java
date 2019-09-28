/*
c * Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.CheckpointableInputFormat;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.ContinuousFileReaderOperator;
import org.apache.flink.streaming.api.functions.source.TimestampedFileInputSplit;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.Preconditions;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Test processing files during rescaling.
 */
public class ContinuousFileProcessingRescalingTest {

	private final int maxParallelism = 10;

	@Test
	public void testReaderScalingDown() throws Exception {
		// simulates the scenario of scaling down from 2 to 1 instances

		final OneShotLatch waitingLatch = new OneShotLatch();

		// create the first instance and let it process the first split till element 5
		final OneShotLatch triggerLatch1 = new OneShotLatch();
		BlockingFileInputFormat format1 = new BlockingFileInputFormat(
			triggerLatch1, waitingLatch, new Path("test"), 20, 5);
		FileInputSplit[] splits = format1.createInputSplits(2);

		OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, String> testHarness1 = getTestHarness(format1, 2, 0);
		testHarness1.open();
		testHarness1.processElement(new StreamRecord<>(getTimestampedSplit(0, splits[0])));

		// wait until its arrives to element 5
		if (!triggerLatch1.isTriggered()) {
			triggerLatch1.await();
		}

		// create the second instance and let it process the second split till element 15
		final OneShotLatch triggerLatch2 = new OneShotLatch();
		BlockingFileInputFormat format2 = new BlockingFileInputFormat(
			triggerLatch2, waitingLatch, new Path("test"), 20, 15);

		OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, String> testHarness2 = getTestHarness(format2, 2, 1);
		testHarness2.open();
		testHarness2.processElement(new StreamRecord<>(getTimestampedSplit(0, splits[1])));

		// wait until its arrives to element 15
		if (!triggerLatch2.isTriggered()) {
			triggerLatch2.await();
		}

		// 1) clear the outputs of the two previous instances so that
		// we can compare their newly produced outputs with the merged one
		testHarness1.getOutput().clear();
		testHarness2.getOutput().clear();

		// 2) take the snapshots from the previous instances and merge them
		// into a new one which will be then used to initialize a third instance
		OperatorSubtaskState mergedState = AbstractStreamOperatorTestHarness.
			repackageState(
				testHarness2.snapshot(0, 0),
				testHarness1.snapshot(0, 0)
			);

		// 3) and repartition to get the initialized state when scaling down.
		OperatorSubtaskState initState =
			AbstractStreamOperatorTestHarness.repartitionOperatorState(mergedState, maxParallelism, 2, 1, 0);

		// create the third instance
		final OneShotLatch wLatch = new OneShotLatch();
		final OneShotLatch tLatch = new OneShotLatch();

		BlockingFileInputFormat format = new BlockingFileInputFormat(wLatch, tLatch, new Path("test"), 20, 5);
		OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, String> testHarness = getTestHarness(format, 1, 0);

		// initialize the state of the new operator with the constructed by
		// combining the partial states of the instances above.
		testHarness.initializeState(initState);
		testHarness.open();

		// now restart the waiting operators
		wLatch.trigger();
		tLatch.trigger();
		waitingLatch.trigger();

		// and wait for the processing to finish
		synchronized (testHarness1.getCheckpointLock()) {
			testHarness1.close();
		}
		synchronized (testHarness2.getCheckpointLock()) {
			testHarness2.close();
		}
		synchronized (testHarness.getCheckpointLock()) {
			testHarness.close();
		}

		Queue<Object> expectedResult = new ArrayDeque<>();
		putElementsInQ(expectedResult, testHarness1.getOutput());
		putElementsInQ(expectedResult, testHarness2.getOutput());

		Queue<Object> actualResult = new ArrayDeque<>();
		putElementsInQ(actualResult, testHarness.getOutput());

		Assert.assertEquals(20, actualResult.size());
		Assert.assertArrayEquals(expectedResult.toArray(), actualResult.toArray());
	}

	@Test
	public void testReaderScalingUp() throws Exception {
		// simulates the scenario of scaling up from 1 to 2 instances

		final OneShotLatch waitingLatch1 = new OneShotLatch();
		final OneShotLatch triggerLatch1 = new OneShotLatch();

		BlockingFileInputFormat format1 = new BlockingFileInputFormat(
			triggerLatch1, waitingLatch1, new Path("test"), 20, 5);
		FileInputSplit[] splits = format1.createInputSplits(2);

		OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, String> testHarness1 = getTestHarness(format1, 1, 0);
		testHarness1.open();

		testHarness1.processElement(new StreamRecord<>(getTimestampedSplit(0, splits[0])));
		testHarness1.processElement(new StreamRecord<>(getTimestampedSplit(1, splits[1])));

		// wait until its arrives to element 5
		if (!triggerLatch1.isTriggered()) {
			triggerLatch1.await();
		}

		OperatorSubtaskState snapshot = testHarness1.snapshot(0, 0);

		// this will be the init state for new instance-0
		OperatorSubtaskState initState1 =
			AbstractStreamOperatorTestHarness.repartitionOperatorState(snapshot, maxParallelism, 1, 2, 0);

		// this will be the init state for new instance-1
		OperatorSubtaskState initState2 =
			AbstractStreamOperatorTestHarness.repartitionOperatorState(snapshot, maxParallelism, 1, 2, 1);

		// 1) clear the output of instance so that we can compare it with one created by the new instances, and
		// 2) let the operator process the rest of its state
		testHarness1.getOutput().clear();
		waitingLatch1.trigger();

		// create the second instance and let it process the second split till element 15
		final OneShotLatch triggerLatch2 = new OneShotLatch();
		final OneShotLatch waitingLatch2 = new OneShotLatch();

		BlockingFileInputFormat format2 = new BlockingFileInputFormat(
			triggerLatch2, waitingLatch2, new Path("test"), 20, 15);

		OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, String> testHarness2 = getTestHarness(format2, 2, 0);
		testHarness2.setup();
		testHarness2.initializeState(initState1);
		testHarness2.open();

		BlockingFileInputFormat format3 = new BlockingFileInputFormat(
			triggerLatch2, waitingLatch2, new Path("test"), 20, 15);

		OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, String> testHarness3 = getTestHarness(format3, 2, 1);
		testHarness3.setup();
		testHarness3.initializeState(initState2);
		testHarness3.open();

		triggerLatch2.trigger();
		waitingLatch2.trigger();

		// and wait for the processing to finish
		synchronized (testHarness1.getCheckpointLock()) {
			testHarness1.close();
		}
		synchronized (testHarness2.getCheckpointLock()) {
			testHarness2.close();
		}
		synchronized (testHarness3.getCheckpointLock()) {
			testHarness3.close();
		}

		Queue<Object> expectedResult = new ArrayDeque<>();
		putElementsInQ(expectedResult, testHarness1.getOutput());

		Queue<Object> actualResult = new ArrayDeque<>();
		putElementsInQ(actualResult, testHarness2.getOutput());
		putElementsInQ(actualResult, testHarness3.getOutput());

		Assert.assertEquals(35, actualResult.size());
		Assert.assertArrayEquals(expectedResult.toArray(), actualResult.toArray());
	}

	private void putElementsInQ(Queue<Object> res, Queue<Object> partial) {
		for (Object o : partial) {
			if (o instanceof Watermark) {
				continue;
			}
			res.add(o);
		}
	}

	private OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, String> getTestHarness(
		BlockingFileInputFormat format, int noOfTasks, int taksIdx) throws Exception {

		ContinuousFileReaderOperator<String> reader = new ContinuousFileReaderOperator<>(format);
		reader.setOutputType(TypeExtractor.getInputFormatTypes(format), new ExecutionConfig());

		OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, String> testHarness =
			new OneInputStreamOperatorTestHarness<>(reader, maxParallelism, noOfTasks, taksIdx);
		testHarness.setTimeCharacteristic(TimeCharacteristic.EventTime);
		return testHarness;
	}

	private TimestampedFileInputSplit getTimestampedSplit(long modTime, FileInputSplit split) {
		Preconditions.checkNotNull(split);
		return new TimestampedFileInputSplit(
			modTime,
			split.getSplitNumber(),
			split.getPath(),
			split.getStart(),
			split.getLength(),
			split.getHostnames());
	}

	private static class BlockingFileInputFormat
		extends FileInputFormat<String>
		implements CheckpointableInputFormat<FileInputSplit, Integer> {

		private final OneShotLatch triggerLatch;
		private final OneShotLatch waitingLatch;

		private final int elementsBeforeCheckpoint;
		private final int linesPerSplit;

		private FileInputSplit split;

		private int state;

		BlockingFileInputFormat(OneShotLatch triggerLatch,
								OneShotLatch waitingLatch,
								Path filePath,
								int sizeOfSplit,
								int elementsBeforeCheckpoint) {
			super(filePath);

			this.triggerLatch = triggerLatch;
			this.waitingLatch = waitingLatch;
			this.elementsBeforeCheckpoint = elementsBeforeCheckpoint;
			this.linesPerSplit = sizeOfSplit;

			this.state = 0;
		}

		@Override
		public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
			FileInputSplit[] splits = new FileInputSplit[minNumSplits];
			for (int i = 0; i < minNumSplits; i++) {
				splits[i] = new FileInputSplit(i, getFilePaths()[0], i * linesPerSplit + 1, linesPerSplit, null);
			}
			return splits;
		}

		@Override
		public void open(FileInputSplit fileSplit) throws IOException {
			this.split = fileSplit;
			this.state = 0;
		}

		@Override
		public void reopen(FileInputSplit split, Integer state) throws IOException {
			this.split = split;
			this.state = state;
		}

		@Override
		public Integer getCurrentState() throws IOException {
			return state;
		}

		@Override
		public boolean reachedEnd() throws IOException {
			if (state == elementsBeforeCheckpoint) {
				triggerLatch.trigger();
				if (!waitingLatch.isTriggered()) {
					try {
						waitingLatch.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			return state == linesPerSplit;
		}

		@Override
		public String nextRecord(String reuse) throws IOException {
			return reachedEnd() ? null : split.getSplitNumber() + ": test line " + state++;
		}
	}
}
