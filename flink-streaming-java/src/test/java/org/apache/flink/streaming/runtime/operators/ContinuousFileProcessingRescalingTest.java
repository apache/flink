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
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.Preconditions;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness.repackageState;
import static org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness.repartitionOperatorState;

/**
 * Test processing files during rescaling.
 */
public class ContinuousFileProcessingRescalingTest {

	private final int maxParallelism = 10;
	private final int sizeOfSplit = 20;

	/**
	* Simulates the scenario of scaling down from 2 to 1 instances.
	*/
	@Test
	public void testReaderScalingDown() throws Exception {
		HarnessWithFormat[] beforeRescale = buildAndStart(5, 15);

		HarnessWithFormat afterRescale = buildAndStart(1, 0, 5, snapshotAndMergeState(beforeRescale));
		afterRescale.awaitEverythingProcessed();

		for (HarnessWithFormat i : beforeRescale) {
			i.getHarness().getOutput().clear(); // we only want output from the 2nd chunk (after the "checkpoint")
			i.awaitEverythingProcessed();
		}

		Assert.assertEquals(collectOutput(beforeRescale), collectOutput(afterRescale));
	}

	/**
	 * Simulates the scenario of scaling up from 1 to 2 instances.
	 */
	@Test
	public void testReaderScalingUp() throws Exception {
		HarnessWithFormat beforeRescale = buildAndStart(1, 0, 5, null, buildSplits(2));

		OperatorSubtaskState snapshot = beforeRescale.getHarness().snapshot(0, 0);

		HarnessWithFormat afterRescale0 = buildAndStart(2, 0, 15, repartitionOperatorState(snapshot, maxParallelism, 1, 2, 0));
		HarnessWithFormat afterRescale1 = buildAndStart(2, 1, 15, repartitionOperatorState(snapshot, maxParallelism, 1, 2, 1));

		beforeRescale.getHarness().getOutput().clear();

		for (HarnessWithFormat harness : Arrays.asList(beforeRescale, afterRescale0, afterRescale1)) {
			harness.awaitEverythingProcessed();
		}

		Assert.assertEquals(collectOutput(beforeRescale), collectOutput(afterRescale0, afterRescale1));
	}

	private HarnessWithFormat[] buildAndStart(int... elementsBeforeCheckpoint) throws Exception {
		int count = elementsBeforeCheckpoint.length;
		FileInputSplit[] splits = buildSplits(count);
		HarnessWithFormat[] res = new HarnessWithFormat[count];
		for (int i = 0; i < count; i++) {
			res[i] = buildAndStart(2, i, elementsBeforeCheckpoint[i], null, splits[i]);
		}
		return res;
	}

	private HarnessWithFormat buildAndStart(
			int noOfTasks,
			int taskIdx,
			int elementsBeforeCheckpoint,
			@Nullable OperatorSubtaskState initState,
			FileInputSplit... splits) throws Exception {

		BlockingFileInputFormat format = new BlockingFileInputFormat(new Path("test"), sizeOfSplit, elementsBeforeCheckpoint);

		OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, String> harness = getTestHarness(format, noOfTasks, taskIdx);
		harness.setup();
		if (initState != null) {
			harness.initializeState(initState);
		}
		harness.open();
		if (splits != null) {
			for (int i = 0; i < splits.length; i++) {
				harness.processElement(new StreamRecord<>(getTimestampedSplit(i, splits[i])));
			}
		}

		format.awaitFirstChunkProcessed();
		return new HarnessWithFormat(harness, format);
	}

	private OperatorSubtaskState snapshotAndMergeState(HarnessWithFormat[] hh) throws Exception {
		OperatorSubtaskState[] oss = new OperatorSubtaskState[hh.length];
		for (int i = 0; i < hh.length; i++) {
			oss[i] = hh[i].getHarness().snapshot(0, 0);
		}
		return repartitionOperatorState(repackageState(oss), maxParallelism, hh.length, 1, 0);
	}

	private FileInputSplit[] buildSplits(int n) {
		return new BlockingFileInputFormat(new Path("test"), sizeOfSplit, 5).createInputSplits(n);
	}

	private OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, String> getTestHarness(
		BlockingFileInputFormat format, int noOfTasks, int taskIdx) throws Exception {

		ContinuousFileReaderOperator<String> reader = new ContinuousFileReaderOperator<>(format);
		reader.setOutputType(TypeExtractor.getInputFormatTypes(format), new ExecutionConfig());

		OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, String> testHarness =
			new OneInputStreamOperatorTestHarness<>(reader, maxParallelism, noOfTasks, taskIdx);
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

	private static class BlockingFileInputFormat extends FileInputFormat<String> implements CheckpointableInputFormat<FileInputSplit, Integer> {
		private final OneShotLatch firstChunkTrigger = new OneShotLatch();
		private final OneShotLatch continueLatch = new OneShotLatch();
		private final OneShotLatch endTrigger = new OneShotLatch();
		private final int elementsBeforeCheckpoint;
		private final int linesPerSplit;

		private FileInputSplit split;
		private int state = 0;

		BlockingFileInputFormat(Path filePath, int sizeOfSplit, int elementsBeforeCheckpoint) {
			super(filePath);
			this.elementsBeforeCheckpoint = elementsBeforeCheckpoint;
			this.linesPerSplit = sizeOfSplit;
		}

		@Override
		public FileInputSplit[] createInputSplits(int minNumSplits) {
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
		public void reopen(FileInputSplit split, Integer state) {
			this.split = split;
			this.state = state;
		}

		@Override
		public Integer getCurrentState() {
			return state;
		}

		@Override
		public boolean reachedEnd() {
			if (state == elementsBeforeCheckpoint) {
				firstChunkTrigger.trigger();
				try {
					continueLatch.await();
				} catch (InterruptedException e) {
					e.printStackTrace();
					Thread.currentThread().interrupt();
				}
			}
			boolean ended = state == linesPerSplit;
			if (ended) {
				endTrigger.trigger();
			}
			return ended;
		}

		@Override
		public String nextRecord(String reuse) {
			return reachedEnd() ? null : split.getSplitNumber() + ": test line " + state++;
		}

		public void awaitFirstChunkProcessed() throws InterruptedException {
			firstChunkTrigger.await();
		}

		public void awaitLastProcessed() throws InterruptedException {
			endTrigger.await();
		}

		public void resume() {
			continueLatch.trigger();
		}
	}

	private static final class HarnessWithFormat {
		private final OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, String> harness;
		private final BlockingFileInputFormat format;

		HarnessWithFormat(OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, String> harness, BlockingFileInputFormat format) {
			this.format = format;
			this.harness = harness;
		}

		public OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, String> getHarness() {
			return harness;
		}

		public BlockingFileInputFormat getFormat() {
			return format;
		}

		public void awaitEverythingProcessed() throws Exception {
			getFormat().awaitFirstChunkProcessed();
			getFormat().resume();
			getFormat().awaitLastProcessed();
		}
	}

	private List<Object> collectOutput(HarnessWithFormat... in) {
		return Stream.of(in)
				.flatMap(i -> i.getHarness().getOutput().stream())
				.filter(output -> !(output instanceof Watermark))
				.collect(Collectors.toList());
	}
}
