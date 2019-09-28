/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.hdfstests;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction;
import org.apache.flink.streaming.api.functions.source.ContinuousFileReaderOperator;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.TimestampedFileInputSplit;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OperatorSnapshotUtil;
import org.apache.flink.testutils.migration.MigrationVersion;
import org.apache.flink.util.OperatingSystem;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Tests that verify the migration from previous Flink version snapshots.
 */
@RunWith(Parameterized.class)
public class ContinuousFileProcessingMigrationTest {

	private static final int LINES_PER_FILE = 10;

	private static final long INTERVAL = 100;

	@Parameterized.Parameters(name = "Migration Savepoint / Mod Time: {0}")
	public static Collection<Tuple2<MigrationVersion, Long>> parameters () {
		return Arrays.asList(
			Tuple2.of(MigrationVersion.v1_2, 1493116191000L),
			Tuple2.of(MigrationVersion.v1_3, 1496532000000L),
			Tuple2.of(MigrationVersion.v1_4, 1516897628000L),
			Tuple2.of(MigrationVersion.v1_5, 1533639934000L),
			Tuple2.of(MigrationVersion.v1_6, 1534696817000L),
			Tuple2.of(MigrationVersion.v1_7, 1544024599000L),
			Tuple2.of(MigrationVersion.v1_8, 1555215710000L),
			Tuple2.of(MigrationVersion.v1_9, 1567499868000L));
	}

	/**
	 * TODO change this to the corresponding savepoint version to be written (e.g. {@link MigrationVersion#v1_3} for 1.3)
	 * TODO and remove all @Ignore annotations on write*Snapshot() methods to generate savepoints
	 * TODO Note: You should generate the savepoint based on the release branch instead of the master.
	 */
	private final MigrationVersion flinkGenerateSavepointVersion = null;

	private final MigrationVersion testMigrateVersion;
	private final Long expectedModTime;

	public ContinuousFileProcessingMigrationTest(Tuple2<MigrationVersion, Long> migrationVersionAndModTime) {
		this.testMigrateVersion = migrationVersionAndModTime.f0;
		this.expectedModTime = migrationVersionAndModTime.f1;
	}

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	@BeforeClass
	public static void verifyOS() {
		Assume.assumeTrue("HDFS cluster cannot be start on Windows without extensions.", !OperatingSystem.isWindows());
	}

	/**
	 * Manually run this to write binary snapshot data. Remove @Ignore to run.
	 */
	@Ignore
	@Test
	public void writeReaderSnapshot() throws Exception {

		File testFolder = tempFolder.newFolder();

		TimestampedFileInputSplit split1 =
				new TimestampedFileInputSplit(0, 3, new Path("test/test1"), 0, 100, null);

		TimestampedFileInputSplit split2 =
				new TimestampedFileInputSplit(10, 2, new Path("test/test2"), 101, 200, null);

		TimestampedFileInputSplit split3 =
				new TimestampedFileInputSplit(10, 1, new Path("test/test2"), 0, 100, null);

		TimestampedFileInputSplit split4 =
				new TimestampedFileInputSplit(11, 0, new Path("test/test3"), 0, 100, null);

		// this always blocks to ensure that the reader doesn't to any actual processing so that
		// we keep the state for the four splits
		final OneShotLatch blockingLatch = new OneShotLatch();
		BlockingFileInputFormat format = new BlockingFileInputFormat(blockingLatch, new Path(testFolder.getAbsolutePath()));

		TypeInformation<FileInputSplit> typeInfo = TypeExtractor.getInputFormatTypes(format);
		ContinuousFileReaderOperator<FileInputSplit> initReader = new ContinuousFileReaderOperator<>(
				format);
		initReader.setOutputType(typeInfo, new ExecutionConfig());
		OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, FileInputSplit> testHarness =
				new OneInputStreamOperatorTestHarness<>(initReader);
		testHarness.setTimeCharacteristic(TimeCharacteristic.EventTime);
		testHarness.open();
		// create some state in the reader
		testHarness.processElement(new StreamRecord<>(split1));
		testHarness.processElement(new StreamRecord<>(split2));
		testHarness.processElement(new StreamRecord<>(split3));
		testHarness.processElement(new StreamRecord<>(split4));
		// take a snapshot of the operator's state. This will be used
		// to initialize another reader and compare the results of the
		// two operators.

		final OperatorSubtaskState snapshot;
		synchronized (testHarness.getCheckpointLock()) {
			snapshot = testHarness.snapshot(0L, 0L);
		}

		OperatorSnapshotUtil.writeStateHandle(snapshot, "src/test/resources/reader-migration-test-flink" + flinkGenerateSavepointVersion + "-snapshot");
	}

	@Test
	public void testReaderRestore() throws Exception {
		File testFolder = tempFolder.newFolder();

		final OneShotLatch latch = new OneShotLatch();

		BlockingFileInputFormat format = new BlockingFileInputFormat(latch, new Path(testFolder.getAbsolutePath()));
		TypeInformation<FileInputSplit> typeInfo = TypeExtractor.getInputFormatTypes(format);

		ContinuousFileReaderOperator<FileInputSplit> initReader = new ContinuousFileReaderOperator<>(format);
		initReader.setOutputType(typeInfo, new ExecutionConfig());

		OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, FileInputSplit> testHarness =
			new OneInputStreamOperatorTestHarness<>(initReader);
		testHarness.setTimeCharacteristic(TimeCharacteristic.EventTime);

		testHarness.setup();

		testHarness.initializeState(
			OperatorSnapshotUtil.getResourceFilename(
				"reader-migration-test-flink" + testMigrateVersion + "-snapshot"));

		testHarness.open();

		latch.trigger();

		// ... and wait for the operators to close gracefully

		synchronized (testHarness.getCheckpointLock()) {
			testHarness.close();
		}

		TimestampedFileInputSplit split1 =
				new TimestampedFileInputSplit(0, 3, new Path("test/test1"), 0, 100, null);

		TimestampedFileInputSplit split2 =
				new TimestampedFileInputSplit(10, 2, new Path("test/test2"), 101, 200, null);

		TimestampedFileInputSplit split3 =
				new TimestampedFileInputSplit(10, 1, new Path("test/test2"), 0, 100, null);

		TimestampedFileInputSplit split4 =
				new TimestampedFileInputSplit(11, 0, new Path("test/test3"), 0, 100, null);

		// compare if the results contain what they should contain and also if
		// they are the same, as they should.

		Assert.assertTrue(testHarness.getOutput().contains(new StreamRecord<>(split1)));
		Assert.assertTrue(testHarness.getOutput().contains(new StreamRecord<>(split2)));
		Assert.assertTrue(testHarness.getOutput().contains(new StreamRecord<>(split3)));
		Assert.assertTrue(testHarness.getOutput().contains(new StreamRecord<>(split4)));
	}

	/**
	 * Manually run this to write binary snapshot data. Remove @Ignore to run.
	 */
	@Ignore
	@Test
	public void writeMonitoringSourceSnapshot() throws Exception {

		File testFolder = tempFolder.newFolder();

		long fileModTime = Long.MIN_VALUE;
		for (int i = 0; i < 1; i++) {
			Tuple2<File, String> file = createFileAndFillWithData(testFolder, "file", i, "This is test line.");
			fileModTime = file.f0.lastModified();
		}

		TextInputFormat format = new TextInputFormat(new Path(testFolder.getAbsolutePath()));

		final ContinuousFileMonitoringFunction<String> monitoringFunction =
			new ContinuousFileMonitoringFunction<>(format, FileProcessingMode.PROCESS_CONTINUOUSLY, 1, INTERVAL);

		StreamSource<TimestampedFileInputSplit, ContinuousFileMonitoringFunction<String>> src =
			new StreamSource<>(monitoringFunction);

		final AbstractStreamOperatorTestHarness<TimestampedFileInputSplit> testHarness =
				new AbstractStreamOperatorTestHarness<>(src, 1, 1, 0);

		testHarness.open();

		final Throwable[] error = new Throwable[1];

		final OneShotLatch latch = new OneShotLatch();

		// run the source asynchronously
		Thread runner = new Thread() {
			@Override
			public void run() {
				try {
					monitoringFunction.run(new DummySourceContext() {
						@Override
						public void collect(TimestampedFileInputSplit element) {
							latch.trigger();
						}

						@Override
						public void markAsTemporarilyIdle() {

						}
					});
				}
				catch (Throwable t) {
					t.printStackTrace();
					error[0] = t;
				}
			}
		};
		runner.start();

		if (!latch.isTriggered()) {
			latch.await();
		}

		final OperatorSubtaskState snapshot;
		synchronized (testHarness.getCheckpointLock()) {
			snapshot = testHarness.snapshot(0L, 0L);
		}

		OperatorSnapshotUtil.writeStateHandle(
				snapshot,
				"src/test/resources/monitoring-function-migration-test-" + fileModTime + "-flink" + flinkGenerateSavepointVersion + "-snapshot");

		monitoringFunction.cancel();
		runner.join();

		testHarness.close();
	}

	@Test
	public void testMonitoringSourceRestore() throws Exception {

		File testFolder = tempFolder.newFolder();

		TextInputFormat format = new TextInputFormat(new Path(testFolder.getAbsolutePath()));

		final ContinuousFileMonitoringFunction<String> monitoringFunction =
			new ContinuousFileMonitoringFunction<>(format, FileProcessingMode.PROCESS_CONTINUOUSLY, 1, INTERVAL);

		StreamSource<TimestampedFileInputSplit, ContinuousFileMonitoringFunction<String>> src =
			new StreamSource<>(monitoringFunction);

		final AbstractStreamOperatorTestHarness<TimestampedFileInputSplit> testHarness =
			new AbstractStreamOperatorTestHarness<>(src, 1, 1, 0);

		testHarness.setup();

		testHarness.initializeState(
			OperatorSnapshotUtil.getResourceFilename(
				"monitoring-function-migration-test-" + expectedModTime + "-flink" + testMigrateVersion + "-snapshot"));

		testHarness.open();

		Assert.assertEquals((long) expectedModTime, monitoringFunction.getGlobalModificationTime());

	}

	private static class BlockingFileInputFormat extends FileInputFormat<FileInputSplit> {

		private static final long serialVersionUID = -6727603565381560267L;

		private final OneShotLatch latch;

		private FileInputSplit split;

		private boolean reachedEnd;

		BlockingFileInputFormat(OneShotLatch latch, Path filePath) {
			super(filePath);
			this.latch = latch;
			this.reachedEnd = false;
		}

		@Override
		public void open(FileInputSplit fileSplit) throws IOException {
			this.split = fileSplit;
			this.reachedEnd = false;
		}

		@Override
		public boolean reachedEnd() throws IOException {
			if (!latch.isTriggered()) {
				try {
					latch.await();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			return reachedEnd;
		}

		@Override
		public FileInputSplit nextRecord(FileInputSplit reuse) throws IOException {
			this.reachedEnd = true;
			return split;
		}

		@Override
		public void close() {

		}
	}

	private abstract static class DummySourceContext
		implements SourceFunction.SourceContext<TimestampedFileInputSplit> {

		private final Object lock = new Object();

		@Override
		public void collectWithTimestamp(TimestampedFileInputSplit element, long timestamp) {
		}

		@Override
		public void emitWatermark(Watermark mark) {
		}

		@Override
		public Object getCheckpointLock() {
			return lock;
		}

		@Override
		public void close() {
		}
	}

	/**
	 * Create a file with pre-determined String format of the form:
	 * {@code fileIdx +": "+ sampleLine +" "+ lineNo}.
	 * */
	private Tuple2<File, String> createFileAndFillWithData(
		File base, String fileName, int fileIdx, String sampleLine) throws IOException {

		File file = new File(base, fileName + fileIdx);
		Assert.assertFalse(file.exists());

		File tmp = new File(base, "." + fileName + fileIdx);
		FileOutputStream stream = new FileOutputStream(tmp);
		StringBuilder str = new StringBuilder();
		for (int i = 0; i < LINES_PER_FILE; i++) {
			String line = fileIdx + ": " + sampleLine + " " + i + "\n";
			str.append(line);
			stream.write(line.getBytes());
		}
		stream.close();

		FileUtils.moveFile(tmp, file);

		Assert.assertTrue("No result file present", file.exists());
		return new Tuple2<>(file, str.toString());
	}

	private FileInputSplit createSplitFromTimestampedSplit(TimestampedFileInputSplit split) {
		checkNotNull(split);

		return new FileInputSplit(
			split.getSplitNumber(),
			split.getPath(),
			split.getStart(),
			split.getLength(),
			split.getHostnames()
		);
	}
}
