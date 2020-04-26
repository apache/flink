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

package org.apache.flink.table.filesystem.streaming;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.filesystem.FileSystemCommitterTest;
import org.apache.flink.table.filesystem.RowDataPartitionComputer;
import org.apache.flink.table.runtime.functions.SqlDateTimeUtils;
import org.apache.flink.table.types.DataType;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test for {@link FileSystemStreamingSink}.
 */
public class FileSystemStreamingSinkTest {

	private static final String SUCCESS_NAME = "_my_success";

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	private File outputFile;
	private FileSystemCommitterTest.TestMetaStoreFactory msFactory;

	@Before
	public void before() throws IOException {
		outputFile = TEMP_FOLDER.newFolder();
	}

	@Test
	public void testClosingWithoutInput() throws Exception {
		try (OneInputStreamOperatorTestHarness<BaseRow, Object> testHarness = createSink(
				false, new AtomicReference<>())) {
			testHarness.setup();
			testHarness.open();
		}
	}

	private GenericRow toRowData(String f0, int f1, String f2) {
		return GenericRow.of(
				BinaryString.fromString(f0),
				f1,
				BinaryString.fromString(f2)
		);
	}

	@Test
	public void testRecoveryWithPartition() throws Exception {
		OperatorSubtaskState snapshot;

		try (OneInputStreamOperatorTestHarness<BaseRow, Object> testHarness = createSink(true)) {

			testHarness.setup();
			testHarness.open();

			testHarness.processElement(new StreamRecord<>(toRowData("a1", 1, "2020-01-01"), 1L));
			testHarness.processElement(new StreamRecord<>(toRowData("a2", 2, "2020-01-01"), 1L));
			testHarness.processElement(new StreamRecord<>(toRowData("a3", 3, "2020-01-01"), 1L));
			testHarness.processElement(new StreamRecord<>(toRowData("a2", 2, "2020-01-02"), 1L));

			testHarness.snapshot(1L, 1L);

			testHarness.processElement(new StreamRecord<>(toRowData("test1", 2, "2020-01-01"), 1L));
			testHarness.processElement(new StreamRecord<>(toRowData("test1", 3, "2020-01-01"), 1L));

			testHarness.processWatermark(
					SqlDateTimeUtils.dateStringToUnixDate("2020-01-02") *
							SqlDateTimeUtils.MILLIS_PER_DAY);
			testHarness.processElement(new StreamRecord<>(toRowData("test1", 3, "2020-01-02"), 1L));

			snapshot = testHarness.snapshot(2L, 1L);

			Assert.assertEquals(2, getFileContentByPath(new File(outputFile, "c=2020-01-01"), ".part-0").size());
			Assert.assertEquals(2, getFileContentByPath(new File(outputFile, "c=2020-01-02"), ".part-0").size());

			testHarness.processElement(new StreamRecord<>(toRowData("test1", 2, "2020-01-02"), 1L));
			testHarness.processElement(new StreamRecord<>(toRowData("test1", 3, "2020-01-02"), 1L));
			testHarness.processElement(new StreamRecord<>(toRowData("test1", 4, "2020-01-03"), 1L));

			testHarness.notifyOfCompletedCheckpoint(2);

			LinkedHashMap<String, String> part = new LinkedHashMap<>();
			part.put("c", "2020-01-01");
			Set<LinkedHashMap<String, String>> partitionCreated = new HashSet<>();
			partitionCreated.add(part);
			Assert.assertEquals(partitionCreated, msFactory.partitionCreated);

			Assert.assertEquals(1, getFileContentByPath(new File(outputFile, "c=2020-01-01"), SUCCESS_NAME).size());

			testHarness.snapshot(3L, 1L);

			Assert.assertEquals(2, getFileContentByPath(new File(outputFile, "c=2020-01-01"), "part-0").size());
			Assert.assertEquals(2, getFileContentByPath(new File(outputFile, "c=2020-01-02"), "part-0").size());
			Assert.assertEquals(1, getFileContentByPath(new File(outputFile, "c=2020-01-02"), ".part-0").size());
			Assert.assertEquals(1, getFileContentByPath(new File(outputFile, "c=2020-01-03"), ".part-0").size());
		}

		try (OneInputStreamOperatorTestHarness<BaseRow, Object> testHarness = createSink(true)) {

			testHarness.setup();
			testHarness.initializeState(snapshot);
			testHarness.open();

			testHarness.processElement(new StreamRecord<>(toRowData("a4", 1, "2020-01-03"), 1L));
			testHarness.processElement(new StreamRecord<>(toRowData("a5", 2, "2020-01-03"), 1L));
			testHarness.processElement(new StreamRecord<>(toRowData("a6", 3, "2020-01-03"), 1L));

			testHarness.processWatermark(
					SqlDateTimeUtils.dateStringToUnixDate("2020-01-04") *
							SqlDateTimeUtils.MILLIS_PER_DAY);
			testHarness.processElement(new StreamRecord<>(toRowData("a4", 2, "2020-01-04"), 1L));

			testHarness.snapshot(3L, 1L);

			// NOTE: StreamingFileSink not clean previous temporary file, and create a new file.
			Assert.assertEquals(1, getFileContentByPath(new File(outputFile, "c=2020-01-02"), ".part-0").size());
			Assert.assertEquals(2, getFileContentByPath(new File(outputFile, "c=2020-01-03"), ".part-0").size());
			Assert.assertEquals(1, getFileContentByPath(new File(outputFile, "c=2020-01-04"), ".part-0").size());

			testHarness.processElement(new StreamRecord<>(toRowData("test7", 2, "2020-01-04"), 1L));
			testHarness.processElement(new StreamRecord<>(toRowData("test8", 3, "2020-01-04"), 1L));

			testHarness.snapshot(4L, 1L);

			testHarness.processElement(new StreamRecord<>(toRowData("test7", 2, "2020-01-04"), 1L));
			testHarness.processElement(new StreamRecord<>(toRowData("test8", 3, "2020-01-05"), 1L));

			testHarness.notifyOfCompletedCheckpoint(4);

			LinkedHashMap<String, String> part1 = new LinkedHashMap<>();
			part1.put("c", "2020-01-01");
			LinkedHashMap<String, String> part2 = new LinkedHashMap<>();
			part2.put("c", "2020-01-02");
			LinkedHashMap<String, String> part3 = new LinkedHashMap<>();
			part3.put("c", "2020-01-03");
			Set<LinkedHashMap<String, String>> partitionCreated = new HashSet<>();
			partitionCreated.add(part1);
			partitionCreated.add(part2);
			partitionCreated.add(part3);
			Assert.assertEquals(partitionCreated, msFactory.partitionCreated);

			Assert.assertEquals(1, getFileContentByPath(new File(outputFile, "c=2020-01-01"), SUCCESS_NAME).size());
			Assert.assertEquals(1, getFileContentByPath(new File(outputFile, "c=2020-01-02"), SUCCESS_NAME).size());
			Assert.assertEquals(1, getFileContentByPath(new File(outputFile, "c=2020-01-03"), SUCCESS_NAME).size());

			Assert.assertEquals(2, getFileContentByPath(new File(outputFile, "c=2020-01-01"), "part-0").size());
			Assert.assertEquals(2, getFileContentByPath(new File(outputFile, "c=2020-01-02"), "part-0").size());
			Assert.assertEquals(1, getFileContentByPath(new File(outputFile, "c=2020-01-03"), "part-0").size());
			Assert.assertEquals(2, getFileContentByPath(new File(outputFile, "c=2020-01-04"), "part-0").size());
		}
	}

	@Test
	public void testRecoveryWithoutPatition() throws Exception {
		OperatorSubtaskState snapshot;

		try (OneInputStreamOperatorTestHarness<BaseRow, Object> testHarness = createSink(false)) {

			testHarness.setup();
			testHarness.open();

			testHarness.processElement(new StreamRecord<>(toRowData("a1", 1, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(toRowData("a2", 2, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(toRowData("a3", 3, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(toRowData("a2", 2, "p2"), 1L));

			testHarness.snapshot(1L, 1L);

			testHarness.processElement(new StreamRecord<>(toRowData("test1", 2, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(toRowData("test1", 3, "p1"), 1L));

			snapshot = testHarness.snapshot(2L, 1L);

			Assert.assertEquals(2, getFileContentByPath(outputFile, ".part-0").size());

			testHarness.processElement(new StreamRecord<>(toRowData("test1", 2, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(toRowData("test1", 3, "p1"), 1L));

			testHarness.notifyOfCompletedCheckpoint(2);
			testHarness.snapshot(3L, 1L);

			Assert.assertEquals(2, getFileContentByPath(outputFile, "part-0").size());
			Assert.assertEquals(1, getFileContentByPath(outputFile, ".part-0").size());
		}

		try (OneInputStreamOperatorTestHarness<BaseRow, Object> testHarness = createSink(false)) {

			testHarness.setup();
			testHarness.initializeState(snapshot);
			testHarness.open();

			testHarness.processElement(new StreamRecord<>(toRowData("a4", 1, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(toRowData("a5", 2, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(toRowData("a6", 3, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(toRowData("a4", 2, "p3"), 1L));

			testHarness.snapshot(3L, 1L);

			// NOTE: StreamingFileSink not clean previous temporary file, and create a new file.
			Assert.assertEquals(2, getFileContentByPath(outputFile, ".part-0").size());

			testHarness.processElement(new StreamRecord<>(toRowData("test7", 2, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(toRowData("test8", 3, "p1"), 1L));

			testHarness.snapshot(4L, 1L);

			testHarness.processElement(new StreamRecord<>(toRowData("test7", 2, "p1"), 1L));
			testHarness.processElement(new StreamRecord<>(toRowData("test8", 3, "p1"), 1L));

			testHarness.notifyOfCompletedCheckpoint(4);

			Assert.assertEquals(4, getFileContentByPath(outputFile, "part-0").size());
		}
	}

	private OneInputStreamOperatorTestHarness<BaseRow, Object> createSink(
			boolean partition) throws Exception {
		return createSink(partition, new AtomicReference<>());
	}

	private OneInputStreamOperatorTestHarness<BaseRow, Object> createSink(
			boolean partition,
			AtomicReference<FileSystemStreamingSink> sinkRef) throws Exception {
		String[] columnNames = new String[]{"a", "b", "c"};
		String[] partitionColumns = partition ? new String[]{"c"} : new String[0];

		Path locationPath = new Path(outputFile.getPath());
		msFactory = new FileSystemCommitterTest.TestMetaStoreFactory(locationPath);

		Map<String, String> properties = new HashMap<>();
		properties.put(FileSystemStreamingSink.CONNECTOR_SINK_PARTITION_COMMIT_POLICY, "metastore,success-file");
		properties.put(FileSystemStreamingSink.CONNECTOR_SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME, SUCCESS_NAME);
		properties.put(FileSystemStreamingSink.CONNECTOR_SINK_PARTITION_COMMIT_TRIGGER, "day");

		DataType[] types = new DataType[] {
				DataTypes.STRING(),
				DataTypes.INT(),
				DataTypes.STRING()
		};

		FileSystemStreamingSink sink = new FileSystemStreamingSink.Builder()
				.setMetaStoreFactory(msFactory)
				.setBasePath(locationPath)
				.setPartitionColumns(partitionColumns)
				.setPartitionComputer(new RowDataPartitionComputer(
						"default", columnNames, types, partitionColumns))
				.setBulkFormat(out -> new BulkWriter<BaseRow>() {
					@Override
					public void addElement(BaseRow r) throws IOException {
						Assert.assertEquals(partition ? 2 : 3, r.getArity());
						String v = r.getString(0) + "," + r.getInt(1);
						v = partition ? v : v + "," + r.getString(2);
						out.write(v.getBytes(StandardCharsets.UTF_8));
						out.write('\n');
					}

					@Override
					public void flush() {
					}

					@Override
					public void finish() {
					}
				})
				.setProperties(properties)
				.build();

		sinkRef.set(sink);

		return new OneInputStreamOperatorTestHarness<>(
				new StreamSink<>(sink),
				// test parallelism
				1, 1, 0);
	}

	private static Map<File, String> getFileContentByPath(File directory, String prefix) throws IOException {
		Map<File, String> contents = new HashMap<>(4);

		if (!directory.exists() || !directory.isDirectory()) {
			return contents;
		}

		final Collection<File> filesInBucket = FileUtils.listFiles(directory, null, true);
		for (File file : filesInBucket) {
			if (file.getName().startsWith(prefix)) {
				contents.put(file, FileUtils.readFileToString(file));
			}
		}
		return contents;
	}
}
