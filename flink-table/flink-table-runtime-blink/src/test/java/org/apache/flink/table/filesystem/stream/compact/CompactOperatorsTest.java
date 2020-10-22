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

package org.apache.flink.table.filesystem.stream.compact;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.filesystem.stream.PartitionCommitInfo;
import org.apache.flink.table.filesystem.stream.compact.CompactMessages.CompactionUnit;
import org.apache.flink.table.filesystem.stream.compact.CompactMessages.CoordinatorInput;
import org.apache.flink.table.filesystem.stream.compact.CompactMessages.CoordinatorOutput;
import org.apache.flink.table.filesystem.stream.compact.CompactMessages.EndCompaction;
import org.apache.flink.table.filesystem.stream.compact.CompactMessages.EndInputFile;
import org.apache.flink.table.filesystem.stream.compact.CompactMessages.InputFile;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Test for {@link CompactCoordinator} and {@link CompactOperator}.
 */
public class CompactOperatorsTest {

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	public static final int TARGET_SIZE = 9;

	private Path folder;

	@Before
	public void before() throws IOException {
		folder = new Path(TEMP_FOLDER.newFolder().getPath());
	}

	@Test
	public void testCoordinatorCrossCheckpoints() throws Exception {
		AtomicReference<OperatorSubtaskState> state = new AtomicReference<>();
		runCoordinator(harness -> {
			harness.setup();
			harness.open();

			harness.processElement(new InputFile("p0", newFile("f0", 3)), 0);
			harness.processElement(new InputFile("p0", newFile("f1", 2)), 0);

			harness.processElement(new InputFile("p1", newFile("f2", 2)), 0);

			harness.processElement(new InputFile("p0", newFile("f3", 5)), 0);
			harness.processElement(new InputFile("p0", newFile("f4", 1)), 0);

			harness.processElement(new InputFile("p1", newFile("f5", 5)), 0);
			harness.processElement(new InputFile("p1", newFile("f6", 4)), 0);

			state.set(harness.snapshot(1, 0));
		});

		runCoordinator(harness -> {
			harness.setup();
			harness.initializeState(state.get());
			harness.open();

			harness.processElement(new InputFile("p0", newFile("f7", 3)), 0);
			harness.processElement(new InputFile("p0", newFile("f8", 2)), 0);

			state.set(harness.snapshot(2, 0));
		});

		runCoordinator(harness -> {
			harness.setup();
			harness.initializeState(state.get());
			harness.open();

			harness.processElement(new EndInputFile(2, 0, 1), 0);

			List<CoordinatorOutput> outputs = harness.extractOutputValues();

			Assert.assertEquals(7, outputs.size());

			assertUnit(outputs.get(0), 0, "p0", Arrays.asList("f0", "f1", "f4"));
			assertUnit(outputs.get(1), 1, "p0", Collections.singletonList("f3"));
			assertUnit(outputs.get(2), 2, "p1", Arrays.asList("f2", "f5"));
			assertUnit(outputs.get(3), 3, "p1", Collections.singletonList("f6"));

			assertEndCompaction(outputs.get(4), 1);

			assertUnit(outputs.get(5), 0, "p0", Arrays.asList("f7", "f8"));

			assertEndCompaction(outputs.get(6), 2);
		});
	}

	@Test
	public void testCompactOperator() throws Exception {
		AtomicReference<OperatorSubtaskState> state = new AtomicReference<>();
		Path f0 = newFile(".uncompacted-f0", 3);
		Path f1 = newFile(".uncompacted-f1", 2);
		Path f2 = newFile(".uncompacted-f2", 2);
		Path f3 = newFile(".uncompacted-f3", 5);
		Path f4 = newFile(".uncompacted-f4", 1);
		Path f5 = newFile(".uncompacted-f5", 5);
		Path f6 = newFile(".uncompacted-f6", 4);
		FileSystem fs = f0.getFileSystem();
		runCompact(harness -> {
			harness.setup();
			harness.open();

			harness.processElement(new CompactionUnit(0, "p0", Arrays.asList(f0, f1, f4)), 0);
			harness.processElement(new CompactionUnit(1, "p0", Collections.singletonList(f3)), 0);
			harness.processElement(new CompactionUnit(2, "p1", Arrays.asList(f2, f5)), 0);
			harness.processElement(new CompactionUnit(1, "p0", Collections.singletonList(f6)), 0);

			harness.processElement(new EndCompaction(1), 0);

			state.set(harness.snapshot(2, 0));

			// check output commit info
			List<PartitionCommitInfo> outputs = harness.extractOutputValues();
			Assert.assertEquals(1, outputs.size());
			Assert.assertEquals(1, outputs.get(0).getCheckpointId());
			Assert.assertEquals(Arrays.asList("p0", "p1"), outputs.get(0).getPartitions());

			// check all compacted file generated
			Assert.assertTrue(fs.exists(new Path(folder, "compacted-f0")));
			Assert.assertTrue(fs.exists(new Path(folder, "compacted-f2")));
			Assert.assertTrue(fs.exists(new Path(folder, "compacted-f3")));
			Assert.assertTrue(fs.exists(new Path(folder, "compacted-f6")));

			// check one compacted file
			byte[] bytes = FileUtils.readAllBytes(new File(folder.getPath(), "compacted-f0").toPath());
			Arrays.sort(bytes);
			Assert.assertArrayEquals(new byte[] {0, 0, 0, 1, 1, 2}, bytes);
		});

		runCompact(harness -> {
			harness.setup();
			harness.initializeState(state.get());
			harness.open();

			harness.notifyOfCompletedCheckpoint(2);

			// check all temp files have been deleted
			Assert.assertFalse(fs.exists(f0));
			Assert.assertFalse(fs.exists(f1));
			Assert.assertFalse(fs.exists(f2));
			Assert.assertFalse(fs.exists(f3));
			Assert.assertFalse(fs.exists(f4));
			Assert.assertFalse(fs.exists(f5));
			Assert.assertFalse(fs.exists(f6));
		});
	}

	private void runCoordinator(ThrowingConsumer<
			OneInputStreamOperatorTestHarness<CoordinatorInput, CoordinatorOutput>, Exception> consumer) throws Exception {
		CompactCoordinator coordinator = new CompactCoordinator(
				() -> folder.getFileSystem(), TARGET_SIZE);
		try (OneInputStreamOperatorTestHarness<CoordinatorInput, CoordinatorOutput> harness =
				     new OneInputStreamOperatorTestHarness<>(coordinator)) {
			consumer.accept(harness);
		}
	}

	private void runCompact(ThrowingConsumer<
			OneInputStreamOperatorTestHarness<CoordinatorOutput, PartitionCommitInfo>, Exception> consumer) throws Exception {
		CompactOperator<Byte> operator = new CompactOperator<>(
				() -> folder.getFileSystem(),
				CompactBulkReader.factory(TestByteFormat.bulkFormat()),
				(config, fileSystem, path) -> {
					Path tempPath = new Path(path.getParent(), "." + path.getName());
					FSDataOutputStream out = fileSystem.create(tempPath, FileSystem.WriteMode.OVERWRITE);
					return new CompactWriter<Byte>() {
						@Override
						public void write(Byte record) throws IOException {
							out.write(record);
						}

						@Override
						public void commit() throws IOException {
							out.close();
							fileSystem.rename(tempPath, path);
						}
					};
				}

		);
		try (OneInputStreamOperatorTestHarness<CoordinatorOutput, PartitionCommitInfo> harness =
				     new OneInputStreamOperatorTestHarness<>(operator)) {
			consumer.accept(harness);
		}
	}

	private Path newFile(String name, int len) throws IOException {
		Path path = new Path(folder, name);
		File file = new File(path.getPath());
		file.delete();
		file.createNewFile();

		try (FileOutputStream out = new FileOutputStream(file)) {
			for (int i = 0; i < len; i++) {
				out.write(i);
			}
		}
		return path;
	}

	private void assertUnit(CoordinatorOutput output, int unitId, String partition, List<String> fileNames) {
		Assert.assertTrue(output instanceof CompactionUnit);
		CompactionUnit unit = (CompactionUnit) output;

		Assert.assertEquals(unitId, unit.getUnitId());
		Assert.assertEquals(partition, unit.getPartition());
		Assert.assertEquals(
				fileNames,
				unit.getPaths().stream().map(Path::getName).collect(Collectors.toList()));
	}

	private void assertEndCompaction(CoordinatorOutput output, long checkpointId) {
		Assert.assertTrue(output instanceof EndCompaction);
		EndCompaction end = (EndCompaction) output;

		Assert.assertEquals(checkpointId, end.getCheckpointId());
	}
}
