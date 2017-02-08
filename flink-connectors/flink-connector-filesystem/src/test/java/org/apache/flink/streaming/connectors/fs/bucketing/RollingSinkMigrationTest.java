/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.fs.bucketing;

import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.fs.RollingSink;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;

@Deprecated
public class RollingSinkMigrationTest {

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	private static final String PART_PREFIX = "part";
	private static final String PENDING_SUFFIX = ".pending";
	private static final String IN_PROGRESS_SUFFIX = ".in-progress";
	private static final String VALID_LENGTH_SUFFIX = ".valid";

	@Test
	public void testMigration() throws Exception {

		/*
		* Code ran to get the snapshot:
		*
		* final File outDir = tempFolder.newFolder();

		RollingSink<String> sink = new RollingSink<String>(outDir.getAbsolutePath())
			.setWriter(new StringWriter<String>())
			.setBatchSize(5)
			.setPartPrefix(PART_PREFIX)
			.setInProgressPrefix("")
			.setPendingPrefix("")
			.setValidLengthPrefix("")
			.setInProgressSuffix(IN_PROGRESS_SUFFIX)
			.setPendingSuffix(PENDING_SUFFIX)
			.setValidLengthSuffix(VALID_LENGTH_SUFFIX);

		OneInputStreamOperatorTestHarness<String, Object> testHarness1 =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

		testHarness1.setup();
		testHarness1.open();

		testHarness1.processElement(new StreamRecord<>("test1", 0L));
		testHarness1.processElement(new StreamRecord<>("test2", 0L));

		checkFs(outDir, 1, 1, 0, 0);

		testHarness1.processElement(new StreamRecord<>("test3", 0L));
		testHarness1.processElement(new StreamRecord<>("test4", 0L));
		testHarness1.processElement(new StreamRecord<>("test5", 0L));

		checkFs(outDir, 1, 4, 0, 0);

		StreamTaskState taskState = testHarness1.snapshot(0, 0);
		testHarness1.snaphotToFile(taskState, "src/test/resources/rolling-sink-migration-test-flink1.1-snapshot");
		testHarness1.close();
		* */

		final File outDir = tempFolder.newFolder();

		RollingSink<String> sink = new ValidatingRollingSink<String>(outDir.getAbsolutePath())
			.setWriter(new StringWriter<String>())
			.setBatchSize(5)
			.setPartPrefix(PART_PREFIX)
			.setInProgressPrefix("")
			.setPendingPrefix("")
			.setValidLengthPrefix("")
			.setInProgressSuffix(IN_PROGRESS_SUFFIX)
			.setPendingSuffix(PENDING_SUFFIX)
			.setValidLengthSuffix(VALID_LENGTH_SUFFIX);

		OneInputStreamOperatorTestHarness<String, Object> testHarness1 = new OneInputStreamOperatorTestHarness<>(
			new StreamSink<>(sink), 10, 1, 0);
		testHarness1.setup();
		testHarness1.initializeStateFromLegacyCheckpoint(getResourceFilename("rolling-sink-migration-test-flink1.1-snapshot"));
		testHarness1.open();

		testHarness1.processElement(new StreamRecord<>("test1", 0L));
		testHarness1.processElement(new StreamRecord<>("test2", 0L));

		checkFs(outDir, 1, 1, 0, 0);

		testHarness1.close();
	}

	private void checkFs(File outDir, int inprogress, int pending, int completed, int valid) throws IOException {
		int inProg = 0;
		int pend = 0;
		int compl = 0;
		int val = 0;

		for (File file: FileUtils.listFiles(outDir, null, true)) {
			if (file.getAbsolutePath().endsWith("crc")) {
				continue;
			}
			String path = file.getPath();
			if (path.endsWith(IN_PROGRESS_SUFFIX)) {
				inProg++;
			} else if (path.endsWith(PENDING_SUFFIX)) {
				pend++;
			} else if (path.endsWith(VALID_LENGTH_SUFFIX)) {
				val++;
			} else if (path.contains(PART_PREFIX)) {
				compl++;
			}
		}

		Assert.assertEquals(inprogress, inProg);
		Assert.assertEquals(pending, pend);
		Assert.assertEquals(completed, compl);
		Assert.assertEquals(valid, val);
	}

	private static String getResourceFilename(String filename) {
		ClassLoader cl = RollingSinkMigrationTest.class.getClassLoader();
		URL resource = cl.getResource(filename);
		return resource.getFile();
	}

	static class ValidatingRollingSink<T> extends RollingSink<T> {

		private static final long serialVersionUID = -4263974081712009141L;

		ValidatingRollingSink(String basePath) {
			super(basePath);
		}

		@Override
		public void restoreState(BucketState state) throws Exception {

			/**
			 * this validates that we read the state that was checkpointed by the previous version. We expect it to be:
			 * In-progress=/var/folders/z5/fxvg1j6s6mn94nsf8b1yc8s80000gn/T/junit2927527303216950257/junit5645682027227039270/2017-01-09--18/part-0-4
			 * 					validLength=6
			 * pendingForNextCheckpoint=[]
			 * pendingForPrevCheckpoints={0=[	/var/folders/z5/fxvg1j6s6mn94nsf8b1yc8s80000gn/T/junit2927527303216950257/junit5645682027227039270/2017-01-09--18/part-0-0,
			 * 									/var/folders/z5/fxvg1j6s6mn94nsf8b1yc8s80000gn/T/junit2927527303216950257/junit5645682027227039270/2017-01-09--18/part-0-1,
			 * 									/var/folders/z5/fxvg1j6s6mn94nsf8b1yc8s80000gn/T/junit2927527303216950257/junit5645682027227039270/2017-01-09--18/part-0-2,
			 * 									/var/folders/z5/fxvg1j6s6mn94nsf8b1yc8s80000gn/T/junit2927527303216950257/junit5645682027227039270/2017-01-09--18/part-0-3]}
			 * */

			String current = state.currentFile;
			long validLength = state.currentFileValidLength;

			Assert.assertEquals("/var/folders/z5/fxvg1j6s6mn94nsf8b1yc8s80000gn/T/junit2927527303216950257/junit5645682027227039270/2017-01-09--18/part-0-4", current);
			Assert.assertEquals(6, validLength);

			List<String> pendingFiles = state.pendingFiles;
			Assert.assertTrue(pendingFiles.isEmpty());

			final Map<Long, List<String>> pendingFilesPerCheckpoint = state.pendingFilesPerCheckpoint;
			Assert.assertEquals(1, pendingFilesPerCheckpoint.size());

			for (Map.Entry<Long, List<String>> entry: pendingFilesPerCheckpoint.entrySet()) {
				long checkpoint = entry.getKey();
				List<String> files = entry.getValue();

				Assert.assertEquals(0L, checkpoint);
				Assert.assertEquals(4, files.size());

				for (int i = 0; i < 4; i++) {
					Assert.assertEquals(
						"/var/folders/z5/fxvg1j6s6mn94nsf8b1yc8s80000gn/T/junit2927527303216950257/junit5645682027227039270/2017-01-09--18/part-0-" + i,
						files.get(i));
				}
			}
			super.restoreState(state);
		}
	}
}
