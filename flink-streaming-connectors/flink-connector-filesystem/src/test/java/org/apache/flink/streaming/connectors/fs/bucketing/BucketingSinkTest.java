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
package org.apache.flink.streaming.connectors.fs.bucketing;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.apache.flink.streaming.runtime.tasks.TestTimeServiceProvider;
import org.apache.flink.streaming.runtime.tasks.TimeServiceProvider;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.hadoop.fs.Path;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class BucketingSinkTest {
	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	private OneInputStreamOperatorTestHarness<String, Object> createTestSink(File dataDir, TimeServiceProvider clock) {
		BucketingSink<String> sink = new BucketingSink<String>(dataDir.getAbsolutePath())
				.setBucketer(new Bucketer<String>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Path getBucketPath(Clock clock, Path basePath, String element) {
						return new Path(basePath, element);
					}
				})
				.setWriter(new StringWriter<String>())
				.setPartPrefix("part")
				.setPendingPrefix("")
				.setInactiveBucketCheckInterval(10)
				.setInactiveBucketThreshold(1000)
				.setPendingSuffix(".pending");

		OneInputStreamOperatorTestHarness<String, Object> testHarness =
				new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink), new ExecutionConfig(), clock);


		return testHarness;
	}

	@Test
	public void testCheckpointWithoutNotify() throws Exception {
		File dataDir = tempFolder.newFolder();

		TestTimeServiceProvider clock = new TestTimeServiceProvider();
		clock.setCurrentTime(0L);

		clock.setCurrentTime(0);

		OneInputStreamOperatorTestHarness<String, Object> testHarness = createTestSink(dataDir, clock);

		testHarness.setup();
		testHarness.open();

		testHarness.processElement(new StreamRecord<>("Hello"));
		testHarness.processElement(new StreamRecord<>("Hello"));
		testHarness.processElement(new StreamRecord<>("Hello"));

		clock.setCurrentTime(10000);

		// snapshot but don't call notify to simulate a notify that never
		// arrives, the sink should move pending files in restore() in that case
		StreamTaskState snapshot1 = testHarness.snapshot(0, 0);

		testHarness = createTestSink(dataDir, clock);
		testHarness.setup();
		testHarness.restore(snapshot1, 1);
		testHarness.open();

		testHarness.processElement(new StreamRecord<>("Hello"));

		testHarness.close();

		for (File file: FileUtils.listFiles(dataDir, null, true)) {
			if (file.getAbsolutePath().endsWith("crc")) {
				continue;
			}
			System.out.println("FILE: " + file);
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));

			while (br.ready()) {
				System.out.println(br.readLine());
			}

			br.close();
		}
	}
}
