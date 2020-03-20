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

package org.apache.flink.formats.orc.writers;

import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.orc.data.Record;
import org.apache.flink.formats.orc.vectorizer.RecordVectorizer;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for the ORC BulkWriter implementation.
 */
public class OrcBulkWriterTest {

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	private final String schema = "struct<_col0:string,_col1:int>";
	private final List<Record> input = Arrays.asList(
		new Record("Shiv", 44), new Record("Jesse", 23), new Record("Walt", 50));

	@Test
	public void testOrcBulkWriter() throws Exception {
		File outDir = TEMPORARY_FOLDER.newFolder();
		OrcBulkWriterFactory<Record> writer = new OrcBulkWriterFactory<>(
			new RecordVectorizer(schema), TypeDescription.fromString(schema));

		StreamingFileSink<Record> sink = StreamingFileSink
			.forBulkFormat(new Path(outDir.toURI()), writer)
			.withBucketCheckInterval(10000)
			.build();

		try (OneInputStreamOperatorTestHarness<Record, Object> testHarness = new OneInputStreamOperatorTestHarness<>(
				new StreamSink<>(sink), 1, 1, 0)) {

			testHarness.setup();
			testHarness.open();

			int time = 0;
			for (final Record record : input) {
				testHarness.processElement(record, ++time);
			}

			testHarness.snapshot(1, ++time);
			testHarness.notifyOfCompletedCheckpoint(1);

			validate(outDir);
		}
	}

	private void validate(File files) throws IOException {
		File[] buckets = files.listFiles();

		assertNotNull(buckets);
		assertEquals(1, buckets.length);

		final File[] partFiles = buckets[0].listFiles();

		assertNotNull(partFiles);
		assertEquals(1, partFiles.length);

		for (File partFile : partFiles) {
			assertTrue(partFile.length() > 0);

			OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(new Configuration());
			Reader reader = OrcFile.createReader(new org.apache.hadoop.fs.Path(partFile.toURI()), readerOptions);
			RecordReader recordReader = reader.rows();
			VectorizedRowBatch batch = reader.getSchema().createRowBatch();

			assertSame(reader.getCompressionKind(), CompressionKind.ZLIB);
			assertEquals(3, reader.getNumberOfRows());
			assertEquals(2, batch.numCols);

			List<Record> results = new ArrayList<>();

			while (recordReader.nextBatch(batch)) {
				BytesColumnVector stringVector = (BytesColumnVector)  batch.cols[0];
				LongColumnVector intVector = (LongColumnVector) batch.cols[1];
				for (int r = 0; r < batch.size; r++) {
					String name = new String(stringVector.vector[r], stringVector.start[r], stringVector.length[r]);
					int age = (int) intVector.vector[r];

					results.add(new Record(name, age));
				}
				recordReader.close();
			}

			assertEquals(3, results.size());
			assertEquals(results, input);
		}
	}
}
