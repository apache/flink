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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.orc.data.Record;
import org.apache.flink.formats.orc.vectorizer.RecordVectorizer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for writing data in ORC bulk format using StreamingFileSink.
 */
public class OrcBulkWriterITCase extends TestLogger {

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	private final String schema = "struct<_col0:string,_col1:int>";
	private final List<Record> testData = Arrays.asList(
		new Record("Sourav", 41), new Record("Saul", 35), new Record("Kim", 31));

	@Test
	public void testOrcBulkWriter() throws Exception {
		final File outDir = TEMPORARY_FOLDER.newFolder();
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final OrcBulkWriterFactory<Record> factory = new OrcBulkWriterFactory<>(
			new RecordVectorizer(schema), TypeDescription.fromString(schema));

		env.setParallelism(1);
		env.enableCheckpointing(100);

		DataStream<Record> stream = env.addSource(new FiniteTestSource<>(testData), TypeInformation.of(Record.class));
		stream.map(str -> str)
			.addSink(StreamingFileSink
				.forBulkFormat(new Path(outDir.toURI()), factory)
				.build());

		env.execute();
		validate(outDir);
	}

	private void validate(File files) throws IOException {
		File[] buckets = files.listFiles();
		assertNotNull(buckets);
		assertEquals(1, buckets.length);

		final File[] partFiles = buckets[0].listFiles();
		assertNotNull(partFiles);
		assertEquals(2, partFiles.length);

		for (File partFile : partFiles) {
			assertTrue(partFile.length() > 0);

			OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(new Configuration());

			Reader reader = OrcFile.createReader(new org.apache.hadoop.fs.Path(partFile.toURI()), readerOptions);

			assertSame(reader.getCompressionKind(), CompressionKind.ZLIB);
			assertEquals(3, reader.getNumberOfRows());

		}

	}
}
