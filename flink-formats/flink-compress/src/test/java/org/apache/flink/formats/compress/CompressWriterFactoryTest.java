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

package org.apache.flink.formats.compress;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.compress.extractor.DefaultExtractor;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link CompressWriterFactory}.
 */
public class CompressWriterFactoryTest extends TestLogger {

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	@Test
	public void testBzip2CompressByName() throws Exception {
		testCompressByName("Bzip2");
	}

	@Test
	public void testBzip2CompressCodec() throws Exception {
		BZip2Codec codec = new BZip2Codec();
		codec.setConf(new Configuration());
		testCompressCodec(codec);
	}

	@Test
	public void testGzipCompressByName() throws Exception {
		testCompressByName("Gzip");
	}

	@Test
	public void testGzipCompressCodec() throws Exception {
		GzipCodec codec = new GzipCodec();
		codec.setConf(new Configuration());
		testCompressCodec(codec);
	}

	@Test
	public void testDeflateCompressByName() throws Exception {
		DeflateCodec codec = new DeflateCodec();
		codec.setConf(new Configuration());
		testCompressCodec(codec);
	}

	@Test
	public void testDefaultCompressByName() throws Exception {
		DefaultCodec codec = new DefaultCodec();
		codec.setConf(new Configuration());
		testCompressCodec(codec);
	}

	private void testCompressByName(String codec) throws Exception {
		CompressWriterFactory<String> writer = CompressWriters.forExtractor(new DefaultExtractor<String>()).withHadoopCompression(codec);
		List<String> lines = Arrays.asList("line1", "line2", "line3");

		File directory = prepareCompressedFile(writer, lines);

		validateResults(directory, lines, new CompressionCodecFactory(new Configuration()).getCodecByName(codec));
	}

	private void testCompressCodec(CompressionCodec codec) throws Exception {

		CompressWriterFactory<String> writer = CompressWriters.forExtractor(new DefaultExtractor<String>()).withHadoopCompression(codec);
		List<String> lines = Arrays.asList("line1", "line2", "line3");

		File directory = prepareCompressedFile(writer, lines);

		validateResults(directory, lines, codec);
	}

	private File prepareCompressedFile(CompressWriterFactory<String> writer, List<String> lines) throws Exception {
		final File outDir = TEMPORARY_FOLDER.newFolder();

		final BucketAssigner<String, String> assigner = new BucketAssigner<String, String> () {
			@Override
			public String getBucketId(String element, BucketAssigner.Context context) {
				return "bucket";
			}

			@Override
			public SimpleVersionedSerializer<String> getSerializer() {
				return SimpleVersionedStringSerializer.INSTANCE;
			}
		};

		StreamingFileSink<String> sink = StreamingFileSink
			.forBulkFormat(new Path(outDir.toURI()), writer)
			.withBucketAssigner(assigner)
			.build();

		try (
			OneInputStreamOperatorTestHarness<String, Object> testHarness = new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink), 1, 1, 0)
		) {
			testHarness.setup();
			testHarness.open();

			int time = 0;
			for (String line: lines) {
				testHarness.processElement(new StreamRecord<>(line, ++time));
			}

			testHarness.snapshot(1, ++time);
			testHarness.notifyOfCompletedCheckpoint(1);
		}

		return outDir;
	}

	private void validateResults(File folder, List<String> expected, CompressionCodec codec) throws Exception {
		File[] buckets = folder.listFiles();
		assertNotNull(buckets);
		assertEquals(1, buckets.length);

		final File[] partFiles = buckets[0].listFiles();
		assertNotNull(partFiles);
		assertEquals(1, partFiles.length);

		for (File partFile : partFiles) {
			assertTrue(partFile.length() > 0);
			final List<String> fileContent = readFile(partFile, codec);
			assertEquals(expected, fileContent);
		}
	}

	private List<String> readFile(File file, CompressionCodec codec) throws Exception {
		try (FileInputStream inputStream = new FileInputStream(file)) {
			try (InputStreamReader readerStream = new InputStreamReader((codec == null) ? inputStream : codec.createInputStream(inputStream))) {
				try (BufferedReader reader = new BufferedReader(readerStream)) {
					return reader.lines().collect(Collectors.toList());
				}
			}
		}
	}
}
