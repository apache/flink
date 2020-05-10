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

package org.apache.flink.formats.hadoop.bulk;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.HadoopPathBasedBulkFormatBuilder;
import org.apache.flink.streaming.api.functions.sink.filesystem.TestStreamingFileSinkFactory;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.formats.hadoop.bulk.HadoopPathBasedPartFileWriter.HadoopPathBasedPendingFileRecoverable;
import static org.apache.flink.formats.hadoop.bulk.HadoopPathBasedPartFileWriter.HadoopPathBasedPendingFileRecoverableSerializer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Base class for testing writing data to the hadoop file system with different configurations.
 */
public class HadoopPathBasedPartFileWriterTest extends AbstractTestBase {
	@Rule
	public final Timeout timeoutPerTest = Timeout.seconds(2000);

	@Test
	public void testPendingFileRecoverableSerializer() throws IOException {
		HadoopPathBasedPendingFileRecoverable recoverable = new HadoopPathBasedPendingFileRecoverable(
			new Path("hdfs://fake/path"));
		HadoopPathBasedPendingFileRecoverableSerializer serializer =
			new HadoopPathBasedPendingFileRecoverableSerializer();

		byte[] serializedBytes = serializer.serialize(recoverable);
		HadoopPathBasedPendingFileRecoverable deSerialized = serializer.deserialize(
			serializer.getVersion(),
			serializedBytes);

		assertEquals(recoverable.getPath(), deSerialized.getPath());
	}

	@Test
	public void testWriteFile() throws Exception {
		File file = TEMPORARY_FOLDER.newFolder();
		Path basePath = new Path(file.toURI());

		List<String> data = Arrays.asList(
			"first line",
			"second line",
			"third line");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.enableCheckpointing(100);

		DataStream<String> stream = env.addSource(
			new FiniteTestSource<>(data), TypeInformation.of(String.class));
		Configuration configuration = new Configuration();

		HadoopPathBasedBulkFormatBuilder<String, String, ?> builder =
			new HadoopPathBasedBulkFormatBuilder<>(
				basePath,
				new TestHadoopPathBasedBulkWriterFactory(),
				configuration,
				new DateTimeBucketAssigner<>());
		TestStreamingFileSinkFactory<String> streamingFileSinkFactory = new TestStreamingFileSinkFactory<>();
		stream.addSink(streamingFileSinkFactory.createSink(builder, 1000));

		env.execute();
		validateResult(data, configuration, basePath);
	}

	// ------------------------------------------------------------------------

	private void validateResult(List<String> expected, Configuration config, Path basePath) throws IOException {
		FileSystem fileSystem = FileSystem.get(basePath.toUri(), config);
		FileStatus[] buckets = fileSystem.listStatus(basePath);
		assertNotNull(buckets);
		assertEquals(1, buckets.length);

		FileStatus[] partFiles = fileSystem.listStatus(buckets[0].getPath());
		assertNotNull(partFiles);
		assertEquals(2, partFiles.length);

		for (FileStatus partFile : partFiles) {
			assertTrue(partFile.getLen() > 0);

			List<String> fileContent = readHadoopPath(fileSystem, partFile.getPath());
			assertEquals(expected, fileContent);
		}
	}

	private List<String> readHadoopPath(FileSystem fileSystem, Path partFile) throws IOException {
		try (FSDataInputStream dataInputStream = fileSystem.open(partFile)) {
			List<String> lines = new ArrayList<>();
			BufferedReader reader = new BufferedReader(new InputStreamReader(dataInputStream));
			String line = null;
			while ((line = reader.readLine()) != null) {
				lines.add(line);
			}

			return lines;
		}
	}

	private static class TestHadoopPathBasedBulkWriterFactory implements HadoopPathBasedBulkWriter.Factory<String> {

		@Override
		public HadoopPathBasedBulkWriter<String> create(Path targetFilePath, Path inProgressFilePath) {
			try {
				FileSystem fileSystem = FileSystem.get(inProgressFilePath.toUri(), new Configuration());
				FSDataOutputStream output = fileSystem.create(inProgressFilePath);
				return new FSDataOutputStreamBulkWriterHadoop(output);
			} catch (IOException e) {
				ExceptionUtils.rethrow(e);
			}

			return null;
		}
	}

	private static class FSDataOutputStreamBulkWriterHadoop implements HadoopPathBasedBulkWriter<String> {
		private final FSDataOutputStream outputStream;

		public FSDataOutputStreamBulkWriterHadoop(FSDataOutputStream outputStream) {
			this.outputStream = outputStream;
		}

		@Override
		public long getSize() throws IOException {
			return outputStream.getPos();
		}

		@Override
		public void dispose() {
			IOUtils.closeQuietly(outputStream);
		}

		@Override
		public void addElement(String element) throws IOException {
			outputStream.writeBytes(element + "\n");
		}

		@Override
		public void flush() throws IOException {
			outputStream.flush();
		}

		@Override
		public void finish() throws IOException {
			outputStream.flush();
			outputStream.close();
		}
	}
}
