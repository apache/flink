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

package org.apache.flink.connector.file.src;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.event.NoMoreSplitsEvent;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.connector.file.src.testutils.TestingFileSystem;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.junit.After;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * This test simulates readers that just produce byte arrays very fast. The test
 * is meant to check that this does not break the system in terms of object allocations, etc.
 */
public class FileSourceHeavyThroughputTest {

	/** Testing file system reference, to be cleaned up in an @After method. That way it also gets
	 * cleaned up on a test failure, without needing finally clauses in every test. */
	private TestingFileSystem testFs;

	@After
	public void unregisterTestFs() throws Exception {
		if (testFs != null) {
			testFs.unregister();
		}
	}

	// ------------------------------------------------------------------------

	@Test
	public void testHeavyThroughput() throws Exception {
		final Path path = new Path("testfs:///testpath");
		final long fileSize = 20L << 30; // 20 GB
		final FileSourceSplit split = new FileSourceSplit("testsplitId", path, 0, fileSize);

		testFs = TestingFileSystem.createForFileStatus(
				path.toUri().getScheme(),
				TestingFileSystem.TestFileStatus.forFileWithStream(path, fileSize, new GeneratingInputStream(fileSize)));
		testFs.register();

		final FileSource<byte[]> source = FileSource.forRecordStreamFormat(new ArrayReaderFormat(), path).build();
		final SourceReader<byte[], FileSourceSplit> reader = source.createReader(new NoOpReaderContext());
		reader.addSplits(Collections.singletonList(split));
		reader.handleSourceEvents(new NoMoreSplitsEvent());

		final ReaderOutput<byte[]> out = new NoOpReaderOutput<>();

		InputStatus status;
		while ((status = reader.pollNext(out)) != InputStatus.END_OF_INPUT) {
			// if nothing is available currently, wait for more
			if (status == InputStatus.NOTHING_AVAILABLE) {
				reader.isAvailable().get();
			}
		}
	}

	// ------------------------------------------------------------------------
	//  test mocks
	// ------------------------------------------------------------------------

	private static final class ArrayReader implements StreamFormat.Reader<byte[]> {

		private static final int ARRAY_SIZE = 1 << 20; // 1 MiByte

		private final FSDataInputStream in;

		ArrayReader(FSDataInputStream in) {
			this.in = in;
		}

		@Nullable
		@Override
		public byte[] read() throws IOException {
			final byte[] array = new byte[ARRAY_SIZE];
			final int read = in.read(array);
			if (read == array.length) {
				return array;
			} else if (read == -1) {
				return null;
			} else {
				return Arrays.copyOf(array, read);
			}
		}

		@Override
		public void close() throws IOException {
			in.close();
		}
	}

	private static final class ArrayReaderFormat extends SimpleStreamFormat<byte[]> {
		private static final long serialVersionUID = 1L;

		@Override
		public Reader<byte[]> createReader(Configuration config, FSDataInputStream stream) throws IOException {
			return new ArrayReader(stream);
		}

		@Override
		public TypeInformation<byte[]> getProducedType() {
			return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
		}
	}

	private static final class GeneratingInputStream extends FSDataInputStream {

		private final long length;
		private long pos;

		GeneratingInputStream(long length) {
			this.length = length;
		}

		@Override
		public void seek(long desired) throws IOException {
			checkArgument(desired >= 0 && desired <= length);
			pos = desired;
		}

		@Override
		public long getPos() throws IOException {
			return pos;
		}

		@Override
		public int read() throws IOException {
			if (pos < length) {
				pos++;
				return 0;
			} else {
				return -1;
			}
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			if (pos < length) {
				final int remaining = (int) Math.min(len, length - pos);
				pos += remaining;
				return remaining;
			} else {
				return -1;
			}
		}
	}

	private static final class NoOpReaderContext implements SourceReaderContext {

		@Override
		public MetricGroup metricGroup() {
			return new UnregisteredMetricsGroup();
		}

		@Override
		public Configuration getConfiguration() {
			return new Configuration();
		}

		@Override
		public String getLocalHostName() {
			return "localhost";
		}

		@Override
		public void sendSourceEventToCoordinator(SourceEvent sourceEvent) {}
	}

	private static final class NoOpReaderOutput<E> implements ReaderOutput<E> {

		@Override
		public void collect(E record) {}

		@Override
		public void collect(E record, long timestamp) {}

		@Override
		public void emitWatermark(Watermark watermark) {}

		@Override
		public void markIdle() {}

		@Override
		public SourceOutput<E> createOutputForSplit(String splitId) {
			return this;
		}

		@Override
		public void releaseOutputForSplit(String splitId) {}
	}
}
