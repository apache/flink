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

package org.apache.flink.connector.file.src.reader;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.src.util.CheckpointablePosition;
import org.apache.flink.connector.file.src.util.IteratorResultIterator;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.MathUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Adapter to turn a {@link StreamFormat} into a {@link BulkFormat}.
 */
public final class StreamFormatAdapter<T> implements BulkFormat<T> {

	/**
	 * The config option to define how many bytes to be read by the I/O thread in
	 * one fetch operation.
	 */
	public static final ConfigOption<MemorySize> FETCH_IO_SIZE = ConfigOptions
		.key("source.file.stream.io-fetch-size")
		.memoryType()
		.defaultValue(MemorySize.ofMebiBytes(1L))
		.withDescription("The approximate of bytes per fetch that is passed from the I/O thread to file reader.");

	// ------------------------------------------------------------------------

	private static final long serialVersionUID = 1L;

	private final StreamFormat<T> streamFormat;

	public StreamFormatAdapter(StreamFormat<T> streamFormat) {
		this.streamFormat = checkNotNull(streamFormat);
	}

	@Override
	public Reader<T> createReader(Configuration config, Path filePath, long offset, long length, long recordsToSkip) throws IOException {
		final FileSystem fs = filePath.getFileSystem();
		final int fetchSize = MathUtils.checkedDownCast(config.get(FETCH_IO_SIZE).getBytes());

		checkArgument(offset == 0L, "Generic stream formats can only read files from the beginning.");
		checkArgument(length == fs.getFileStatus(filePath).getLen(), "Generic stream formats can only read full files");
		checkArgument(fetchSize > 0, "the fetch size must be > 0");

		final FSDataInputStream inStream = fs.open(filePath);
		try {
			final TrackingFsDataInputStream trackingStream = new TrackingFsDataInputStream(inStream, fetchSize);
			final StreamFormat.Reader<T> reader = streamFormat.createReader(trackingStream, config);

			for (long remaining = recordsToSkip; remaining > 0; remaining--) {
				reader.read();
			}

			return new Reader<>(reader, trackingStream);
		}
		catch (Throwable t) {
			IOUtils.closeQuietly(inStream);
			ExceptionUtils.rethrowIOException(t);
			return null; // silence the compiler
		}
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return streamFormat.getProducedType();
	}

	// ----------------------------------------------------------------------------------

	/**
	 * The reader adapter, from {@link StreamFormat.Reader} to {@link BulkFormat.Reader}.
	 */
	public static final class Reader<T> implements BulkFormat.Reader<T> {

		private final StreamFormat.Reader<T> reader;

		private final TrackingFsDataInputStream stream;

		private long lastOffset;
		private long lastRecordsAfterOffset;

		private Reader(
				final StreamFormat.Reader<T> reader,
				final TrackingFsDataInputStream stream) {

			this.reader = checkNotNull(reader);
			this.stream = checkNotNull(stream);
		}

		@Nullable
		@Override
		public RecordIterator<T> readBatch() throws IOException {
			updateCheckpointablePosition();
			stream.newBatch();

			final ArrayList<T> result = new ArrayList<>();
			T next;
			while (stream.hasRemainingInBatch() && (next = reader.read()) != null) {
				result.add(next);
			}

			if (result.isEmpty()) {
				return null;
			}

			final RecordIterator<T> iter = new IteratorResultIterator<>(result.iterator(), lastOffset, lastRecordsAfterOffset);
			lastRecordsAfterOffset += result.size();
			return iter;
		}

		@Override
		public void close() throws IOException {
			try {
				reader.close();
			} finally {
				// this is just in case, to guard against resource leaks
				IOUtils.closeQuietly(stream);
			}
		}

		private void updateCheckpointablePosition() {
			final CheckpointablePosition position = reader.getCheckpointablePosition();
			if (position != null) {
				this.lastOffset = position.getOffset();
				this.lastRecordsAfterOffset = position.getRecordsAfterOffset();
			}
		}
	}

	// ----------------------------------------------------------------------------------

	private static final class TrackingFsDataInputStream extends FSDataInputStream {

		private final FSDataInputStream stream;
		private final int batchSize;
		private int remainingInBatch;

		TrackingFsDataInputStream(FSDataInputStream stream, int batchSize) {
			checkArgument(batchSize > 0);
			this.stream = stream;
			this.batchSize = batchSize;
		}

		@Override
		public void seek(long desired) throws IOException {
			stream.seek(desired);
			remainingInBatch = 0;
		}

		@Override
		public long getPos() throws IOException {
			return stream.getPos();
		}

		@Override
		public int read() throws IOException {
			remainingInBatch--;
			return stream.read();
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			remainingInBatch -= len;
			return stream.read(b, off, len);
		}

		boolean hasRemainingInBatch() {
			return remainingInBatch > 0;
		}

		void newBatch() {
			remainingInBatch = batchSize;
		}
	}
}
