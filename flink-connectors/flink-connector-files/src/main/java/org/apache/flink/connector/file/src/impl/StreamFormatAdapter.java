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

package org.apache.flink.connector.file.src.impl;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.InputStreamFSInputWrapper;
import org.apache.flink.api.common.io.compression.InflaterInputStreamFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.compression.StandardDeCompressors;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connector.file.src.util.IteratorResultIterator;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.MathUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.flink.connector.file.src.util.Utils.doWithCleanupOnException;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Adapter to turn a {@link StreamFormat} into a {@link BulkFormat}.
 */
@Internal
public final class StreamFormatAdapter<T> implements BulkFormat<T, FileSourceSplit> {

	private static final long serialVersionUID = 1L;

	private final StreamFormat<T> streamFormat;

	public StreamFormatAdapter(StreamFormat<T> streamFormat) {
		this.streamFormat = checkNotNull(streamFormat);
	}

	@Override
	public BulkFormat.Reader<T> createReader(
			final Configuration config,
			final FileSourceSplit split) throws IOException {

		final TrackingFsDataInputStream trackingStream = openStream(split.path(), config, split.offset());
		final long splitEnd = split.offset() + split.length();

		return doWithCleanupOnException(trackingStream, () -> {
			final StreamFormat.Reader<T> streamReader = streamFormat.createReader(
					config, trackingStream, trackingStream.getFileLength(), splitEnd);
			return new Reader<>(streamReader, trackingStream, CheckpointedPosition.NO_OFFSET, 0L);
		});
	}

	@Override
	public BulkFormat.Reader<T> restoreReader(
			final Configuration config,
			final FileSourceSplit split) throws IOException {

		assert split.getReaderPosition().isPresent();
		final CheckpointedPosition checkpointedPosition = split.getReaderPosition().get();

		final TrackingFsDataInputStream trackingStream = openStream(split.path(), config, split.offset());
		final long splitEnd = split.offset() + split.length();

		return doWithCleanupOnException(trackingStream, () -> {
			// if there never was a checkpointed offset, yet, we need to initialize the reader like a fresh reader.
			// see the JavaDocs on StreamFormat.restoreReader() for details
			final StreamFormat.Reader<T> streamReader = checkpointedPosition.getOffset() == CheckpointedPosition.NO_OFFSET
					? streamFormat.createReader(
							config, trackingStream, trackingStream.getFileLength(), splitEnd)
					: streamFormat.restoreReader(
							config, trackingStream, checkpointedPosition.getOffset(),
							trackingStream.getFileLength(), splitEnd);

			// skip the records to skip, but make sure we close the reader if something goes wrong
			doWithCleanupOnException(streamReader, () -> {
				long toSkip = checkpointedPosition.getRecordsAfterOffset();
				while (toSkip > 0 && streamReader.read() != null) {
					toSkip--;
				}
			});

			return new Reader<>(
					streamReader, trackingStream,
					checkpointedPosition.getOffset(), checkpointedPosition.getRecordsAfterOffset());
		});
	}

	@Override
	public boolean isSplittable() {
		return streamFormat.isSplittable();
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return streamFormat.getProducedType();
	}

	private static TrackingFsDataInputStream openStream(
			final Path file,
			final Configuration config,
			final long seekPosition) throws IOException {

		final FileSystem fs = file.getFileSystem();
		final long fileLength = fs.getFileStatus(file).getLen();

		final int fetchSize = MathUtils.checkedDownCast(config.get(StreamFormat.FETCH_IO_SIZE).getBytes());
		if (fetchSize <= 0) {
			throw new IllegalConfigurationException(
					String.format("The fetch size (%s) must be > 0, but is %d",
							StreamFormat.FETCH_IO_SIZE.key(), fetchSize));
		}

		final InflaterInputStreamFactory<?> deCompressor =
				StandardDeCompressors.getDecompressorForFileName(file.getPath());

		final FSDataInputStream inStream = fs.open(file);
		return doWithCleanupOnException(inStream, () -> {
			final FSDataInputStream in = deCompressor == null
					? inStream
					: new InputStreamFSInputWrapper(deCompressor.create(inStream));
			in.seek(seekPosition);
			return new TrackingFsDataInputStream(in, fileLength, fetchSize);
		});
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

		Reader(
				final StreamFormat.Reader<T> reader,
				final TrackingFsDataInputStream stream,
				final long initialOffset,
				final long initialSkipCount) {

			this.reader = checkNotNull(reader);
			this.stream = checkNotNull(stream);
			this.lastOffset = initialOffset;
			this.lastRecordsAfterOffset = initialSkipCount;
		}

		@Nullable
		@Override
		public RecordIterator<T> readBatch() throws IOException {
			updateCheckpointedPosition();
			stream.newBatch();

			final ArrayList<T> result = new ArrayList<>();
			T next;
			while (stream.hasRemainingInBatch() && (next = reader.read()) != null) {
				result.add(next);
			}

			if (result.isEmpty()) {
				return null;
			}

			final RecordIterator<T> iter = new IteratorResultIterator<>(
					result.iterator(), lastOffset, lastRecordsAfterOffset);
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

		private void updateCheckpointedPosition() {
			final CheckpointedPosition position = reader.getCheckpointedPosition();
			if (position != null) {
				this.lastOffset = position.getOffset();
				this.lastRecordsAfterOffset = position.getRecordsAfterOffset();
			}
		}
	}

	// ----------------------------------------------------------------------------------

	/**
	 * Utility stream that tracks how much has been read. This is used to decide when the reader
	 * should finish the current batch and start the next batch. That way we make the batch sizes
	 * dependent on the consumed data volume, which is more robust than making it dependent on a
	 * record count.
	 */
	private static final class TrackingFsDataInputStream extends FSDataInputStream {

		private final FSDataInputStream stream;
		private final long fileLength;
		private final int batchSize;
		private int remainingInBatch;

		TrackingFsDataInputStream(FSDataInputStream stream, long fileLength, int batchSize) {
			checkArgument(fileLength > 0L);
			checkArgument(batchSize > 0);
			this.stream = stream;
			this.fileLength = fileLength;
			this.batchSize = batchSize;
		}

		@Override
		public void seek(long desired) throws IOException {
			stream.seek(desired);
			remainingInBatch = 0;  // after each seek, we need to start a new batch
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

		@Override
		public void close() throws IOException {
			stream.close();
		}

		boolean hasRemainingInBatch() {
			return remainingInBatch > 0;
		}

		void newBatch() {
			remainingInBatch = batchSize;
		}

		long getFileLength() {
			return fileLength;
		}
	}
}
