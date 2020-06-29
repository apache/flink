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
import org.apache.flink.connector.file.src.util.CheckpointablePosition;
import org.apache.flink.connector.file.src.util.IteratorResultIterator;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The FormatReaderAdapter turns a {@link FileRecordFormat} into a {@link BulkFormat}.
 */
public final class FileRecordFormatAdapter<T> implements BulkFormat<T> {

	/**
	 * Config option for the number of records to hand over in each fetch.
	 *
	 * <p>The number should be large enough so that the thread-to-thread handover overhead
	 * is amortized across the records, but small enough so that the these records together do
	 * not consume too memory to be feasible.
	 */
	public static final ConfigOption<Integer> RECORDS_PER_FETCH = ConfigOptions
			.key("source.file.records.fetch-size")
			.intType()
			.defaultValue(128)
			.withDescription("The number of records to hand over from the I/O thread to file reader in one unit.");

	// ------------------------------------------------------------------------

	private static final long serialVersionUID = 1L;

	private final FileRecordFormat<T> fileFormat;

	public FileRecordFormatAdapter(FileRecordFormat<T> fileFormat) {
		this.fileFormat = checkNotNull(fileFormat);
	}

	@Override
	public Reader<T> createReader(Configuration config, Path filePath, long offset, long length, long recordsToSkip) throws IOException {
		final int numRecordsPerBatch = config.get(RECORDS_PER_FETCH);
		final FileRecordFormat.Reader<T> reader = fileFormat.createReader(config, filePath, offset, length);
		try {
			for (long remaining = recordsToSkip; remaining > 0; remaining--) {
				reader.read();
			}

			return new Reader<>(reader, numRecordsPerBatch);
		}
		catch (Throwable t) {
			IOUtils.closeQuietly(reader);
			ExceptionUtils.rethrowIOException(t);
			return null; // silence the compiler
		}
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return fileFormat.getProducedType();
	}

	// ------------------------------------------------------------------------

	/**
	 * The reader adapter, from {@link FileRecordFormat.Reader} to {@link BulkFormat.Reader}.
	 */
	public static final class Reader<T> implements BulkFormat.Reader<T> {

		private final FileRecordFormat.Reader<T> reader;
		private final int numRecordsPerBatch;

		private long lastOffset;
		private long lastRecordsAfterOffset;

		Reader(FileRecordFormat.Reader<T> reader, int numRecordsPerBatch) {
			checkArgument(numRecordsPerBatch > 0, "numRecordsPerBatch must be > 0");
			this.reader = checkNotNull(reader);
			this.numRecordsPerBatch = numRecordsPerBatch;
		}

		@Nullable
		@Override
		public RecordIterator<T> readBatch() throws IOException {
			updateCheckpointablePosition();

			final ArrayList<T> result = new ArrayList<>(numRecordsPerBatch);
			T next;
			int remaining = numRecordsPerBatch;
			while (remaining-- > 0 && ((next = reader.read()) != null)) {
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
			reader.close();
		}

		private void updateCheckpointablePosition() {
			final CheckpointablePosition position = reader.getCheckpointablePosition();
			if (position != null) {
				this.lastOffset = position.getOffset();
				this.lastRecordsAfterOffset = position.getRecordsAfterOffset();
			}
		}
	}
}
