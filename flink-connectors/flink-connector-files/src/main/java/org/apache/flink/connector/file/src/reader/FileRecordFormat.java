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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.util.CheckpointablePosition;
import org.apache.flink.core.fs.Path;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

/**
 * A reader format that reads individual records from a file.
 *
 * <p>The actual reading is done by the {@link FileRecordFormat.Reader}, which is created based on an
 * input stream in the {@link #createReader(Configuration, Path, long, long)} method.
 * The outer class acts mainly as a configuration holder and factory for the reader.
 *
 * <h2>Checkpointing</h2>
 *
 * <p>Readers can optionally return the current position of the reader, via the
 * {@link FileRecordFormat.Reader#getCheckpointablePosition()}. This can improve recovery speed from
 * a checkpoint.
 *
 * <p>By default (if that method is not overridden or returns null), then recovery from a checkpoint
 * works by reading the split again and skipping the number of records that were processed before
 * the checkpoint. Implementing this method allows formats to directly seek to that position, rather
 * than read and discard a number or records.
 *
 * <p>The position is a combination of offset in the file and a number of records to skip after
 * this offset (see {@link CheckpointablePosition}). This helps formats that cannot describe all
 * record positions by an offset, for example  because records are compressed in batches or stored
 * in a columnar layout (e.g., ORC, Parquet).
 * The default behavior can be viewed as returning a {@code CheckpointablePosition} where the offset
 * is always zero and only the {@link CheckpointablePosition#getRecordsAfterOffset()} is incremented
 * with each emitted record.
 *
 * <h2>Serializable</h2>
 *
 * <p>Like many other API classes in Flink, the outer class is serializable to support sending instances
 * to distributed workers for parallel execution. This is purely short-term serialization for RPC and
 * no instance of this will be long-term persisted in a serialized form.
 *
 * <h2>Record Batching</h2>
 *
 * <p>Internally in the file source, the readers pass batches of records from the reading
 * threads (that perform the typically blocking I/O operations) to the async mailbox threads that
 * do the streaming and batch data processing. Passing records in batches (rather than one-at-a-time)
 * much reduce the thread-to-thread handover overhead.
 *
 * <p>This batching is by default based a number of records. See {@link FileRecordFormatAdapter#RECORDS_PER_FETCH}
 * to configure that handover batch size.
 */
public interface FileRecordFormat<T> extends Serializable {

	/**
	 * Creates the reader to read in this format.
	 */
	FileRecordFormat.Reader<T> createReader(Configuration config, Path filePath, long offset, long length) throws IOException;

	/**
	 * Gets the type produced by this format. This type will be the type produced by the file
	 * source as a whole.
	 */
	TypeInformation<T> getProducedType();

	// ------------------------------------------------------------------------


	/**
	 * The actual reader that reads the records.
	 */
	interface Reader<T> extends Closeable {

		/**
		 * Reads the next record. Returns {@code null} when the input has reached its end.
		 */
		@Nullable
		T read() throws IOException;

		/**
		 * Closes the reader to release all resources.
		 */
		@Override
		void close() throws IOException;

		/**
		 * Optionally returns the current position of the reader. This can be implemented by readers
		 * that want to speed up from a checkpoint.
		 *
		 * <p>See the {@link FileRecordFormat top-level class comment} (section "Checkpointing") for details.
		 */
		@Nullable
		default CheckpointablePosition getCheckpointablePosition() {
			return null;
		}
	}
}
