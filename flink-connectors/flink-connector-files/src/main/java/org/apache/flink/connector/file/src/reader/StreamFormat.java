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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.core.fs.FSDataInputStream;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

/**
 * A reader format that reads individual records from a stream.
 *
 * <p>The outer class {@code StreamFormat} acts mainly as a configuration holder and factory for the reader.
 * The actual reading is done by the {@link StreamFormat.Reader}, which is created based on an
 * input stream in the {@link #createReader(Configuration, FSDataInputStream, long, long)} method
 * and restored (from checkpointed positions) in the method
 * {@link #restoreReader(Configuration, FSDataInputStream, long, long, long)}.
 *
 * <p>Compared to the {@link BulkFormat}, the stream format handles a few things out-of-the-box, like
 * deciding how to batch records or dealing with compression.
 *
 * <p>For a simpler version of this interface, for format that do not support splitting or logical record
 * offsets during checkpointing, see {@link SimpleStreamFormat}.
 *
 * <h2>Splitting</h2>
 *
 * <p>File splitting means dividing a file into multiple regions that can be read independently.
 * Whether a format supports splitting is indicated via the {@link #isSplittable()} method.
 *
 * <p>Splitting has the potential to increase parallelism and performance, but poses additional
 * constraints on the format readers: Readers need to be able to find a consistent starting point
 * within the file near the offset where the split starts, (like the next record delimiter, or a
 * block start or a sync marker). This is not necessarily possible for all formats, which is why
 * splitting is optional.
 *
 * <h2>Checkpointing</h2>
 *
 * <p>Readers can optionally return the current position of the reader, via the
 * {@link StreamFormat.Reader#getCheckpointedPosition()}. This can improve recovery speed from
 * a checkpoint.
 *
 * <p>By default (if that method is not overridden or returns null), then recovery from a checkpoint
 * works by reading the split again and skipping the number of records that were processed before
 * the checkpoint. Implementing this method allows formats to directly seek to that position, rather
 * than read and discard a number or records.
 *
 * <p>The position is a combination of offset in the file and a number of records to skip after
 * this offset (see {@link CheckpointedPosition}). This helps formats that cannot describe all
 * record positions by an offset, for example  because records are compressed in batches or stored
 * in a columnar layout (e.g., ORC, Parquet).
 * The default behavior can be viewed as returning a {@code CheckpointedPosition} where the offset
 * is always zero and only the {@link CheckpointedPosition#getRecordsAfterOffset()} is incremented
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
 * much reduces the thread-to-thread handover overhead.
 *
 * <p>This batching is by default based on I/O fetch size for the {@code StreamFormat}, meaning
 * the set of records derived from one I/O buffer will be handed over as one.
 * See {@link StreamFormat#FETCH_IO_SIZE} to configure that fetch size.
 *
 * @param <T> The type of records created by this format reader.
 */
@PublicEvolving
public interface StreamFormat<T> extends Serializable, ResultTypeQueryable<T> {

	/**
	 * Creates a new reader to read in this format. This method is called when a fresh reader is
	 * created for a split that was assigned from the enumerator. This method may also be called on
	 * recovery from a checkpoint, if the reader never stored an offset in the checkpoint
	 * (see {@link #restoreReader(Configuration, FSDataInputStream, long, long, long)} for details.
	 *
	 * <p>If the format is {@link #isSplittable() splittable}, then the {@code stream} is positioned
	 * to the beginning of the file split, otherwise it will be at position zero.
	 *
	 * <p>The {@code fileLen} is the length of the entire file, while {@code splitEnd} is the offset of
	 * the first byte after the split end boundary (exclusive end boundary). For non-splittable formats,
	 * both values are identical.
	 */
	Reader<T> createReader(
			Configuration config,
			FSDataInputStream stream,
			long fileLen,
			long splitEnd) throws IOException;

	/**
	 * Restores a reader from a checkpointed position. This method is called when the reader is recovered
	 * from a checkpoint and the reader has previously stored an offset into the checkpoint, by returning
	 * from the {@link Reader#getCheckpointedPosition()} a value with non-negative
	 * {@link CheckpointedPosition#getOffset() offset}. That value is supplied as the {@code restoredOffset}.
	 *
	 * <p>If the format is {@link #isSplittable() splittable}, then the {@code stream} is positioned
	 * to the beginning of the file split, otherwise it will be at position zero. The stream is NOT
	 * positioned to the checkpointed offset, because the format is free to interpret this offset in
	 * a different way than the byte offset in the file (for example as a record index).
	 *
	 * <p>If the reader never produced a {@code CheckpointedPosition} with a non-negative offset before, then
	 * this method is not called, and the reader is created in the same way as a fresh reader via the method
	 * {@link #createReader(Configuration, FSDataInputStream, long, long)} and the appropriate number of
	 * records are read and discarded, to position to reader to the checkpointed position.
	 *
	 * <p>Having a different method for restoring readers to a checkpointed position allows readers to
	 * seek to the start position differently in that case, compared to when the reader is created from
	 * a split offset generated at the enumerator. In the latter case, the offsets are commonly "approximate",
	 * because the enumerator typically generates splits based only on metadata. Reader then have to skip some
	 * bytes while searching for the next position to start from (based on a delimiter, sync marker, block
	 * offset, etc.). In contrast, checkpointed offsets are often precise, because they were recorded as the
	 * reader when through the data stream. Starting a reader from a checkpointed offset may hence not
	 * require and search for the next delimiter/block/marker.
	 *
	 * <p>The {@code fileLen} is the length of the entire file, while {@code splitEnd} is the offset of
	 * the first byte after the split end boundary (exclusive end boundary). For non-splittable formats,
	 * both values are identical.
	 */
	Reader<T> restoreReader(
			Configuration config,
			FSDataInputStream stream,
			long restoredOffset,
			long fileLen,
			long splitEnd) throws IOException;

	/**
	 * Checks whether this format is splittable. Splittable formats allow Flink to create multiple splits
	 * per file, so that Flink can read multiple regions of the file concurrently.
	 *
	 * <p>See {@link StreamFormat top-level JavaDocs} (section "Splitting") for details.
	 */
	boolean isSplittable();

	/**
	 * Gets the type produced by this format. This type will be the type produced by the file
	 * source as a whole.
	 */
	@Override
	TypeInformation<T> getProducedType();

	// ------------------------------------------------------------------------

	/**
	 * The config option to define how many bytes to be read by the I/O thread in one fetch operation.
	 */
	ConfigOption<MemorySize> FETCH_IO_SIZE = ConfigOptions
			.key("source.file.stream.io-fetch-size")
			.memoryType()
			.defaultValue(MemorySize.ofMebiBytes(1L))
			.withDescription("The approximate of bytes per fetch that is passed from the I/O thread to file reader.");

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
		 * Optionally returns the current position of the reader. This can be implemented by readers that
		 * want to speed up recovery from a checkpoint.
		 *
		 * <p>The current position of the reader is the position of the next record that will be returned
		 * in a call to {@link #read()}. This can be implemented by readers that want to speed up recovery
		 * from a checkpoint.
		 *
		 * <p>See the {@link StreamFormat top-level class comment} (section "Checkpointing") for details.
		 */
		@Nullable
		default CheckpointedPosition getCheckpointedPosition() {
			return null;
		}
	}
}
