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

package org.apache.flink.orc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.Pool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.shim.OrcShim;
import org.apache.flink.orc.util.SerializableHadoopConfigWrapper;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The base for ORC readers for the {@link org.apache.flink.connector.file.src.FileSource}.
 * Implements the reader initialization, vectorized reading, and pooling of column vector objects.
 *
 * <p>Subclasses implement the conversion to the specific result record(s) that they return by creating
 * via extending {@link AbstractOrcFileInputFormat.OrcReaderBatch}.
 *
 * @param <T> The type of records produced by this reader format.
 */
public abstract class AbstractOrcFileInputFormat<T> implements BulkFormat<T> {

	private static final long serialVersionUID = 1L;

	// ------------------------------------------------------------------------

	/**
	 * Config option for the number of column vector batches that are created and circulating
	 * between the I/O threads and the processing threads.
	 */
	public static final ConfigOption<Integer> NUM_BATCHES_IN_FLIGHT = ConfigOptions
			.key("format.orc.batches-in-flight")
			.intType()
			.defaultValue(2)
			.withDescription(
					"The number of column vector batches that are in-flight between the I/O threads " +
					"and the processing threads.");

	// ------------------------------------------------------------------------

	protected final SerializableHadoopConfigWrapper hadoopConfigWrapper;

	protected final TypeDescription schema;

	protected final int[] selectedFields;

	protected final List<OrcSplitReader.Predicate> conjunctPredicates;

	protected final int batchSize;

	protected AbstractOrcFileInputFormat(
			final org.apache.hadoop.conf.Configuration hadoopConfig,
			final TypeDescription schema,
			final int[] selectedFields,
			final List<OrcSplitReader.Predicate> conjunctPredicates,
			final int batchSize) {

		this.hadoopConfigWrapper = new SerializableHadoopConfigWrapper(checkNotNull(hadoopConfig));
		this.schema = checkNotNull(schema);
		this.selectedFields = checkNotNull(selectedFields);
		this.conjunctPredicates = checkNotNull(conjunctPredicates);
		this.batchSize = batchSize;
	}

	// ------------------------------------------------------------------------

	@Override
	public Reader<T> createReader(
			final Configuration config,
			final Path filePath,
			final long offset,
			final long length,
			final long recordsToSkip) throws IOException {

		final int numBatchesToCirculate = config.getInteger(NUM_BATCHES_IN_FLIGHT);
		final Pool<OrcReaderBatch<T>> poolOfBatches = createPoolOfBatches(numBatchesToCirculate);

		final RecordReader orcReader = OrcShim.defaultShim().createRecordReader(
				hadoopConfigWrapper.getHadoopConfig(),
				schema,
				selectedFields,
				conjunctPredicates,
				filePath, offset, length);

		if (recordsToSkip > 0) {
			orcReader.seekToRow(recordsToSkip);
		}

		return new OrcVectorizedReader<>(orcReader, poolOfBatches, recordsToSkip);
	}

	/**
	 * Creates the {@link OrcReaderBatch} structure, which is responsible for holding the data structures
	 * that hold the batch data (column vectors, row arrays, ...) and the batch conversion from the
	 * ORC representation to the result format.
	 */
	public abstract OrcReaderBatch<T> createReaderBatch(
			VectorizedRowBatch orcVectorizedRowBatch,
			Pool.Recycler<OrcReaderBatch<T>> recycler,
			int batchSize);

	/**
	 * Gets the type produced by this format.
	 */
	@Override
	public abstract TypeInformation<T> getProducedType();

	private Pool<OrcReaderBatch<T>> createPoolOfBatches(final int numBatches) throws IOException {
		final Pool<OrcReaderBatch<T>> pool = new Pool<>(numBatches);

		for (int i = 0; i < numBatches; i++) {
			final VectorizedRowBatch orcVectorizedRowBatch = schema.createRowBatch(batchSize);
			final OrcReaderBatch<T> batch = createReaderBatch(orcVectorizedRowBatch, pool.recycler(), batchSize);
			pool.add(batch);
		}

		return pool;
	}

	// ------------------------------------------------------------------------

	/**
	 * The {@code OrcReaderBatch} class holds the data structures containing the batch data
	 * (column vectors, row arrays, ...) and performs the batch conversion from the ORC
	 * representation to the result format.
	 *
	 * <p>This base class only holds the ORC Column Vectors, subclasses hold additionally the result
	 * structures and implement the conversion in
	 * {@link OrcReaderBatch#convertAndGetIterator(VectorizedRowBatch, long, long)}.
	 */
	protected abstract static class OrcReaderBatch<T> {

		private final VectorizedRowBatch orcVectorizedRowBatch;

		private final Pool.Recycler<OrcReaderBatch<T>> recycler;

		protected OrcReaderBatch(
				final VectorizedRowBatch orcVectorizedRowBatch,
				final Pool.Recycler<OrcReaderBatch<T>> recycler) {
			this.orcVectorizedRowBatch = checkNotNull(orcVectorizedRowBatch);
			this.recycler = checkNotNull(recycler);
		}

		public void recycle() {
			recycler.recycle(this);
		}

		public VectorizedRowBatch orcVectorizedRowBatch() {
			return orcVectorizedRowBatch;
		}

		public abstract RecordIterator<T> convertAndGetIterator(
				final VectorizedRowBatch orcVectorizedRowBatch,
				final long position,
				final long recordsBefore) throws IOException;
	}

	// ------------------------------------------------------------------------

	/**
	 * A vectorized ORC reader. This reader reads an ORC {@link VectorizedRowBatch} at a time and
	 * converts it to one or more records to be returned. An ORC Row-wise reader would convert the
	 * batch into a set of rows, while a reader for a vectorized query processor might return
	 * the whole batch as one record.
	 *
	 * <p>The conversion of the {@code VectorizedRowBatch} happens in the specific {@link OrcReaderBatch}
	 * implementation.
	 *
	 * @param <T> The type of the records returned by the reader.
	 */
	protected static final class OrcVectorizedReader<T> implements BulkFormat.Reader<T> {

		private final RecordReader orcReader;

		private final Pool<OrcReaderBatch<T>> pool;

		private long recordsReturnedSoFar;

		protected OrcVectorizedReader(
				final RecordReader orcReader,
				final Pool<OrcReaderBatch<T>> pool,
				final long recordsPreviouslyReturned) {

			checkArgument(recordsPreviouslyReturned >= 0, "recordsPreviouslyReturned must be >= 0");
			this.orcReader = checkNotNull(orcReader, "orcReader");
			this.pool = checkNotNull(pool, "pool");
			this.recordsReturnedSoFar = recordsPreviouslyReturned;
		}

		@Nullable
		@Override
		public RecordIterator<T> readBatch() throws IOException {
			final OrcReaderBatch<T> batch = getCachedEntry();
			final VectorizedRowBatch orcVectorBatch = batch.orcVectorizedRowBatch();

			if (orcReader.nextBatch(orcVectorBatch)) {
				final long recordsBefore = recordsReturnedSoFar;
				recordsReturnedSoFar += orcVectorBatch.size;
				return batch.convertAndGetIterator(orcVectorBatch, 0L, recordsBefore);
			} else {
				batch.recycle();
				return null;
			}
		}

		@Override
		public void close() throws IOException {
			orcReader.close();
		}

		private OrcReaderBatch<T> getCachedEntry() throws IOException {
			try {
				return pool.pollEntry();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IOException("Interrupted");
			}
		}
	}
}
