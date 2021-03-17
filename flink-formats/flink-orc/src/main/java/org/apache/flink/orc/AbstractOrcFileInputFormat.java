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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connector.file.src.util.Pool;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.orc.shim.OrcShim;
import org.apache.flink.orc.util.SerializableHadoopConfigWrapper;
import org.apache.flink.orc.vector.OrcVectorizedBatchWrapper;

import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The base for ORC readers for the {@link org.apache.flink.connector.file.src.FileSource}.
 * Implements the reader initialization, vectorized reading, and pooling of column vector objects.
 *
 * <p>Subclasses implement the conversion to the specific result record(s) that they return by
 * creating via extending {@link AbstractOrcFileInputFormat.OrcReaderBatch}.
 *
 * @param <T> The type of records produced by this reader format.
 */
public abstract class AbstractOrcFileInputFormat<T, BatchT, SplitT extends FileSourceSplit>
        implements BulkFormat<T, SplitT> {

    private static final long serialVersionUID = 1L;

    protected final OrcShim<BatchT> shim;

    protected final SerializableHadoopConfigWrapper hadoopConfigWrapper;

    protected final TypeDescription schema;

    protected final int[] selectedFields;

    protected final List<OrcFilters.Predicate> conjunctPredicates;

    protected final int batchSize;

    /**
     * @param shim the shim for various Orc dependent versions. If you use the latest version,
     *     please use {@link OrcShim#defaultShim()} directly.
     * @param hadoopConfig the hadoop config for orc reader.
     * @param schema the full schema of orc format.
     * @param selectedFields the read selected field of orc format.
     * @param conjunctPredicates the filter predicates that can be evaluated.
     * @param batchSize the batch size of orc reader.
     */
    protected AbstractOrcFileInputFormat(
            final OrcShim<BatchT> shim,
            final org.apache.hadoop.conf.Configuration hadoopConfig,
            final TypeDescription schema,
            final int[] selectedFields,
            final List<OrcFilters.Predicate> conjunctPredicates,
            final int batchSize) {

        this.shim = shim;
        this.hadoopConfigWrapper = new SerializableHadoopConfigWrapper(checkNotNull(hadoopConfig));
        this.schema = checkNotNull(schema);
        this.selectedFields = checkNotNull(selectedFields);
        this.conjunctPredicates = checkNotNull(conjunctPredicates);
        this.batchSize = batchSize;
    }

    // ------------------------------------------------------------------------

    @Override
    public OrcVectorizedReader<T, BatchT> createReader(
            final Configuration config, final SplitT split) throws IOException {

        final int numBatchesToCirculate =
                config.getInteger(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY);
        final Pool<OrcReaderBatch<T, BatchT>> poolOfBatches =
                createPoolOfBatches(split, numBatchesToCirculate);

        final RecordReader orcReader =
                shim.createRecordReader(
                        hadoopConfigWrapper.getHadoopConfig(),
                        schema,
                        selectedFields,
                        conjunctPredicates,
                        split.path(),
                        split.offset(),
                        split.length());

        return new OrcVectorizedReader<>(shim, orcReader, poolOfBatches);
    }

    @Override
    public OrcVectorizedReader<T, BatchT> restoreReader(
            final Configuration config, final SplitT split) throws IOException {
        assert split.getReaderPosition().isPresent();
        final OrcVectorizedReader<T, BatchT> reader = createReader(config, split);
        reader.seek(split.getReaderPosition().get());
        return reader;
    }

    @Override
    public boolean isSplittable() {
        return true;
    }

    /**
     * Creates the {@link OrcReaderBatch} structure, which is responsible for holding the data
     * structures that hold the batch data (column vectors, row arrays, ...) and the batch
     * conversion from the ORC representation to the result format.
     */
    public abstract OrcReaderBatch<T, BatchT> createReaderBatch(
            SplitT split,
            OrcVectorizedBatchWrapper<BatchT> orcBatch,
            Pool.Recycler<OrcReaderBatch<T, BatchT>> recycler,
            int batchSize);

    /** Gets the type produced by this format. */
    @Override
    public abstract TypeInformation<T> getProducedType();

    // ------------------------------------------------------------------------

    private Pool<OrcReaderBatch<T, BatchT>> createPoolOfBatches(
            final SplitT split, final int numBatches) {
        final Pool<OrcReaderBatch<T, BatchT>> pool = new Pool<>(numBatches);

        for (int i = 0; i < numBatches; i++) {
            final OrcVectorizedBatchWrapper<BatchT> orcBatch =
                    shim.createBatchWrapper(schema, batchSize);
            final OrcReaderBatch<T, BatchT> batch =
                    createReaderBatch(split, orcBatch, pool.recycler(), batchSize);
            pool.add(batch);
        }

        return pool;
    }

    // ------------------------------------------------------------------------

    /**
     * The {@code OrcReaderBatch} class holds the data structures containing the batch data (column
     * vectors, row arrays, ...) and performs the batch conversion from the ORC representation to
     * the result format.
     *
     * <p>This base class only holds the ORC Column Vectors, subclasses hold additionally the result
     * structures and implement the conversion in {@link
     * OrcReaderBatch#convertAndGetIterator(OrcVectorizedBatchWrapper, long)}.
     */
    protected abstract static class OrcReaderBatch<T, BatchT> {

        private final OrcVectorizedBatchWrapper<BatchT> orcVectorizedRowBatch;
        private final Pool.Recycler<OrcReaderBatch<T, BatchT>> recycler;

        protected OrcReaderBatch(
                final OrcVectorizedBatchWrapper<BatchT> orcVectorizedRowBatch,
                final Pool.Recycler<OrcReaderBatch<T, BatchT>> recycler) {
            this.orcVectorizedRowBatch = checkNotNull(orcVectorizedRowBatch);
            this.recycler = checkNotNull(recycler);
        }

        /**
         * Puts this batch back into the pool. This should be called after all records from the
         * batch have been returned, typically in the {@link RecordIterator#releaseBatch()} method.
         */
        public void recycle() {
            recycler.recycle(this);
        }

        /** Gets the ORC VectorizedRowBatch structure from this batch. */
        public OrcVectorizedBatchWrapper<BatchT> orcVectorizedRowBatch() {
            return orcVectorizedRowBatch;
        }

        /**
         * Converts the ORC VectorizedRowBatch into the result structure and returns an iterator
         * over the entries.
         *
         * <p>This method may, for example, return a single element iterator that returns the entire
         * batch as one, or (as another example) return an iterator over the rows projected from
         * this column batch.
         *
         * <p>The position information in the result needs to be constructed as follows: The value
         * of {@code startingOffset} is the offset value ({@link RecordAndPosition#getOffset()}) for
         * all rows in the batch. Each row then increments the records-to-skip value ({@link
         * RecordAndPosition#getRecordSkipCount()}).
         */
        public abstract RecordIterator<T> convertAndGetIterator(
                final OrcVectorizedBatchWrapper<BatchT> orcVectorizedRowBatch,
                final long startingOffset)
                throws IOException;
    }

    // ------------------------------------------------------------------------

    /**
     * A vectorized ORC reader. This reader reads an ORC {@link BatchT} at a time and converts it to
     * one or more records to be returned. An ORC Row-wise reader would convert the batch into a set
     * of rows, while a reader for a vectorized query processor might return the whole batch as one
     * record.
     *
     * <p>The conversion of the {@code VectorizedRowBatch} happens in the specific {@link
     * OrcReaderBatch} implementation.
     *
     * <p>The reader tracks its current position using ORC's <i>row numbers</i>. Each record in a
     * batch is addressed by the starting row number of the batch, plus the number of records to be
     * skipped before.
     *
     * @param <T> The type of the records returned by the reader.
     */
    protected static final class OrcVectorizedReader<T, BatchT> implements BulkFormat.Reader<T> {

        private final OrcShim<BatchT> shim;
        private final RecordReader orcReader;
        private final Pool<OrcReaderBatch<T, BatchT>> pool;
        private long recordsToSkip;

        protected OrcVectorizedReader(
                final OrcShim<BatchT> shim,
                final RecordReader orcReader,
                final Pool<OrcReaderBatch<T, BatchT>> pool) {

            this.shim = checkNotNull(shim, "orc shim");
            this.orcReader = checkNotNull(orcReader, "orcReader");
            this.pool = checkNotNull(pool, "pool");
        }

        @Nullable
        @Override
        public RecordIterator<T> readBatch() throws IOException {
            final OrcReaderBatch<T, BatchT> batch = getCachedEntry();
            final OrcVectorizedBatchWrapper<BatchT> orcVectorBatch = batch.orcVectorizedRowBatch();

            final long orcRowNumber = orcReader.getRowNumber();
            if (!shim.nextBatch(orcReader, orcVectorBatch.getBatch())) {
                batch.recycle();
                return null;
            }

            final RecordIterator<T> records =
                    batch.convertAndGetIterator(orcVectorBatch, orcRowNumber);
            if (recordsToSkip > 0) {
                // this may leave an exhausted iterator, which is a valid result for this method
                // and is not interpreted as end-of-input or anything
                skipRecord(records);
            }
            return records;
        }

        @Override
        public void close() throws IOException {
            orcReader.close();
        }

        /**
         * The argument of {@link RecordReader#seekToRow(long)} must come from {@link
         * RecordReader#getRowNumber()}. The internal implementation of ORC is very confusing. It
         * has special behavior when dealing with Predicate.
         */
        public void seek(CheckpointedPosition position) throws IOException {
            orcReader.seekToRow(position.getOffset());
            recordsToSkip = position.getRecordsAfterOffset();
        }

        private OrcReaderBatch<T, BatchT> getCachedEntry() throws IOException {
            try {
                return pool.pollEntry();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted");
            }
        }

        private void skipRecord(RecordIterator<T> records) {
            while (recordsToSkip > 0 && records.next() != null) {
                recordsToSkip--;
            }
        }
    }
}
