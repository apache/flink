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

package org.apache.flink.formats.parquet;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connector.file.src.util.Pool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.utils.ParquetSchemaConverter;
import org.apache.flink.formats.parquet.utils.SerializableConfiguration;
import org.apache.flink.formats.parquet.vector.ColumnBatchFactory;
import org.apache.flink.formats.parquet.vector.ParquetDecimalVector;
import org.apache.flink.formats.parquet.vector.reader.ColumnReader;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.formats.parquet.vector.ParquetSplitReaderUtil.createColumnReader;
import static org.apache.flink.formats.parquet.vector.ParquetSplitReaderUtil.createWritableColumnVector;
import static org.apache.parquet.hadoop.ParquetInputFormat.getFilter;

/**
 * Parquet {@link BulkFormat} that reads data from the file to {@link VectorizedColumnBatch} in
 * vectorized mode.
 */
public abstract class ParquetVectorizedInputFormat<T, SplitT extends FileSourceSplit>
        implements BulkFormat<T, SplitT> {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetVectorizedInputFormat.class);
    private static final long serialVersionUID = 1L;

    protected final SerializableConfiguration hadoopConfig;
    private final String[] projectedFields;
    private final LogicalType[] projectedTypes;
    private final ColumnBatchFactory<SplitT> batchFactory;
    private final int batchSize;
    protected final boolean isUtcTimestamp;
    private final boolean isCaseSensitive;

    public ParquetVectorizedInputFormat(
            SerializableConfiguration hadoopConfig,
            RowType projectedType,
            ColumnBatchFactory<SplitT> batchFactory,
            int batchSize,
            boolean isUtcTimestamp,
            boolean isCaseSensitive) {
        this.hadoopConfig = hadoopConfig;
        this.projectedFields = projectedType.getFieldNames().toArray(new String[0]);
        this.projectedTypes = projectedType.getChildren().toArray(new LogicalType[0]);
        this.batchFactory = batchFactory;
        this.batchSize = batchSize;
        this.isUtcTimestamp = isUtcTimestamp;
        this.isCaseSensitive = isCaseSensitive;
    }

    @Override
    public ParquetReader createReader(final Configuration config, final SplitT split)
            throws IOException {

        final Path filePath = split.path();
        final long splitOffset = split.offset();
        final long splitLength = split.length();

        // Using Flink FileSystem instead of Hadoop FileSystem directly, so we can get the hadoop
        // config that create inputFile needed from flink-conf.yaml
        final FileSystem fs = filePath.getFileSystem();
        final ParquetInputFile inputFile =
                new ParquetInputFile(fs.open(filePath), fs.getFileStatus(filePath).getLen());

        // Notice: This filter is RowGroups level, not individual records.
        FilterCompat.Filter filter = getFilter(hadoopConfig.conf());
        ParquetReadOptions parquetReadOptions =
                ParquetReadOptions.builder()
                        .withRange(splitOffset, splitOffset + splitLength)
                        .withRecordFilter(filter)
                        .build();
        ParquetFileReader parquetFileReader = ParquetFileReader.open(inputFile, parquetReadOptions);

        Set<Integer> unknownFieldsIndices = new HashSet<>();
        MessageType fileSchema = parquetFileReader.getFooter().getFileMetaData().getSchema();
        // Pruning unnecessary column, we should set the projection schema before running any
        // filtering (e.g. getting filtered record count) because projection impacts filtering
        MessageType requestedSchema = clipParquetSchema(fileSchema, unknownFieldsIndices);
        parquetFileReader.setRequestedSchema(requestedSchema);

        checkSchema(fileSchema, requestedSchema);

        final long totalRowCount = parquetFileReader.getFilteredRecordCount();
        final Pool<ParquetReaderBatch<T>> poolOfBatches =
                createPoolOfBatches(split, requestedSchema, numBatchesToCirculate(config));

        return new ParquetReader(
                parquetFileReader,
                requestedSchema,
                unknownFieldsIndices,
                totalRowCount,
                poolOfBatches);
    }

    protected int numBatchesToCirculate(Configuration config) {
        return config.getInteger(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY);
    }

    @Override
    public ParquetReader restoreReader(final Configuration config, final SplitT split)
            throws IOException {

        assert split.getReaderPosition().isPresent();
        final CheckpointedPosition checkpointedPosition = split.getReaderPosition().get();

        Preconditions.checkArgument(
                checkpointedPosition.getOffset() == CheckpointedPosition.NO_OFFSET,
                "The offset of CheckpointedPosition should always be NO_OFFSET");
        ParquetReader reader = createReader(config, split);
        reader.seek(checkpointedPosition.getRecordsAfterOffset());
        return reader;
    }

    @Override
    public boolean isSplittable() {
        return true;
    }

    /** Clips `parquetSchema` according to `fieldNames`. */
    private MessageType clipParquetSchema(
            GroupType parquetSchema, Collection<Integer> unknownFieldsIndices) {
        Type[] types = new Type[projectedFields.length];
        if (isCaseSensitive) {
            for (int i = 0; i < projectedFields.length; ++i) {
                String fieldName = projectedFields[i];
                if (!parquetSchema.containsField(fieldName)) {
                    LOG.warn(
                            "{} does not exist in {}, will fill the field with null.",
                            fieldName,
                            parquetSchema);
                    types[i] =
                            ParquetSchemaConverter.convertToParquetType(
                                    fieldName, projectedTypes[i]);
                    unknownFieldsIndices.add(i);
                } else {
                    types[i] = parquetSchema.getType(fieldName);
                }
            }
        } else {
            Map<String, Type> caseInsensitiveFieldMap = new HashMap<>();
            for (Type type : parquetSchema.getFields()) {
                caseInsensitiveFieldMap.compute(
                        type.getName().toLowerCase(Locale.ROOT),
                        (key, previousType) -> {
                            if (previousType != null) {
                                throw new FlinkRuntimeException(
                                        "Parquet with case insensitive mode should have no duplicate key: "
                                                + key);
                            }
                            return type;
                        });
            }
            for (int i = 0; i < projectedFields.length; ++i) {
                Type type =
                        caseInsensitiveFieldMap.get(projectedFields[i].toLowerCase(Locale.ROOT));
                if (type == null) {
                    LOG.warn(
                            "{} does not exist in {}, will fill the field with null.",
                            projectedFields[i],
                            parquetSchema);
                    type =
                            ParquetSchemaConverter.convertToParquetType(
                                    projectedFields[i].toLowerCase(Locale.ROOT), projectedTypes[i]);
                    unknownFieldsIndices.add(i);
                }
                // TODO clip for array,map,row types.
                types[i] = type;
            }
        }

        return Types.buildMessage().addFields(types).named("flink-parquet");
    }

    private void checkSchema(MessageType fileSchema, MessageType requestedSchema)
            throws IOException, UnsupportedOperationException {
        if (projectedFields.length != requestedSchema.getFieldCount()) {
            throw new RuntimeException(
                    "The quality of field type is incompatible with the request schema!");
        }

        /*
         * Check that the requested schema is supported.
         */
        for (int i = 0; i < requestedSchema.getFieldCount(); ++i) {
            String[] colPath = requestedSchema.getPaths().get(i);
            if (fileSchema.containsPath(colPath)) {
                ColumnDescriptor fd = fileSchema.getColumnDescription(colPath);
                if (!fd.equals(requestedSchema.getColumns().get(i))) {
                    throw new UnsupportedOperationException("Schema evolution not supported.");
                }
            } else {
                if (requestedSchema.getColumns().get(i).getMaxDefinitionLevel() == 0) {
                    // Column is missing in data but the required data is non-nullable. This file is
                    // invalid.
                    throw new IOException(
                            "Required column is missing in data file. Col: "
                                    + Arrays.toString(colPath));
                }
            }
        }
    }

    private Pool<ParquetReaderBatch<T>> createPoolOfBatches(
            SplitT split, MessageType requestedSchema, int numBatches) {
        final Pool<ParquetReaderBatch<T>> pool = new Pool<>(numBatches);

        for (int i = 0; i < numBatches; i++) {
            pool.add(createReaderBatch(split, requestedSchema, pool.recycler()));
        }

        return pool;
    }

    private ParquetReaderBatch<T> createReaderBatch(
            SplitT split,
            MessageType requestedSchema,
            Pool.Recycler<ParquetReaderBatch<T>> recycler) {
        WritableColumnVector[] writableVectors = createWritableVectors(requestedSchema);
        VectorizedColumnBatch columnarBatch =
                batchFactory.create(split, createReadableVectors(writableVectors));
        return createReaderBatch(writableVectors, columnarBatch, recycler);
    }

    private WritableColumnVector[] createWritableVectors(MessageType requestedSchema) {
        WritableColumnVector[] columns = new WritableColumnVector[projectedTypes.length];
        List<Type> types = requestedSchema.getFields();
        for (int i = 0; i < projectedTypes.length; i++) {
            columns[i] =
                    createWritableColumnVector(
                            batchSize,
                            projectedTypes[i],
                            types.get(i),
                            requestedSchema.getColumns(),
                            0);
        }
        return columns;
    }

    /**
     * Create readable vectors from writable vectors. Especially for decimal, see {@link
     * ParquetDecimalVector}.
     */
    private ColumnVector[] createReadableVectors(WritableColumnVector[] writableVectors) {
        ColumnVector[] vectors = new ColumnVector[writableVectors.length];
        for (int i = 0; i < writableVectors.length; i++) {
            vectors[i] =
                    projectedTypes[i].getTypeRoot() == LogicalTypeRoot.DECIMAL
                            ? new ParquetDecimalVector(writableVectors[i])
                            : writableVectors[i];
        }
        return vectors;
    }

    private class ParquetReader implements BulkFormat.Reader<T> {

        private ParquetFileReader reader;

        private final MessageType requestedSchema;

        private final Set<Integer> unknownFieldsIndices;

        /**
         * The total number of rows this RecordReader will eventually read. The sum of the rows of
         * all the row groups.
         */
        private final long totalRowCount;

        private final Pool<ParquetReaderBatch<T>> pool;

        /** The number of rows that have been returned. */
        private long rowsReturned;

        /** The number of rows that have been reading, including the current in flight row group. */
        private long totalCountLoadedSoFar;

        /**
         * For each request column, the reader to read this column. This is NULL if this column is
         * missing from the file, in which case we populate the attribute with NULL.
         */
        @SuppressWarnings("rawtypes")
        private ColumnReader[] columnReaders;

        private long recordsToSkip;

        private ParquetReader(
                ParquetFileReader reader,
                MessageType requestedSchema,
                Set<Integer> unknownFieldsIndices,
                long totalRowCount,
                Pool<ParquetReaderBatch<T>> pool) {
            this.reader = reader;
            this.requestedSchema = requestedSchema;
            this.unknownFieldsIndices = unknownFieldsIndices;
            this.totalRowCount = totalRowCount;
            this.pool = pool;
            this.rowsReturned = 0;
            this.totalCountLoadedSoFar = 0;
            this.recordsToSkip = 0;
        }

        @Nullable
        @Override
        public RecordIterator<T> readBatch() throws IOException {
            final ParquetReaderBatch<T> batch = getCachedEntry();

            final long rowsReturnedBefore = rowsReturned;
            if (!nextBatch(batch)) {
                batch.recycle();
                return null;
            }

            final RecordIterator<T> records = batch.convertAndGetIterator(rowsReturnedBefore);

            // this may leave an exhausted iterator, which is a valid result for this method
            // and is not interpreted as end-of-input or anything
            skipRecord(records);
            return records;
        }

        /** Advances to the next batch of rows. Returns false if there are no more. */
        private boolean nextBatch(ParquetReaderBatch<T> batch) throws IOException {
            for (WritableColumnVector v : batch.writableVectors) {
                v.reset();
            }
            batch.columnarBatch.setNumRows(0);
            if (rowsReturned >= totalRowCount) {
                return false;
            }
            if (rowsReturned == totalCountLoadedSoFar) {
                readNextRowGroup();
            }

            int num = (int) Math.min(batchSize, totalCountLoadedSoFar - rowsReturned);
            for (int i = 0; i < columnReaders.length; ++i) {
                if (columnReaders[i] == null) {
                    batch.writableVectors[i].fillWithNulls();
                } else {
                    //noinspection unchecked
                    columnReaders[i].readToVector(num, batch.writableVectors[i]);
                }
            }
            rowsReturned += num;
            batch.columnarBatch.setNumRows(num);
            return true;
        }

        private void readNextRowGroup() throws IOException {
            PageReadStore pages = reader.readNextRowGroup();
            if (pages == null) {
                throw new IOException(
                        "expecting more rows but reached last block. Read "
                                + rowsReturned
                                + " out of "
                                + totalRowCount);
            }

            List<Type> types = requestedSchema.getFields();
            columnReaders = new ColumnReader[types.size()];
            for (int i = 0; i < types.size(); ++i) {
                if (!unknownFieldsIndices.contains(i)) {
                    columnReaders[i] =
                            createColumnReader(
                                    isUtcTimestamp,
                                    projectedTypes[i],
                                    types.get(i),
                                    requestedSchema.getColumns(),
                                    pages,
                                    0);
                }
            }
            totalCountLoadedSoFar += pages.getRowCount();
        }

        public void seek(long rowCount) {
            if (totalCountLoadedSoFar != 0) {
                throw new UnsupportedOperationException("Only support seek at first.");
            }

            List<BlockMetaData> blockMetaData = reader.getRowGroups();

            for (BlockMetaData metaData : blockMetaData) {
                if (metaData.getRowCount() > rowCount) {
                    break;
                } else {
                    reader.skipNextRowGroup();
                    rowsReturned += metaData.getRowCount();
                    totalCountLoadedSoFar += metaData.getRowCount();
                    rowCount -= metaData.getRowCount();
                }
            }

            this.recordsToSkip = rowCount;
        }

        private ParquetReaderBatch<T> getCachedEntry() throws IOException {
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

        @Override
        public void close() throws IOException {
            if (reader != null) {
                reader.close();
                reader = null;
            }
        }
    }

    // ----------------------- Abstract method and class --------------------------

    /**
     * @param writableVectors vectors to be write
     * @param columnarBatch vectors to be read
     * @param recycler batch recycler
     */
    protected abstract ParquetReaderBatch<T> createReaderBatch(
            WritableColumnVector[] writableVectors,
            VectorizedColumnBatch columnarBatch,
            Pool.Recycler<ParquetReaderBatch<T>> recycler);

    /**
     * Reader batch that provides writing and reading capabilities. Provides {@link RecordIterator}
     * reading interface from {@link #convertAndGetIterator(long)}.
     */
    protected abstract static class ParquetReaderBatch<T> {

        private final WritableColumnVector[] writableVectors;
        protected final VectorizedColumnBatch columnarBatch;
        private final Pool.Recycler<ParquetReaderBatch<T>> recycler;

        protected ParquetReaderBatch(
                WritableColumnVector[] writableVectors,
                VectorizedColumnBatch columnarBatch,
                Pool.Recycler<ParquetReaderBatch<T>> recycler) {
            this.writableVectors = writableVectors;
            this.columnarBatch = columnarBatch;
            this.recycler = recycler;
        }

        public void recycle() {
            recycler.recycle(this);
        }

        /**
         * Provides reading iterator after the records are written to the {@link #columnarBatch}.
         *
         * @param rowsReturned The number of rows that have been returned before this batch.
         */
        public abstract RecordIterator<T> convertAndGetIterator(long rowsReturned)
                throws IOException;
    }
}
