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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.util.Pool;
import org.apache.flink.connector.file.table.ColumnarRowIterator;
import org.apache.flink.connector.file.table.PartitionFieldExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.utils.SerializableConfiguration;
import org.apache.flink.formats.parquet.vector.ColumnBatchFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.columnar.ColumnarRowData;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.formats.parquet.vector.ParquetSplitReaderUtil.createVectorFromConstant;

/**
 * A {@link ParquetVectorizedInputFormat} to provide {@link RowData} iterator. Using {@link
 * ColumnarRowData} to provide a row view of column batch.
 */
public class ParquetColumnarRowInputFormat<SplitT extends FileSourceSplit>
        extends ParquetVectorizedInputFormat<RowData, SplitT> {

    private static final long serialVersionUID = 1L;

    private final TypeInformation<RowData> producedTypeInfo;

    /** Constructor to create parquet format without extra fields. */
    ParquetColumnarRowInputFormat(
            Configuration hadoopConfig,
            RowType projectedType,
            TypeInformation<RowData> producedTypeInfo,
            int batchSize,
            boolean isUtcTimestamp,
            boolean isCaseSensitive) {
        this(
                hadoopConfig,
                projectedType,
                producedTypeInfo,
                ColumnBatchFactory.withoutExtraFields(),
                batchSize,
                isUtcTimestamp,
                isCaseSensitive);
    }

    /**
     * Constructor to create parquet format with extra fields created by {@link ColumnBatchFactory}.
     *
     * @param projectedType the projected row type for parquet format, excludes extra fields.
     * @param producedTypeInfo the produced row type info for this input format, includes extra
     *     fields.
     * @param batchFactory factory for creating column batch, can cram in extra fields.
     */
    ParquetColumnarRowInputFormat(
            Configuration hadoopConfig,
            RowType projectedType,
            TypeInformation<RowData> producedTypeInfo,
            ColumnBatchFactory<SplitT> batchFactory,
            int batchSize,
            boolean isUtcTimestamp,
            boolean isCaseSensitive) {
        super(
                new SerializableConfiguration(hadoopConfig),
                projectedType,
                batchFactory,
                batchSize,
                isUtcTimestamp,
                isCaseSensitive);
        this.producedTypeInfo = producedTypeInfo;
    }

    @Override
    protected int numBatchesToCirculate(org.apache.flink.configuration.Configuration config) {
        // In a VectorizedColumnBatch, the dictionary will be lazied deserialized.
        // If there are multiple batches at the same time, there may be thread safety problems,
        // because the deserialization of the dictionary depends on some internal structures.
        // We need set numBatchesToCirculate to 1.
        return 1;
    }

    @Override
    protected ParquetReaderBatch<RowData> createReaderBatch(
            WritableColumnVector[] writableVectors,
            VectorizedColumnBatch columnarBatch,
            Pool.Recycler<ParquetReaderBatch<RowData>> recycler) {
        return new ColumnarRowReaderBatch(writableVectors, columnarBatch, recycler);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    private static class ColumnarRowReaderBatch extends ParquetReaderBatch<RowData> {

        private final ColumnarRowIterator result;

        private ColumnarRowReaderBatch(
                WritableColumnVector[] writableVectors,
                VectorizedColumnBatch columnarBatch,
                Pool.Recycler<ParquetReaderBatch<RowData>> recycler) {
            super(writableVectors, columnarBatch, recycler);
            this.result =
                    new ColumnarRowIterator(new ColumnarRowData(columnarBatch), this::recycle);
        }

        @Override
        public RecordIterator<RowData> convertAndGetIterator(long rowsReturned) {
            result.set(columnarBatch.getNumRows(), rowsReturned);
            return result;
        }
    }

    /**
     * Create a partitioned {@link ParquetColumnarRowInputFormat}, the partition columns can be
     * generated by {@link Path}.
     */
    public static <SplitT extends FileSourceSplit>
            ParquetColumnarRowInputFormat<SplitT> createPartitionedFormat(
                    Configuration hadoopConfig,
                    RowType producedRowType,
                    TypeInformation<RowData> producedTypeInfo,
                    List<String> partitionKeys,
                    PartitionFieldExtractor<SplitT> extractor,
                    int batchSize,
                    boolean isUtcTimestamp,
                    boolean isCaseSensitive) {
        // TODO FLINK-25113 all this partition keys code should be pruned from the parquet format,
        //  because now FileSystemTableSource uses FileInfoExtractorBulkFormat for reading partition
        //  keys.

        RowType projectedRowType =
                new RowType(
                        producedRowType.getFields().stream()
                                .filter(field -> !partitionKeys.contains(field.getName()))
                                .collect(Collectors.toList()));
        List<String> projectedNames = projectedRowType.getFieldNames();

        ColumnBatchFactory<SplitT> factory =
                (SplitT split, ColumnVector[] parquetVectors) -> {
                    // create and initialize the row batch
                    ColumnVector[] vectors = new ColumnVector[producedRowType.getFieldCount()];
                    for (int i = 0; i < vectors.length; i++) {
                        RowType.RowField field = producedRowType.getFields().get(i);

                        vectors[i] =
                                partitionKeys.contains(field.getName())
                                        ? createVectorFromConstant(
                                                field.getType(),
                                                extractor.extract(
                                                        split, field.getName(), field.getType()),
                                                batchSize)
                                        : parquetVectors[projectedNames.indexOf(field.getName())];
                    }
                    return new VectorizedColumnBatch(vectors);
                };

        return new ParquetColumnarRowInputFormat<>(
                hadoopConfig,
                projectedRowType,
                producedTypeInfo,
                factory,
                batchSize,
                isUtcTimestamp,
                isCaseSensitive);
    }
}
