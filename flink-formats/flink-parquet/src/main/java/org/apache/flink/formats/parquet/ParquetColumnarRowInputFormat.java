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
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.utils.SerializableConfiguration;
import org.apache.flink.formats.parquet.vector.ColumnBatchFactory;
import org.apache.flink.table.data.ColumnarRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.vector.ColumnVector;
import org.apache.flink.table.data.vector.VectorizedColumnBatch;
import org.apache.flink.table.data.vector.writable.WritableColumnVector;
import org.apache.flink.table.filesystem.ColumnarRowIterator;
import org.apache.flink.table.filesystem.PartitionFieldExtractor;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
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

    private final RowType producedType;

    /** Constructor to create parquet format without extra fields. */
    public ParquetColumnarRowInputFormat(
            Configuration hadoopConfig,
            RowType projectedType,
            int batchSize,
            boolean isUtcTimestamp,
            boolean isCaseSensitive) {
        this(
                hadoopConfig,
                projectedType,
                projectedType,
                ColumnBatchFactory.withoutExtraFields(),
                batchSize,
                isUtcTimestamp,
                isCaseSensitive);
    }

    /**
     * Constructor to create parquet format with extra fields created by {@link ColumnBatchFactory}.
     *
     * @param projectedType the projected row type for parquet format, excludes extra fields.
     * @param producedType the produced row type for this input format, includes extra fields.
     * @param batchFactory factory for creating column batch, can cram in extra fields.
     */
    public ParquetColumnarRowInputFormat(
            Configuration hadoopConfig,
            RowType projectedType,
            RowType producedType,
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
        this.producedType = producedType;
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
        return InternalTypeInfo.of(producedType);
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
                    List<String> partitionKeys,
                    PartitionFieldExtractor<SplitT> extractor,
                    int batchSize,
                    boolean isUtcTimestamp,
                    boolean isCaseSensitive) {
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
                producedRowType,
                factory,
                batchSize,
                isUtcTimestamp,
                isCaseSensitive);
    }
}
