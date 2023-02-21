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
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.util.Pool;
import org.apache.flink.connector.file.table.ColumnarRowIterator;
import org.apache.flink.connector.file.table.PartitionFieldExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.shim.OrcShim;
import org.apache.flink.orc.util.OrcFormatStatisticsReportUtil;
import org.apache.flink.orc.vector.ColumnBatchFactory;
import org.apache.flink.orc.vector.OrcVectorizedBatchWrapper;
import org.apache.flink.table.connector.format.FileBasedStatisticsReportableInputFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.columnar.ColumnarRowData;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.orc.OrcSplitReaderUtil.convertToOrcTypeWithPart;
import static org.apache.flink.orc.OrcSplitReaderUtil.getNonPartNames;
import static org.apache.flink.orc.OrcSplitReaderUtil.getSelectedOrcFields;
import static org.apache.flink.orc.vector.AbstractOrcColumnVector.createFlinkVector;
import static org.apache.flink.orc.vector.AbstractOrcColumnVector.createFlinkVectorFromConstant;

/**
 * An ORC reader that produces a stream of {@link ColumnarRowData} records.
 *
 * <p>This class can add extra fields through {@link ColumnBatchFactory}, for example, add partition
 * fields, which can be extracted from path. Therefore, the {@link #getProducedType()} may be
 * different and types of extra fields need to be added.
 */
public class OrcColumnarRowInputFormat<BatchT, SplitT extends FileSourceSplit>
        extends AbstractOrcFileInputFormat<RowData, BatchT, SplitT>
        implements FileBasedStatisticsReportableInputFormat {

    private static final long serialVersionUID = 1L;

    private final ColumnBatchFactory<BatchT, SplitT> batchFactory;
    private final TypeInformation<RowData> producedTypeInfo;

    public OrcColumnarRowInputFormat(
            final OrcShim<BatchT> shim,
            final Configuration hadoopConfig,
            final TypeDescription schema,
            final int[] selectedFields,
            final List<OrcFilters.Predicate> conjunctPredicates,
            final int batchSize,
            final ColumnBatchFactory<BatchT, SplitT> batchFactory,
            TypeInformation<RowData> producedTypeInfo) {
        super(shim, hadoopConfig, schema, selectedFields, conjunctPredicates, batchSize);
        this.batchFactory = batchFactory;
        this.producedTypeInfo = producedTypeInfo;
    }

    @Override
    public OrcReaderBatch<RowData, BatchT> createReaderBatch(
            final SplitT split,
            final OrcVectorizedBatchWrapper<BatchT> orcBatch,
            final Pool.Recycler<OrcReaderBatch<RowData, BatchT>> recycler,
            final int batchSize) {

        final VectorizedColumnBatch flinkColumnBatch =
                batchFactory.create(split, orcBatch.getBatch());
        return new VectorizedColumnReaderBatch<>(orcBatch, flinkColumnBatch, recycler);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return this.producedTypeInfo;
    }

    @Override
    public TableStats reportStatistics(List<Path> files, DataType producedDataType) {
        return OrcFormatStatisticsReportUtil.getTableStatistics(
                files, producedDataType, hadoopConfigWrapper.getHadoopConfig());
    }

    // ------------------------------------------------------------------------

    /** One batch of ORC columnar vectors and Flink column vectors. */
    private static final class VectorizedColumnReaderBatch<BatchT>
            extends OrcReaderBatch<RowData, BatchT> {

        private final VectorizedColumnBatch flinkColumnBatch;
        private final ColumnarRowIterator result;

        VectorizedColumnReaderBatch(
                final OrcVectorizedBatchWrapper<BatchT> orcBatch,
                final VectorizedColumnBatch flinkColumnBatch,
                final Pool.Recycler<OrcReaderBatch<RowData, BatchT>> recycler) {
            super(orcBatch, recycler);
            this.flinkColumnBatch = flinkColumnBatch;
            this.result =
                    new ColumnarRowIterator(new ColumnarRowData(flinkColumnBatch), this::recycle);
        }

        @Override
        public RecordIterator<RowData> convertAndGetIterator(
                final OrcVectorizedBatchWrapper<BatchT> orcBatch, final long startingOffset) {
            // no copying from the ORC column vectors to the Flink columns vectors necessary,
            // because they point to the same data arrays internally design
            int batchSize = orcBatch.size();
            flinkColumnBatch.setNumRows(batchSize);
            result.set(batchSize, startingOffset, 0);
            return result;
        }
    }

    /**
     * Create a partitioned {@link OrcColumnarRowInputFormat}, the partition columns can be
     * generated by split.
     */
    public static <SplitT extends FileSourceSplit>
            OrcColumnarRowInputFormat<VectorizedRowBatch, SplitT> createPartitionedFormat(
                    OrcShim<VectorizedRowBatch> shim,
                    Configuration hadoopConfig,
                    RowType tableType,
                    List<String> partitionKeys,
                    PartitionFieldExtractor<SplitT> extractor,
                    int[] selectedFields,
                    List<OrcFilters.Predicate> conjunctPredicates,
                    int batchSize,
                    Function<RowType, TypeInformation<RowData>> rowTypeInfoFactory) {
        // TODO FLINK-25113 all this partition keys code should be pruned from the orc format,
        //  because now FileSystemTableSource uses FileInfoExtractorBulkFormat for reading partition
        //  keys.

        String[] tableFieldNames = tableType.getFieldNames().toArray(new String[0]);
        LogicalType[] tableFieldTypes = tableType.getChildren().toArray(new LogicalType[0]);
        List<String> orcFieldNames = getNonPartNames(tableFieldNames, partitionKeys);
        int[] orcSelectedFields =
                getSelectedOrcFields(tableFieldNames, selectedFields, orcFieldNames);

        ColumnBatchFactory<VectorizedRowBatch, SplitT> batchGenerator =
                (SplitT split, VectorizedRowBatch rowBatch) -> {
                    // create and initialize the row batch
                    ColumnVector[] vectors = new ColumnVector[selectedFields.length];
                    for (int i = 0; i < vectors.length; i++) {
                        String name = tableFieldNames[selectedFields[i]];
                        LogicalType type = tableFieldTypes[selectedFields[i]];
                        vectors[i] =
                                partitionKeys.contains(name)
                                        ? createFlinkVectorFromConstant(
                                                type,
                                                extractor.extract(split, name, type),
                                                batchSize)
                                        : createFlinkVector(
                                                rowBatch.cols[orcFieldNames.indexOf(name)], type);
                    }
                    return new VectorizedColumnBatch(vectors);
                };

        return new OrcColumnarRowInputFormat<>(
                shim,
                hadoopConfig,
                convertToOrcTypeWithPart(tableFieldNames, tableFieldTypes, partitionKeys),
                orcSelectedFields,
                conjunctPredicates,
                batchSize,
                batchGenerator,
                rowTypeInfoFactory.apply(
                        new RowType(
                                Arrays.stream(selectedFields)
                                        .mapToObj(i -> tableType.getFields().get(i))
                                        .collect(Collectors.toList()))));
    }
}
