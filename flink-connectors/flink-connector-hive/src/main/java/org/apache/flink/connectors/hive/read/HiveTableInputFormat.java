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

package org.apache.flink.connectors.hive.read;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.CheckpointableInputFormat;
import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.hadoop.common.HadoopInputFormatCommonBase;
import org.apache.flink.connectors.hive.HiveSourceFileEnumerator;
import org.apache.flink.connectors.hive.HiveTablePartition;
import org.apache.flink.connectors.hive.JobConfWrapper;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The HiveTableInputFormat are inspired by the HCatInputFormat and HadoopInputFormatBase. It's used
 * to read from hive partition/non-partition table.
 */
public class HiveTableInputFormat extends HadoopInputFormatCommonBase<RowData, HiveTableInputSplit>
        implements CheckpointableInputFormat<HiveTableInputSplit, Long> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(HiveTableInputFormat.class);

    // schema evolution configs are not available in older versions of IOConstants, let's define
    // them ourselves
    private static final String SCHEMA_EVOLUTION_COLUMNS = "schema.evolution.columns";
    private static final String SCHEMA_EVOLUTION_COLUMNS_TYPES = "schema.evolution.columns.types";

    private final JobConfWrapper jobConf;

    private final String hiveVersion;

    private final List<String> partitionKeys;

    private final DataType[] fieldTypes;

    private final String[] fieldNames;

    // For non-partition hive table, partitions only contains one partition which partitionValues is
    // empty.
    private final List<HiveTablePartition> partitions;

    // indices of fields to be returned, with projection applied (if any)
    private final int[] selectedFields;

    // We should limit the input read count of this splits, null represents no limit.
    private final Long limit;

    private final boolean useMapRedReader;

    private transient long currentReadCount = 0L;

    @VisibleForTesting protected transient SplitReader reader;

    public HiveTableInputFormat(
            JobConf jobConf,
            List<String> partitionKeys,
            DataType[] fieldTypes,
            String[] fieldNames,
            int[] projectedFields,
            Long limit,
            String hiveVersion,
            boolean useMapRedReader,
            List<HiveTablePartition> partitions) {
        super(jobConf.getCredentials());
        this.jobConf = new JobConfWrapper(new JobConf(jobConf));
        this.partitionKeys = partitionKeys;
        this.fieldTypes = fieldTypes;
        this.fieldNames = fieldNames;
        this.limit = limit;
        this.hiveVersion = hiveVersion;
        int rowArity = fieldTypes.length;
        this.selectedFields =
                projectedFields != null ? projectedFields : IntStream.range(0, rowArity).toArray();
        this.useMapRedReader = useMapRedReader;
        this.partitions = checkNotNull(partitions, "partitions can not be null.");
    }

    public JobConf getJobConf() {
        return jobConf.conf();
    }

    @Override
    public void configure(org.apache.flink.configuration.Configuration parameters) {}

    @Override
    public void open(HiveTableInputSplit split) throws IOException {
        HiveTablePartition partition = split.getHiveTablePartition();
        if (!useMapRedReader && useOrcVectorizedRead(partition)) {
            this.reader =
                    new HiveVectorizedOrcSplitReader(
                            hiveVersion,
                            jobConf.conf(),
                            fieldNames,
                            fieldTypes,
                            selectedFields,
                            split);
        } else if (!useMapRedReader && useParquetVectorizedRead(partition)) {
            this.reader =
                    new HiveVectorizedParquetSplitReader(
                            hiveVersion,
                            jobConf.conf(),
                            fieldNames,
                            fieldTypes,
                            selectedFields,
                            split);
        } else {
            JobConf clonedConf = new JobConf(jobConf.conf());
            addSchemaToConf(clonedConf);
            this.reader =
                    new HiveMapredSplitReader(
                            clonedConf,
                            partitionKeys,
                            fieldTypes,
                            selectedFields,
                            split,
                            HiveShimLoader.loadHiveShim(hiveVersion));
        }
        currentReadCount = 0L;
    }

    // Hive readers may rely on the schema info in configuration
    private void addSchemaToConf(JobConf jobConf) {
        // set columns/types -- including partition cols
        List<String> typeStrs =
                Arrays.stream(fieldTypes)
                        .map(t -> HiveTypeUtil.toHiveTypeInfo(t, true).toString())
                        .collect(Collectors.toList());
        jobConf.set(IOConstants.COLUMNS, String.join(",", fieldNames));
        jobConf.set(IOConstants.COLUMNS_TYPES, String.join(",", typeStrs));
        // set schema evolution -- excluding partition cols
        int numNonPartCol = fieldNames.length - partitionKeys.size();
        jobConf.set(
                SCHEMA_EVOLUTION_COLUMNS,
                String.join(",", Arrays.copyOfRange(fieldNames, 0, numNonPartCol)));
        jobConf.set(
                SCHEMA_EVOLUTION_COLUMNS_TYPES,
                String.join(",", typeStrs.subList(0, numNonPartCol)));

        // in older versions, parquet reader also expects the selected col indices in conf,
        // excluding part cols
        String readColIDs =
                Arrays.stream(selectedFields)
                        .filter(i -> i < numNonPartCol)
                        .mapToObj(String::valueOf)
                        .collect(Collectors.joining(","));
        jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, readColIDs);
    }

    @Override
    public void reopen(HiveTableInputSplit split, Long state) throws IOException {
        this.open(split);
        this.currentReadCount = state;
        this.reader.seekToRow(state, new GenericRowData(selectedFields.length));
    }

    @Override
    public Long getCurrentState() {
        return currentReadCount;
    }

    private static boolean isVectorizationUnsupported(LogicalType t) {
        switch (t.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BOOLEAN:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return false;
            case TIMESTAMP_WITH_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case ARRAY:
            case MULTISET:
            case MAP:
            case ROW:
            case DISTINCT_TYPE:
            case STRUCTURED_TYPE:
            case NULL:
            case RAW:
            case SYMBOL:
            default:
                return true;
        }
    }

    private boolean useParquetVectorizedRead(HiveTablePartition partition) {
        boolean isParquet =
                partition
                        .getStorageDescriptor()
                        .getSerdeInfo()
                        .getSerializationLib()
                        .toLowerCase()
                        .contains("parquet");
        if (!isParquet) {
            return false;
        }

        for (int i : selectedFields) {
            if (isVectorizationUnsupported(fieldTypes[i].getLogicalType())) {
                LOG.info(
                        "Fallback to hadoop mapred reader, unsupported field type: "
                                + fieldTypes[i]);
                return false;
            }
        }

        LOG.info("Use flink parquet ColumnarRowData reader.");
        return true;
    }

    private boolean useOrcVectorizedRead(HiveTablePartition partition) {
        boolean isOrc =
                partition
                        .getStorageDescriptor()
                        .getSerdeInfo()
                        .getSerializationLib()
                        .toLowerCase()
                        .contains("orc");
        if (!isOrc) {
            return false;
        }

        for (int i : selectedFields) {
            if (isVectorizationUnsupported(fieldTypes[i].getLogicalType())) {
                LOG.info(
                        "Fallback to hadoop mapred reader, unsupported field type: "
                                + fieldTypes[i]);
                return false;
            }
        }

        LOG.info("Use flink orc ColumnarRowData reader.");
        return true;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        if (limit != null && currentReadCount >= limit) {
            return true;
        } else {
            return reader.reachedEnd();
        }
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        currentReadCount++;
        return reader.nextRecord(reuse);
    }

    @Override
    public void close() throws IOException {
        if (this.reader != null) {
            this.reader.close();
            this.reader = null;
        }
    }

    @Override
    public HiveTableInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        return createInputSplits(minNumSplits, partitions, jobConf.conf());
    }

    public static HiveTableInputSplit[] createInputSplits(
            int minNumSplits, List<HiveTablePartition> partitions, JobConf jobConf)
            throws IOException {
        List<HiveTableInputSplit> hiveSplits = new ArrayList<>();
        int splitNum = 0;
        for (HiveTablePartition partition : partitions) {
            for (InputSplit inputSplit :
                    HiveSourceFileEnumerator.createMRSplits(
                            minNumSplits, partition.getStorageDescriptor(), jobConf)) {
                hiveSplits.add(new HiveTableInputSplit(splitNum++, inputSplit, jobConf, partition));
            }
        }

        return hiveSplits.toArray(new HiveTableInputSplit[0]);
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStats) {
        // no statistics available
        return null;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(HiveTableInputSplit[] inputSplits) {
        return new LocatableInputSplitAssigner(inputSplits);
    }
}
