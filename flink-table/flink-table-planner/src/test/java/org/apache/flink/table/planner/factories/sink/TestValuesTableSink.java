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

package org.apache.flink.table.planner.factories.sink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Values {@link DynamicTableSink} for testing. */
public class TestValuesTableSink
        implements DynamicTableSink, SupportsWritingMetadata, SupportsPartitioning {

    private DataType consumedDataType;
    private final int[] primaryKeyIndices;
    private final String tableName;
    private final boolean isInsertOnly;
    private final String runtimeSink;
    private final int expectedNum;
    private final Map<String, DataType> writableMetadata;
    private final Integer parallelism;
    private final ChangelogMode changelogModeEnforced;
    private final int rowtimeIndex;

    public TestValuesTableSink(
            DataType consumedDataType,
            int[] primaryKeyIndices,
            String tableName,
            boolean isInsertOnly,
            String runtimeSink,
            int expectedNum,
            Map<String, DataType> writableMetadata,
            @Nullable Integer parallelism,
            @Nullable ChangelogMode changelogModeEnforced,
            int rowtimeIndex) {
        this.consumedDataType = consumedDataType;
        this.primaryKeyIndices = primaryKeyIndices;
        this.tableName = tableName;
        this.isInsertOnly = isInsertOnly;
        this.runtimeSink = runtimeSink;
        this.expectedNum = expectedNum;
        this.writableMetadata = writableMetadata;
        this.parallelism = parallelism;
        this.changelogModeEnforced = changelogModeEnforced;
        this.rowtimeIndex = rowtimeIndex;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // if param [changelogModeEnforced] is passed in, return it directly
        if (changelogModeEnforced != null) {
            return changelogModeEnforced;
        }
        if (isInsertOnly) {
            return ChangelogMode.insertOnly();
        } else {
            if (primaryKeyIndices.length > 0) {
                // can update on key, ignore UPDATE_BEFORE
                return ChangelogMode.upsert();
            } else {
                // don't have key, works in retract mode
                return requestedMode;
            }
        }
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DataStructureConverter converter = context.createDataStructureConverter(consumedDataType);
        final Boolean isEnforcedInsertOnly =
                Optional.ofNullable(changelogModeEnforced)
                        .map(changelogMode -> changelogMode.equals(ChangelogMode.insertOnly()))
                        .orElse(false);
        final Boolean isInsertOnly = isEnforcedInsertOnly || this.isInsertOnly;
        if (isInsertOnly) {
            return createInsertOnlySinkFunction(converter);
        } else {
            SinkFunction<RowData> sinkFunction;
            if ("SinkWithCollectingWatermark".equals(runtimeSink)) {
                sinkFunction = new CollectionWatermarkSinkFunction(tableName, converter);
            } else {
                // we don't support OutputFormat for updating query in the TestValues connector
                assert runtimeSink.equals("SinkFunction");

                if (primaryKeyIndices.length > 0) {
                    sinkFunction =
                            new KeyedUpsertingSinkFunction(
                                    tableName, converter, primaryKeyIndices, expectedNum);
                } else {
                    Preconditions.checkArgument(
                            expectedNum == -1,
                            "Retracting Sink doesn't support '"
                                    + TestValuesTableFactory.SINK_EXPECTED_MESSAGES_NUM.key()
                                    + "' yet.");
                    sinkFunction = new RetractingSinkFunction(tableName, converter);
                }
            }
            return SinkFunctionProvider.of(sinkFunction);
        }
    }

    private SinkRuntimeProvider createInsertOnlySinkFunction(DataStructureConverter converter) {
        Preconditions.checkArgument(
                expectedNum == -1,
                "Appending Sink doesn't support '"
                        + TestValuesTableFactory.SINK_EXPECTED_MESSAGES_NUM.key()
                        + "' yet.");
        switch (runtimeSink) {
            case "SinkFunction":
                return SinkFunctionProvider.of(
                        new AppendingSinkFunction(tableName, converter, rowtimeIndex), parallelism);
            case "OutputFormat":
                return OutputFormatProvider.of(
                        new AppendingOutputFormat(tableName, converter), parallelism);
            case "DataStream":
                return new DataStreamSinkProvider() {
                    @Override
                    public DataStreamSink<?> consumeDataStream(DataStream<RowData> dataStream) {
                        return dataStream.addSink(
                                new AppendingSinkFunction(tableName, converter, rowtimeIndex));
                    }

                    @Override
                    public Optional<Integer> getParallelism() {
                        return Optional.ofNullable(parallelism);
                    }
                };

            default:
                throw new IllegalArgumentException(
                        "Unsupported runtime sink class: " + runtimeSink);
        }
    }

    @Override
    public DynamicTableSink copy() {
        return new TestValuesTableSink(
                consumedDataType,
                primaryKeyIndices,
                tableName,
                isInsertOnly,
                runtimeSink,
                expectedNum,
                writableMetadata,
                parallelism,
                changelogModeEnforced,
                rowtimeIndex);
    }

    @Override
    public String asSummaryString() {
        return "TestValuesSink";
    }

    @Override
    public Map<String, DataType> listWritableMetadata() {
        return writableMetadata;
    }

    @Override
    public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
        this.consumedDataType = consumedDataType;
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {}

    @Override
    public boolean requiresPartitionGrouping(boolean supportsGrouping) {
        return supportsGrouping;
    }
}
