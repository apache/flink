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

package org.apache.flink.table.sinks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.sql.Date;
import java.time.LocalDate;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.table.catalog.FileCatalogStoreFactoryOptions.PATH;
import static org.apache.flink.table.sinks.LegacyCsvDynamicTableSinkOptions.FIELD_DELIM;
import static org.apache.flink.table.sinks.LegacyCsvDynamicTableSinkOptions.IDENTIFIER;
import static org.apache.flink.table.sinks.LegacyCsvDynamicTableSinkOptions.NUM_FILES;
import static org.apache.flink.table.sinks.LegacyCsvDynamicTableSinkOptions.WRITE_MODE;

/**
 * This is a legacy CSV connector that shares similarities with {@link CsvTableSink} and utilizes
 * the {@link DynamicTableFactory} stack.
 *
 * <p>Currently, some of the tests that use {@link CsvTableSink} cannot be seamlessly switched to
 * use the FileSystem connector with CSV format. For example, CsvTableSink writes to a single file,
 * and when writing String types, it does not include double quotes, among other differences.
 *
 * @deprecated The legacy CSV connector has been replaced by {@code FileSink}. It is kept only to
 *     support tests for the legacy connector stack.
 */
@Internal
@Deprecated
public class LegacyCsvDynamicTableSinkFactory implements DynamicTableSinkFactory {

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        return new LegacyCsvDynamicTableSink(
                helper.getOptions().get(PATH),
                helper.getOptions().get(FIELD_DELIM),
                helper.getOptions().get(NUM_FILES),
                helper.getOptions().get(WRITE_MODE),
                schema.getColumnNames().toArray(new String[] {}),
                schema.getColumnDataTypes().toArray(new DataType[0]));
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.singleton(PATH);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(FIELD_DELIM);
        set.add(NUM_FILES);
        set.add(WRITE_MODE);
        return set;
    }

    /**
     * @deprecated The legacy CSV connector has been replaced by {@code FileSink}. It is kept only
     *     to support tests for the legacy connector stack.
     */
    @Internal
    @Deprecated
    public static class LegacyCsvDynamicTableSink implements DynamicTableSink {

        private final String path;

        private final String fieldDelim;

        private final int numFiles;

        @Nullable private final FileSystem.WriteMode writeMode;

        private final String[] fieldNames;

        private final DataType[] fieldTypes;

        public LegacyCsvDynamicTableSink(
                String path,
                String fieldDelim,
                int numFiles,
                @Nullable FileSystem.WriteMode writeMode,
                String[] fieldNames,
                DataType[] fieldTypes) {
            this.path = path;
            this.fieldDelim = fieldDelim;
            this.numFiles = numFiles;
            this.writeMode = writeMode;
            this.fieldNames = fieldNames;
            this.fieldTypes = fieldTypes;
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            return ChangelogMode.insertOnly();
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
            return new DataStreamSinkProvider() {
                @Override
                public DataStreamSink<?> consumeDataStream(
                        ProviderContext providerContext, DataStream<RowData> dataStream) {
                    CsvTableSink sink =
                            new CsvTableSink(
                                    path, fieldDelim, numFiles, writeMode, fieldNames, fieldTypes);
                    final SingleOutputStreamOperator<Row> dataStreamWithRow =
                            dataStream.map(new RowDataToRow(fieldTypes));

                    getParallelism().ifPresent(dataStreamWithRow::setParallelism);
                    return sink.consumeDataStream(dataStreamWithRow);
                }

                @Override
                public Optional<Integer> getParallelism() {
                    if (numFiles > 0) {
                        return Optional.of(numFiles);
                    }
                    return DataStreamSinkProvider.super.getParallelism();
                }
            };
        }

        @Override
        public DynamicTableSink copy() {
            return new LegacyCsvDynamicTableSink(
                    path, fieldDelim, numFiles, writeMode, fieldNames, fieldTypes);
        }

        @Override
        public String asSummaryString() {
            return "legacy-csv-sink";
        }

        private static class RowDataToRow implements MapFunction<RowData, Row> {

            private final RowData.FieldGetter[] fieldGetters;

            public RowDataToRow(DataType[] fieldTypes) {
                this.fieldGetters = new RowData.FieldGetter[fieldTypes.length];
                for (int i = 0; i < fieldGetters.length; i++) {
                    fieldGetters[i] = getFieldGetter(fieldTypes[i].getLogicalType(), i);
                }
            }

            @Override
            public Row map(RowData rowData) throws Exception {
                Row row = new Row(rowData.getRowKind(), rowData.getArity());
                for (int i = 0; i < rowData.getArity(); i++) {
                    row.setField(i, fieldGetters[i].getFieldOrNull(rowData));
                }
                return row;
            }

            private RowData.FieldGetter getFieldGetter(LogicalType fieldType, int fieldPos) {
                // keep the legacy behavior to write data
                switch (fieldType.getTypeRoot()) {
                    case DATE:
                        return row -> Date.valueOf(LocalDate.ofEpochDay(row.getInt(fieldPos)));
                    default:
                        return RowData.createFieldGetter(fieldType, fieldPos);
                }
            }
        }
    }
}
