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

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.connector.jdbc.split.JdbcNumericBetweenParametersProvider;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatementImpl;
import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;

import static org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link TableSource} for JDBC. */
public class JdbcTableSource
        implements StreamTableSource<Row>, ProjectableTableSource<Row>, LookupableTableSource<Row> {

    private final JdbcOptions options;
    private final JdbcReadOptions readOptions;
    private final JdbcLookupOptions lookupOptions;
    private final TableSchema schema;

    // index of fields selected, null means that all fields are selected
    private final int[] selectFields;
    private final DataType producedDataType;

    private JdbcTableSource(
            JdbcOptions options,
            JdbcReadOptions readOptions,
            JdbcLookupOptions lookupOptions,
            TableSchema schema) {
        this(options, readOptions, lookupOptions, schema, null);
    }

    private JdbcTableSource(
            JdbcOptions options,
            JdbcReadOptions readOptions,
            JdbcLookupOptions lookupOptions,
            TableSchema schema,
            int[] selectFields) {
        this.options = options;
        this.readOptions = readOptions;
        this.lookupOptions = lookupOptions;
        this.schema = schema;

        this.selectFields = selectFields;

        final DataType[] schemaDataTypes = schema.getFieldDataTypes();
        final String[] schemaFieldNames = schema.getFieldNames();
        if (selectFields != null) {
            DataType[] dataTypes = new DataType[selectFields.length];
            String[] fieldNames = new String[selectFields.length];
            for (int i = 0; i < selectFields.length; i++) {
                dataTypes[i] = schemaDataTypes[selectFields[i]];
                fieldNames[i] = schemaFieldNames[selectFields[i]];
            }
            this.producedDataType =
                    TableSchema.builder().fields(fieldNames, dataTypes).build().toRowDataType();
        } else {
            this.producedDataType = schema.toRowDataType();
        }
    }

    @Override
    public boolean isBounded() {
        return true;
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        return execEnv.createInput(
                        getInputFormat(), (RowTypeInfo) fromDataTypeToLegacyInfo(producedDataType))
                .name(explainSource());
    }

    @Override
    public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
        final RowTypeInfo rowTypeInfo = (RowTypeInfo) fromDataTypeToLegacyInfo(producedDataType);
        return JdbcLookupFunction.builder()
                .setOptions(options)
                .setLookupOptions(lookupOptions)
                .setFieldTypes(rowTypeInfo.getFieldTypes())
                .setFieldNames(rowTypeInfo.getFieldNames())
                .setKeyNames(lookupKeys)
                .build();
    }

    @Override
    public DataType getProducedDataType() {
        return producedDataType;
    }

    @Override
    public TableSource<Row> projectFields(int[] fields) {
        return new JdbcTableSource(options, readOptions, lookupOptions, schema, fields);
    }

    @Override
    public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isAsyncEnabled() {
        return false;
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }

    @Override
    public String explainSource() {
        final RowTypeInfo rowTypeInfo = (RowTypeInfo) fromDataTypeToLegacyInfo(producedDataType);
        return TableConnectorUtils.generateRuntimeName(getClass(), rowTypeInfo.getFieldNames());
    }

    public static Builder builder() {
        return new Builder();
    }

    private JdbcInputFormat getInputFormat() {
        final RowTypeInfo rowTypeInfo = (RowTypeInfo) fromDataTypeToLegacyInfo(producedDataType);
        JdbcInputFormat.JdbcInputFormatBuilder builder =
                JdbcInputFormat.buildJdbcInputFormat()
                        .setDrivername(options.getDriverName())
                        .setDBUrl(options.getDbURL())
                        .setRowTypeInfo(
                                new RowTypeInfo(
                                        rowTypeInfo.getFieldTypes(), rowTypeInfo.getFieldNames()));
        options.getUsername().ifPresent(builder::setUsername);
        options.getPassword().ifPresent(builder::setPassword);

        if (readOptions.getFetchSize() != 0) {
            builder.setFetchSize(readOptions.getFetchSize());
        }

        final JdbcDialect dialect = options.getDialect();
        String query = getBaseQueryStatement(rowTypeInfo);
        if (readOptions.getPartitionColumnName().isPresent()) {
            long lowerBound = readOptions.getPartitionLowerBound().get();
            long upperBound = readOptions.getPartitionUpperBound().get();
            int numPartitions = readOptions.getNumPartitions().get();
            builder.setParametersProvider(
                    new JdbcNumericBetweenParametersProvider(lowerBound, upperBound)
                            .ofBatchNum(numPartitions));
            query +=
                    " WHERE "
                            + dialect.quoteIdentifier(readOptions.getPartitionColumnName().get())
                            + " BETWEEN ? AND ?";
        }
        builder.setQuery(query);

        return builder.finish();
    }

    private String getBaseQueryStatement(RowTypeInfo rowTypeInfo) {
        return readOptions
                .getQuery()
                .orElseGet(
                        () ->
                                FieldNamedPreparedStatementImpl.parseNamedStatement(
                                        options.getDialect()
                                                .getSelectFromStatement(
                                                        options.getTableName(),
                                                        rowTypeInfo.getFieldNames(),
                                                        new String[0]),
                                        new HashMap<>()));
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof JdbcTableSource) {
            JdbcTableSource source = (JdbcTableSource) o;
            return Objects.equals(options, source.options)
                    && Objects.equals(readOptions, source.readOptions)
                    && Objects.equals(lookupOptions, source.lookupOptions)
                    && Objects.equals(schema, source.schema)
                    && Arrays.equals(selectFields, source.selectFields);
        } else {
            return false;
        }
    }

    /** Builder for a {@link JdbcTableSource}. */
    public static class Builder {

        private JdbcOptions options;
        private JdbcReadOptions readOptions;
        private JdbcLookupOptions lookupOptions;
        protected TableSchema schema;

        /** required, jdbc options. */
        public Builder setOptions(JdbcOptions options) {
            this.options = options;
            return this;
        }

        /**
         * optional, scan related options. {@link JdbcReadOptions} will be only used for {@link
         * StreamTableSource}.
         */
        public Builder setReadOptions(JdbcReadOptions readOptions) {
            this.readOptions = readOptions;
            return this;
        }

        /**
         * optional, lookup related options. {@link JdbcLookupOptions} only be used for {@link
         * LookupableTableSource}.
         */
        public Builder setLookupOptions(JdbcLookupOptions lookupOptions) {
            this.lookupOptions = lookupOptions;
            return this;
        }

        /** required, table schema of this table source. */
        public Builder setSchema(TableSchema schema) {
            this.schema = JdbcTypeUtil.normalizeTableSchema(schema);
            return this;
        }

        /**
         * Finalizes the configuration and checks validity.
         *
         * @return Configured JdbcTableSource
         */
        public JdbcTableSource build() {
            checkNotNull(options, "No options supplied.");
            checkNotNull(schema, "No schema supplied.");
            if (readOptions == null) {
                readOptions = JdbcReadOptions.builder().build();
            }
            if (lookupOptions == null) {
                lookupOptions = JdbcLookupOptions.builder().build();
            }
            return new JdbcTableSource(options, readOptions, lookupOptions, schema);
        }
    }
}
