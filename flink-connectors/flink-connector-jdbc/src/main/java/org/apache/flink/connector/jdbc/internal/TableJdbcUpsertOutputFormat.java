/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.internal;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.executor.InsertOrUpdateJdbcExecutor;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatementImpl;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.function.Function;

import static org.apache.flink.connector.jdbc.utils.JdbcUtils.getPrimaryKey;
import static org.apache.flink.connector.jdbc.utils.JdbcUtils.setRecordToStatement;
import static org.apache.flink.util.Preconditions.checkArgument;

class TableJdbcUpsertOutputFormat
        extends JdbcBatchingOutputFormat<
                Tuple2<Boolean, Row>, Row, JdbcBatchStatementExecutor<Row>> {
    private static final Logger LOG = LoggerFactory.getLogger(TableJdbcUpsertOutputFormat.class);

    private JdbcBatchStatementExecutor<Row> deleteExecutor;
    private final JdbcDmlOptions dmlOptions;

    TableJdbcUpsertOutputFormat(
            JdbcConnectionProvider connectionProvider,
            JdbcDmlOptions dmlOptions,
            JdbcExecutionOptions batchOptions) {
        super(
                connectionProvider,
                batchOptions,
                ctx -> createUpsertRowExecutor(dmlOptions, ctx),
                tuple2 -> tuple2.f1);
        this.dmlOptions = dmlOptions;
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);
        deleteExecutor = createDeleteExecutor();
        try {
            deleteExecutor.prepareStatements(connectionProvider.getConnection());
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    private JdbcBatchStatementExecutor<Row> createDeleteExecutor() {
        int[] pkFields =
                Arrays.stream(dmlOptions.getFieldNames())
                        .mapToInt(Arrays.asList(dmlOptions.getFieldNames())::indexOf)
                        .toArray();
        int[] pkTypes =
                dmlOptions.getFieldTypes() == null
                        ? null
                        : Arrays.stream(pkFields).map(f -> dmlOptions.getFieldTypes()[f]).toArray();
        String deleteSql =
                FieldNamedPreparedStatementImpl.parseNamedStatement(
                        dmlOptions
                                .getDialect()
                                .getDeleteStatement(
                                        dmlOptions.getTableName(), dmlOptions.getFieldNames()),
                        new HashMap<>());
        return createKeyedRowExecutor(pkFields, pkTypes, deleteSql);
    }

    @Override
    protected void addToBatch(Tuple2<Boolean, Row> original, Row extracted) throws SQLException {
        if (original.f0) {
            super.addToBatch(original, extracted);
        } else {
            deleteExecutor.addToBatch(extracted);
        }
    }

    @Override
    public synchronized void close() {
        try {
            super.close();
        } finally {
            try {
                if (deleteExecutor != null) {
                    deleteExecutor.closeStatements();
                }
            } catch (SQLException e) {
                LOG.warn("unable to close delete statement runner", e);
            }
        }
    }

    @Override
    protected void attemptFlush() throws SQLException {
        super.attemptFlush();
        deleteExecutor.executeBatch();
    }

    private static JdbcBatchStatementExecutor<Row> createKeyedRowExecutor(
            int[] pkFields, int[] pkTypes, String sql) {
        return JdbcBatchStatementExecutor.keyed(
                sql,
                createRowKeyExtractor(pkFields),
                (st, record) ->
                        setRecordToStatement(
                                st, pkTypes, createRowKeyExtractor(pkFields).apply(record)));
    }

    private static JdbcBatchStatementExecutor<Row> createUpsertRowExecutor(
            JdbcDmlOptions opt, RuntimeContext ctx) {
        checkArgument(opt.getKeyFields().isPresent());

        int[] pkFields =
                Arrays.stream(opt.getKeyFields().get())
                        .mapToInt(Arrays.asList(opt.getFieldNames())::indexOf)
                        .toArray();
        int[] pkTypes =
                opt.getFieldTypes() == null
                        ? null
                        : Arrays.stream(pkFields).map(f -> opt.getFieldTypes()[f]).toArray();

        return opt.getDialect()
                .getUpsertStatement(
                        opt.getTableName(), opt.getFieldNames(), opt.getKeyFields().get())
                .map(
                        sql ->
                                createSimpleRowExecutor(
                                        parseNamedStatement(sql),
                                        opt.getFieldTypes(),
                                        ctx.getExecutionConfig().isObjectReuseEnabled()))
                .orElseGet(
                        () ->
                                new InsertOrUpdateJdbcExecutor<>(
                                        parseNamedStatement(
                                                opt.getDialect()
                                                        .getRowExistsStatement(
                                                                opt.getTableName(),
                                                                opt.getKeyFields().get())),
                                        parseNamedStatement(
                                                opt.getDialect()
                                                        .getInsertIntoStatement(
                                                                opt.getTableName(),
                                                                opt.getFieldNames())),
                                        parseNamedStatement(
                                                opt.getDialect()
                                                        .getUpdateStatement(
                                                                opt.getTableName(),
                                                                opt.getFieldNames(),
                                                                opt.getKeyFields().get())),
                                        createRowJdbcStatementBuilder(pkTypes),
                                        createRowJdbcStatementBuilder(opt.getFieldTypes()),
                                        createRowJdbcStatementBuilder(opt.getFieldTypes()),
                                        createRowKeyExtractor(pkFields),
                                        ctx.getExecutionConfig().isObjectReuseEnabled()
                                                ? Row::copy
                                                : Function.identity()));
    }

    private static String parseNamedStatement(String statement) {
        return FieldNamedPreparedStatementImpl.parseNamedStatement(statement, new HashMap<>());
    }

    private static Function<Row, Row> createRowKeyExtractor(int[] pkFields) {
        return row -> getPrimaryKey(row, pkFields);
    }
}
