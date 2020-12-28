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

package org.apache.flink.table.client.cli.utils;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** {@link TableResult} for testing. */
public class TestTableResult implements TableResult {
    private final TableSchema tableSchema;
    private final ResultKind resultKind;
    private final CloseableIterator<Row> data;

    public static final TestTableResult TABLE_RESULT_OK =
            new TestTableResult(
                    ResultKind.SUCCESS,
                    TableSchema.builder().field("result", DataTypes.STRING()).build(),
                    CloseableIterator.adapterForIterator(
                            Collections.singletonList(Row.of("OK")).iterator()));

    public TestTableResult(ResultKind resultKind, TableSchema tableSchema) {
        this(resultKind, tableSchema, CloseableIterator.empty());
    }

    public TestTableResult(
            ResultKind resultKind, TableSchema tableSchema, CloseableIterator<Row> data) {
        this.resultKind = resultKind;
        this.tableSchema = tableSchema;
        this.data = data;
    }

    @Override
    public Optional<JobClient> getJobClient() {
        return Optional.empty();
    }

    @Override
    public void await() throws InterruptedException, ExecutionException {}

    @Override
    public void await(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {}

    @Override
    public TableSchema getTableSchema() {
        return tableSchema;
    }

    @Override
    public ResultKind getResultKind() {
        return resultKind;
    }

    @Override
    public CloseableIterator<Row> collect() {
        return data;
    }

    @Override
    public void print() {
        // do nothing
    }
}
