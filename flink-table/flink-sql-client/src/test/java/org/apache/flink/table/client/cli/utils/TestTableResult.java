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
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** {@link TableResult} for testing. */
public class TestTableResult implements TableResult {
    private final JobClient jobClient;
    private final ResolvedSchema resolvedSchema;
    private final ResultKind resultKind;
    private final CloseableIterator<Row> data;

    public static final TestTableResult TABLE_RESULT_OK =
            new TestTableResult(
                    ResultKind.SUCCESS,
                    ResolvedSchema.of(Column.physical("result", DataTypes.STRING())),
                    CloseableIterator.adapterForIterator(
                            Collections.singletonList(Row.of("OK")).iterator()));

    public TestTableResult(ResultKind resultKind, ResolvedSchema resolvedSchema) {
        this(resultKind, resolvedSchema, CloseableIterator.empty());
    }

    public TestTableResult(
            ResultKind resultKind, ResolvedSchema resolvedSchema, CloseableIterator<Row> data) {
        this(null, resultKind, resolvedSchema, data);
    }

    public TestTableResult(
            JobClient jobClient,
            ResultKind resultKind,
            ResolvedSchema resolvedSchema,
            CloseableIterator<Row> data) {
        this.jobClient = jobClient;
        this.resultKind = resultKind;
        this.resolvedSchema = resolvedSchema;
        this.data = data;
    }

    @Override
    public Optional<JobClient> getJobClient() {
        return Optional.ofNullable(jobClient);
    }

    @Override
    public void await() throws InterruptedException {
        Thread.sleep(60000);
    }

    @Override
    public void await(long timeout, TimeUnit unit) {}

    @Override
    public ResolvedSchema getResolvedSchema() {
        return resolvedSchema;
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
