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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.utils.print.PrintStyle;
import org.apache.flink.table.utils.print.RowDataToStringConverter;
import org.apache.flink.table.utils.print.TableauStyle;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Implementation for {@link TableResult}. */
@Internal
public class TableResultImpl implements TableResultInternal {

    private final JobClient jobClient;
    private final ResolvedSchema resolvedSchema;
    private final ResultKind resultKind;
    private final ResultProvider resultProvider;
    private final PrintStyle printStyle;

    private TableResultImpl(
            @Nullable JobClient jobClient,
            ResolvedSchema resolvedSchema,
            ResultKind resultKind,
            ResultProvider resultProvider,
            PrintStyle printStyle) {
        this.jobClient = jobClient;
        this.resolvedSchema =
                Preconditions.checkNotNull(resolvedSchema, "resolvedSchema should not be null");
        this.resultKind = Preconditions.checkNotNull(resultKind, "resultKind should not be null");
        Preconditions.checkNotNull(resultProvider, "result provider should not be null");
        this.resultProvider = resultProvider;
        this.printStyle = Preconditions.checkNotNull(printStyle, "printStyle should not be null");
    }

    @Override
    public Optional<JobClient> getJobClient() {
        return Optional.ofNullable(jobClient);
    }

    @Override
    public void await() throws InterruptedException, ExecutionException {
        try {
            awaitInternal(-1, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            // do nothing
        }
    }

    @Override
    public void await(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        awaitInternal(timeout, unit);
    }

    private void awaitInternal(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (jobClient == null) {
            return;
        }

        ExecutorService executor =
                Executors.newFixedThreadPool(1, r -> new Thread(r, "TableResult-await-thread"));
        try {
            CompletableFuture<Void> future =
                    CompletableFuture.runAsync(
                            () -> {
                                while (!resultProvider.isFirstRowReady()) {
                                    try {
                                        Thread.sleep(100);
                                    } catch (InterruptedException e) {
                                        throw new TableException("Thread is interrupted");
                                    }
                                }
                            },
                            executor);

            if (timeout >= 0) {
                future.get(timeout, unit);
            } else {
                future.get();
            }
        } finally {
            executor.shutdown();
        }
    }

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
        return resultProvider.toExternalIterator();
    }

    @Override
    public CloseableIterator<RowData> collectInternal() {
        return resultProvider.toInternalIterator();
    }

    @Override
    public RowDataToStringConverter getRowDataToStringConverter() {
        return resultProvider.getRowDataStringConverter();
    }

    @Override
    public void print() {
        Iterator<RowData> it = resultProvider.toInternalIterator();
        printStyle.print(it, new PrintWriter(System.out));
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for creating a {@link TableResultImpl}. */
    @Internal
    public static class Builder {
        private JobClient jobClient = null;
        private ResolvedSchema resolvedSchema = null;
        private ResultKind resultKind = null;
        private ResultProvider resultProvider = null;
        private PrintStyle printStyle = null;

        private Builder() {}

        /**
         * Specifies job client which associates the submitted Flink job.
         *
         * @param jobClient a {@link JobClient} for the submitted Flink job.
         */
        public Builder jobClient(JobClient jobClient) {
            this.jobClient = jobClient;
            return this;
        }

        /**
         * Specifies schema of the execution result.
         *
         * @param resolvedSchema a {@link ResolvedSchema} for the execution result.
         */
        public Builder schema(ResolvedSchema resolvedSchema) {
            Preconditions.checkNotNull(resolvedSchema, "resolvedSchema should not be null");
            this.resolvedSchema = resolvedSchema;
            return this;
        }

        /**
         * Specifies result kind of the execution result.
         *
         * @param resultKind a {@link ResultKind} for the execution result.
         */
        public Builder resultKind(ResultKind resultKind) {
            Preconditions.checkNotNull(resultKind, "resultKind should not be null");
            this.resultKind = resultKind;
            return this;
        }

        public Builder resultProvider(ResultProvider resultProvider) {
            Preconditions.checkNotNull(resultProvider, "resultProvider should not be null");
            this.resultProvider = resultProvider;
            return this;
        }

        /**
         * Specifies an row list as the execution result.
         *
         * @param rowList a row list as the execution result.
         */
        public Builder data(List<Row> rowList) {
            Preconditions.checkNotNull(rowList, "listRows should not be null");
            this.resultProvider = new StaticResultProvider(rowList);
            return this;
        }

        /** Specifies print style. Default is {@link TableauStyle} with max integer column width. */
        public Builder setPrintStyle(PrintStyle printStyle) {
            Preconditions.checkNotNull(printStyle, "printStyle should not be null");
            this.printStyle = printStyle;
            return this;
        }

        /** Returns a {@link TableResult} instance. */
        public TableResultInternal build() {
            if (printStyle == null) {
                printStyle = PrintStyle.rawContent(resultProvider.getRowDataStringConverter());
            }
            return new TableResultImpl(
                    jobClient, resolvedSchema, resultKind, resultProvider, printStyle);
        }
    }
}
