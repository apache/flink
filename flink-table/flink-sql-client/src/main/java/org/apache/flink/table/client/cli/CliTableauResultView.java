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

package org.apache.flink.table.client.cli;

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.utils.PrintUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.jline.terminal.Terminal;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/** Print result in tableau mode. */
public class CliTableauResultView implements AutoCloseable {

    private final Terminal terminal;
    private final Executor sqlExecutor;
    private final String sessionId;
    private final ZoneId sessionTimeZone;
    private final ResultDescriptor resultDescriptor;
    private final ExecutorService displayResultExecutorService;

    public CliTableauResultView(
            final Terminal terminal,
            final Executor sqlExecutor,
            final String sessionId,
            final ResultDescriptor resultDescriptor) {
        this.terminal = terminal;
        this.sqlExecutor = sqlExecutor;
        this.sessionId = sessionId;
        this.resultDescriptor = resultDescriptor;
        this.displayResultExecutorService =
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory("CliTableauResultView"));
        this.sessionTimeZone = CliUtils.getSessionTimeZone(sqlExecutor.getSessionConfig(sessionId));
    }

    public void displayResults() throws SqlExecutionException {
        final AtomicInteger receivedRowCount = new AtomicInteger(0);
        Future<?> resultFuture =
                displayResultExecutorService.submit(
                        () -> {
                            if (resultDescriptor.isStreamingMode()) {
                                printStreamingResults(receivedRowCount);
                            } else {
                                printBatchResults(receivedRowCount);
                            }
                        });

        // capture CTRL-C
        terminal.handle(
                Terminal.Signal.INT,
                signal -> {
                    resultFuture.cancel(true);
                });

        boolean cleanUpQuery = true;
        try {
            resultFuture.get();
            cleanUpQuery = false; // job finished successfully
        } catch (CancellationException e) {
            terminal.writer()
                    .println(
                            "Query terminated, received a total of "
                                    + receivedRowCount.get()
                                    + " "
                                    + getRowTerm(receivedRowCount));
            terminal.flush();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof SqlExecutionException) {
                throw (SqlExecutionException) e.getCause();
            }
            throw new SqlExecutionException("unknown exception", e.getCause());
        } catch (InterruptedException e) {
            throw new SqlExecutionException("Query interrupted", e);
        } finally {
            checkAndCleanUpQuery(cleanUpQuery);
        }
    }

    @Override
    public void close() {
        this.displayResultExecutorService.shutdown();
    }

    private void checkAndCleanUpQuery(boolean cleanUpQuery) {
        if (cleanUpQuery) {
            try {
                sqlExecutor.cancelQuery(sessionId, resultDescriptor.getResultId());
            } catch (SqlExecutionException e) {
                // ignore further exceptions
            }
        }
    }

    private void printBatchResults(AtomicInteger receivedRowCount) {
        final List<Row> resultRows = waitBatchResults();
        receivedRowCount.addAndGet(resultRows.size());
        PrintUtils.printAsTableauForm(
                resultDescriptor.getResultSchema(),
                resultRows.iterator(),
                terminal.writer(),
                sessionTimeZone);
    }

    private void printStreamingResults(AtomicInteger receivedRowCount) {
        List<Column> columns = resultDescriptor.getResultSchema().getColumns();
        final String[] fieldNames =
                Stream.concat(
                                Stream.of(PrintUtils.ROW_KIND_COLUMN),
                                columns.stream().map(Column::getName))
                        .toArray(String[]::new);

        final int[] colWidths =
                PrintUtils.columnWidthsByType(
                        columns,
                        resultDescriptor.maxColumnWidth(),
                        PrintUtils.NULL_COLUMN,
                        PrintUtils.ROW_KIND_COLUMN);

        String borderline = PrintUtils.genBorderLine(colWidths);

        // print filed names
        terminal.writer().println(borderline);
        PrintUtils.printSingleRow(colWidths, fieldNames, terminal.writer());
        terminal.writer().println(borderline);
        terminal.flush();

        while (true) {
            final TypedResult<List<Row>> result =
                    sqlExecutor.retrieveResultChanges(sessionId, resultDescriptor.getResultId());

            switch (result.getType()) {
                case EMPTY:
                    try {
                        // prevent busy loop
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        // get ctrl+c from terminal and fallback
                        return;
                    }
                    break;
                case EOS:
                    if (receivedRowCount.get() > 0) {
                        terminal.writer().println(borderline);
                    }
                    String rowTerm = getRowTerm(receivedRowCount);
                    terminal.writer()
                            .println(
                                    "Received a total of "
                                            + receivedRowCount.get()
                                            + " "
                                            + rowTerm);
                    terminal.flush();
                    return;
                case PAYLOAD:
                    List<Row> changes = result.getPayload();
                    for (Row change : changes) {
                        final String[] row =
                                PrintUtils.rowToString(
                                        change,
                                        PrintUtils.NULL_COLUMN,
                                        true,
                                        resultDescriptor.getResultSchema(),
                                        sessionTimeZone);
                        PrintUtils.printSingleRow(colWidths, row, terminal.writer());
                        receivedRowCount.incrementAndGet();
                    }
                    break;
                default:
                    throw new SqlExecutionException("Unknown result type: " + result.getType());
            }
        }
    }

    private List<Row> waitBatchResults() {
        List<Row> resultRows = new ArrayList<>();
        do {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            TypedResult<List<Row>> result =
                    sqlExecutor.retrieveResultChanges(sessionId, resultDescriptor.getResultId());

            if (result.getType() == TypedResult.ResultType.EOS) {
                break;
            } else if (result.getType() == TypedResult.ResultType.PAYLOAD) {
                resultRows.addAll(result.getPayload());
            }
        } while (true);

        return resultRows;
    }

    private String getRowTerm(AtomicInteger receivedRowCount) {
        return receivedRowCount.get() > 1 ? "rows" : "row";
    }
}
