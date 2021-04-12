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

import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.utils.PrintUtils;
import org.apache.flink.types.Row;

import org.jline.terminal.Terminal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Print result in tableau mode. */
public class CliTableauResultView implements AutoCloseable {

    private static final int DEFAULT_COLUMN_WIDTH = 20;
    private static final int BATCH_CACHE_DATA_SIZE = 5000;

    private final Terminal terminal;
    private final Executor sqlExecutor;
    private final String sessionId;
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
    }

    public void displayResults() throws SqlExecutionException {
        final AtomicInteger receivedRowCount = new AtomicInteger(0);
        Future<?> resultFuture =
                displayResultExecutorService.submit(
                        () -> {
                            printResults(receivedRowCount, resultDescriptor.isStreamingMode());
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
            terminal.writer()
                    .println(
                            "Received a total of "
                                    + receivedRowCount.get()
                                    + " "
                                    + getRowTerm(receivedRowCount));
        } catch (CancellationException e) {
            terminal.writer()
                    .println(
                            "Query terminated, received a total of "
                                    + receivedRowCount.get()
                                    + " "
                                    + getRowTerm(receivedRowCount));
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
        terminal.flush();
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

    private void printResults(AtomicInteger receivedRowCount, boolean isStreamingMode) {
        List<Column> columns = resultDescriptor.getResultSchema().getColumns();
        final String[] fieldNames;
        final int[] colWidths;
        final List<Row> change = new ArrayList<>();
        boolean isEndOfStream = false;

        if (isStreamingMode) {
            fieldNames =
                    Stream.concat(
                                    Stream.of(PrintUtils.ROW_KIND_COLUMN),
                                    columns.stream().map(Column::getName))
                            .toArray(String[]::new);
            colWidths =
                    PrintUtils.columnWidthsByType(
                            columns,
                            DEFAULT_COLUMN_WIDTH,
                            PrintUtils.NULL_COLUMN,
                            PrintUtils.ROW_KIND_COLUMN);
            isEndOfStream = getEndOfStreamWhenRetrieveRows(change);
        } else {
            // Retrieve part of data to prevent OOM
            while (change.size() < BATCH_CACHE_DATA_SIZE) {
                isEndOfStream = getEndOfStreamWhenRetrieveRows(change);
                if (isEndOfStream) {
                    break;
                }
            }
            fieldNames = columns.stream().map(Column::getName).toArray(String[]::new);
            colWidths =
                    PrintUtils.columnWidthsByContent(
                            fieldNames,
                            change.stream()
                                    .map(PrintUtils::rowToString)
                                    .collect(Collectors.toList()),
                            PrintUtils.MAX_COLUMN_WIDTH);
        }

        String borderline = PrintUtils.genBorderLine(colWidths);

        // print filed names
        terminal.writer().println(borderline);
        PrintUtils.printSingleRow(colWidths, fieldNames, terminal.writer());
        terminal.writer().println(borderline);
        terminal.flush();

        do {
            displayRows(change, colWidths, isStreamingMode);
            receivedRowCount.addAndGet(change.size());
            change.clear();

            if (isEndOfStream) {
                if (receivedRowCount.get() > 0) {
                    terminal.writer().println(borderline);
                }
                break;
            } else {
                isEndOfStream = getEndOfStreamWhenRetrieveRows(change);
            }
        } while (true);
    }

    /**
     * Retrieve the rows from the {@link Executor}. The return value indicate whether is
     * end-of-stream. The retrieved row is cached in the input container.
     */
    private boolean getEndOfStreamWhenRetrieveRows(final List<Row> change) {
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
                        return true;
                    }
                    break;
                case EOS:
                    return true;
                case PAYLOAD:
                    change.addAll(result.getPayload());
                    return false;
                default:
                    throw new SqlExecutionException("Unknown result type: " + result.getType());
            }
        }
    }

    private void displayRows(List<Row> changes, int[] colWidths, boolean isStreamingMode) {
        for (Row change : changes) {
            final String[] row =
                    PrintUtils.rowToString(change, PrintUtils.NULL_COLUMN, isStreamingMode);
            PrintUtils.printSingleRow(colWidths, row, terminal.writer());
        }
    }

    private String getRowTerm(AtomicInteger receivedRowCount) {
        return receivedRowCount.get() > 1 ? "rows" : "row";
    }
}
