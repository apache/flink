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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.utils.PrintUtils;
import org.apache.flink.types.Row;

import org.jline.terminal.Terminal;

import java.util.Collections;
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

    private static final int DEFAULT_COLUMN_WIDTH = 20;
    private static final String CHANGEFLAG_COLUMN_NAME = "+/-";

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

    public void displayStreamResults() throws SqlExecutionException {
        final AtomicInteger receivedRowCount = new AtomicInteger(0);
        Future<?> resultFuture =
                displayResultExecutorService.submit(
                        () -> {
                            printStreamResults(receivedRowCount);
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
                                    + " rows");
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

    public void displayBatchResults() throws SqlExecutionException {
        Future<?> resultFuture =
                displayResultExecutorService.submit(
                        () -> {
                            final List<Row> resultRows = waitBatchResults();
                            PrintUtils.printAsTableauForm(
                                    resultDescriptor.getResultSchema(),
                                    resultRows.iterator(),
                                    terminal.writer());
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
            terminal.writer().println("Query terminated");
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

    private List<Row> waitBatchResults() {
        List<Row> resultRows;
        // take snapshot and make all results in one page
        do {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            TypedResult<Integer> result =
                    sqlExecutor.snapshotResult(
                            sessionId, resultDescriptor.getResultId(), Integer.MAX_VALUE);

            if (result.getType() == TypedResult.ResultType.EOS) {
                resultRows = Collections.emptyList();
                break;
            } else if (result.getType() == TypedResult.ResultType.PAYLOAD) {
                resultRows = sqlExecutor.retrieveResultPage(resultDescriptor.getResultId(), 1);
                break;
            } else {
                // result not retrieved yet
            }
        } while (true);

        return resultRows;
    }

    private void printStreamResults(AtomicInteger receivedRowCount) {
        List<TableColumn> columns = resultDescriptor.getResultSchema().getTableColumns();
        String[] fieldNames =
                Stream.concat(
                                Stream.of(CHANGEFLAG_COLUMN_NAME),
                                columns.stream().map(TableColumn::getName))
                        .toArray(String[]::new);

        int[] colWidths =
                PrintUtils.columnWidthsByType(
                        columns,
                        DEFAULT_COLUMN_WIDTH,
                        CliStrings.NULL_COLUMN,
                        CHANGEFLAG_COLUMN_NAME);
        String borderline = PrintUtils.genBorderLine(colWidths);

        // print filed names
        terminal.writer().println(borderline);
        PrintUtils.printSingleRow(colWidths, fieldNames, terminal.writer());
        terminal.writer().println(borderline);
        terminal.flush();

        while (true) {
            final TypedResult<List<Tuple2<Boolean, Row>>> result =
                    sqlExecutor.retrieveResultChanges(sessionId, resultDescriptor.getResultId());

            switch (result.getType()) {
                case EMPTY:
                    // do nothing
                    break;
                case EOS:
                    if (receivedRowCount.get() > 0) {
                        terminal.writer().println(borderline);
                    }
                    terminal.writer()
                            .println("Received a total of " + receivedRowCount.get() + " rows");
                    terminal.flush();
                    return;
                case PAYLOAD:
                    List<Tuple2<Boolean, Row>> changes = result.getPayload();
                    for (Tuple2<Boolean, Row> change : changes) {
                        final String[] cols = PrintUtils.rowToString(change.f1);
                        String[] row = new String[cols.length + 1];
                        row[0] = change.f0 ? "+" : "-";
                        System.arraycopy(cols, 0, row, 1, cols.length);
                        PrintUtils.printSingleRow(colWidths, row, terminal.writer());
                        receivedRowCount.incrementAndGet();
                    }
                    break;
                default:
                    throw new SqlExecutionException("Unknown result type: " + result.getType());
            }
        }
    }
}
