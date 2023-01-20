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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.client.gateway.result.ChangelogResult;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.utils.print.PrintStyle;
import org.apache.flink.table.utils.print.TableauStyle;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.jline.terminal.Terminal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/** Print result in tableau mode. */
public class CliTableauResultView implements AutoCloseable {

    private final Terminal terminal;
    private final ResultDescriptor resultDescriptor;

    private final ChangelogResult collectResult;
    private final ExecutorService displayResultExecutorService;

    public CliTableauResultView(final Terminal terminal, final ResultDescriptor resultDescriptor) {
        this(terminal, resultDescriptor, resultDescriptor.createResult());
    }

    @VisibleForTesting
    public CliTableauResultView(
            final Terminal terminal,
            final ResultDescriptor resultDescriptor,
            final ChangelogResult collectResult) {
        this.terminal = terminal;
        this.resultDescriptor = resultDescriptor;
        this.collectResult = collectResult;
        this.displayResultExecutorService =
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory("CliTableauResultView"));
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
            collectResult.close();
        }
    }

    private void printBatchResults(AtomicInteger receivedRowCount) {
        final List<RowData> resultRows = waitBatchResults();
        receivedRowCount.addAndGet(resultRows.size());
        TableauStyle style =
                PrintStyle.tableauWithDataInferredColumnWidths(
                        resultDescriptor.getResultSchema(),
                        resultDescriptor.getRowDataStringConverter(),
                        resultDescriptor.maxColumnWidth(),
                        false,
                        false);
        style.print(resultRows.iterator(), terminal.writer());
    }

    private void printStreamingResults(AtomicInteger receivedRowCount) {
        TableauStyle style =
                PrintStyle.tableauWithTypeInferredColumnWidths(
                        resultDescriptor.getResultSchema(),
                        resultDescriptor.getRowDataStringConverter(),
                        resultDescriptor.maxColumnWidth(),
                        false,
                        true);

        // print filed names
        style.printBorderLine(terminal.writer());
        style.printColumnNamesTableauRow(terminal.writer());
        style.printBorderLine(terminal.writer());
        terminal.flush();

        while (true) {
            final TypedResult<List<RowData>> result = collectResult.retrieveChanges();

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
                        style.printBorderLine(terminal.writer());
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
                    List<RowData> changes = result.getPayload();
                    for (RowData change : changes) {
                        if (Thread.currentThread().isInterrupted()) {
                            return;
                        }
                        style.printTableauRow(style.rowFieldsToString(change), terminal.writer());
                        receivedRowCount.incrementAndGet();
                    }
                    break;
                default:
                    throw new SqlExecutionException("Unknown result type: " + result.getType());
            }
        }
    }

    private List<RowData> waitBatchResults() {
        List<RowData> resultRows = new ArrayList<>();
        do {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            TypedResult<List<RowData>> result = collectResult.retrieveChanges();

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
