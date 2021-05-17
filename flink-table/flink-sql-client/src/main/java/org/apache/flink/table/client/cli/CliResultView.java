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
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.types.DataType;

import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.client.cli.CliUtils.normalizeColumn;

/** Abstract CLI view for showing results (either as changelog or table). */
public abstract class CliResultView<O extends Enum<O>> extends CliView<O, Void> {

    protected static final int NO_ROW_SELECTED = -1;

    protected static final List<Tuple2<String, Long>> REFRESH_INTERVALS;

    static {
        REFRESH_INTERVALS = new ArrayList<>();
        REFRESH_INTERVALS.add(Tuple2.of("Fastest", 0L));
        REFRESH_INTERVALS.add(Tuple2.of("100 ms", 100L));
        REFRESH_INTERVALS.add(Tuple2.of("500 ms", 100L));
        REFRESH_INTERVALS.add(Tuple2.of("1 s", 1_000L));
        REFRESH_INTERVALS.add(Tuple2.of("5 s", 5_000L));
        REFRESH_INTERVALS.add(Tuple2.of("10 s", 10_000L));
        REFRESH_INTERVALS.add(Tuple2.of("1 min", 60_000L));
        REFRESH_INTERVALS.add(Tuple2.of("-", -1L));
    }

    private final RefreshThread refreshThread;

    protected final ResultDescriptor resultDescriptor;

    protected int refreshInterval;

    protected List<String[]> previousResults;

    protected List<String[]> results;

    protected int selectedRow;

    public CliResultView(CliClient client, ResultDescriptor resultDescriptor) {
        super(client);
        this.resultDescriptor = resultDescriptor;

        refreshThread = new RefreshThread();
        selectedRow = NO_ROW_SELECTED;
    }

    // --------------------------------------------------------------------------------------------

    protected void increaseRefreshInterval() {
        refreshInterval = Math.min(REFRESH_INTERVALS.size() - 1, refreshInterval + 1);

        // reset view
        resetAllParts();

        synchronized (refreshThread) {
            refreshThread.notify();
        }
    }

    protected void decreaseRefreshInterval(int minInterval) {
        refreshInterval = Math.max(minInterval, refreshInterval - 1);

        // reset view
        resetAllParts();

        synchronized (refreshThread) {
            refreshThread.notify();
        }
    }

    protected void selectRowUp() {
        final int visibleRowTop = offsetY;
        if (selectedRow == NO_ROW_SELECTED) {
            if (!getMainLines().isEmpty()) {
                // most bottom visible row
                selectedRow = Math.min(getMainLines().size(), offsetY + getVisibleMainHeight()) - 1;
            }
        }
        // in visible area
        else if (selectedRow > visibleRowTop) {
            selectedRow = selectedRow - 1;
        }
        // not visible, scrolling needed
        else {
            selectedRow = Math.max(0, selectedRow - 1);
            scrollUp();
        }

        // reset view
        resetMainPart();
    }

    protected void selectRowDown() {
        final int visibleRowBottom =
                Math.min(getMainLines().size(), offsetY + getVisibleMainHeight()) - 1;
        if (selectedRow == NO_ROW_SELECTED) {
            selectedRow = offsetY;
        }
        // in visible area
        else if (visibleRowBottom >= 0 && selectedRow < visibleRowBottom) {
            selectedRow = selectedRow + 1;
        }
        // not visible, scrolling needed
        else {
            selectedRow = Math.min(Math.max(0, getMainLines().size() - 1), selectedRow + 1);
            scrollDown();
        }

        // reset view
        resetMainPart();
    }

    protected void openRow() {
        if (selectedRow == NO_ROW_SELECTED) {
            return;
        }
        final CliRowView view =
                new CliRowView(
                        client,
                        resultDescriptor.getResultSchema().getColumnNames().toArray(new String[0]),
                        CliUtils.typesToString(
                                resultDescriptor
                                        .getResultSchema()
                                        .getColumnDataTypes()
                                        .toArray(new DataType[0])),
                        getRow(results.get(selectedRow)));
        view.open(); // enter view
    }

    protected void stopRetrieval(boolean cleanUpQuery) {
        // stop retrieval
        refreshThread.cleanUpQuery = cleanUpQuery;
        refreshThread.isRunning = false;
        synchronized (refreshThread) {
            refreshThread.notify();
        }
    }

    protected boolean isRetrieving() {
        return refreshThread.isRunning;
    }

    // --------------------------------------------------------------------------------------------

    protected abstract void refresh();

    protected abstract int computeColumnWidth(int idx);

    protected abstract String[] getRow(String[] resultRow);

    // --------------------------------------------------------------------------------------------

    @Override
    protected void init() {
        refreshThread.start();
    }

    @Override
    protected List<AttributedString> computeMainLines() {
        final List<AttributedString> lines = new ArrayList<>();

        int lineIdx = 0;
        for (String[] line : results) {
            final AttributedStringBuilder row = new AttributedStringBuilder();

            // highlight selected row
            if (lineIdx == selectedRow) {
                row.style(AttributedStyle.DEFAULT.inverse());
            }

            for (int colIdx = 0; colIdx < line.length; colIdx++) {
                final String col = line[colIdx];
                final int columnWidth = computeColumnWidth(colIdx);

                row.append(' ');
                // check if value was present before last update, if not, highlight it
                // we don't highlight if the retrieval stopped
                // both inverse and bold together do not work correctly
                if (previousResults != null
                        && lineIdx != selectedRow
                        && refreshThread.isRunning
                        && (lineIdx >= previousResults.size()
                                || !col.equals(previousResults.get(lineIdx)[colIdx]))) {
                    row.style(AttributedStyle.BOLD);
                    normalizeColumn(row, col, columnWidth);
                    row.style(AttributedStyle.DEFAULT);
                } else {
                    normalizeColumn(row, col, columnWidth);
                }
            }
            lines.add(row.toAttributedString());

            lineIdx++;
        }

        return lines;
    }

    @Override
    protected void cleanUp() {
        stopRetrieval(true);
        try {
            refreshThread.join();
        } catch (InterruptedException ex) {
            // ignore
        }
    }

    // --------------------------------------------------------------------------------------------

    private class RefreshThread extends Thread {

        public volatile boolean isRunning = true;

        public volatile boolean cleanUpQuery = true;

        public long lastUpdatedResults = System.currentTimeMillis();

        @Override
        public void run() {
            while (isRunning) {
                final long interval = REFRESH_INTERVALS.get(refreshInterval).f1;
                if (interval >= 0) {
                    // refresh according to specified interval
                    if (interval > 0) {
                        synchronized (RefreshThread.this) {
                            if (isRunning) {
                                try {
                                    RefreshThread.this.wait(interval);
                                } catch (InterruptedException e) {
                                    continue;
                                }
                            }
                        }
                    }

                    synchronized (CliResultView.this) {
                        refresh();

                        // do the display only every 100 ms (even in fastest mode)
                        if (System.currentTimeMillis() - lastUpdatedResults > 100) {
                            if (CliResultView.this.isRunning()) {
                                display();
                            }
                            lastUpdatedResults = System.currentTimeMillis();
                        }
                    }
                } else {
                    // keep the thread running but without refreshing
                    synchronized (RefreshThread.this) {
                        if (isRunning) {
                            try {
                                RefreshThread.this.wait(100);
                            } catch (InterruptedException e) {
                                // continue
                            }
                        }
                    }
                }
            }

            // final display
            synchronized (CliResultView.this) {
                if (CliResultView.this.isRunning()) {
                    display();
                }
            }

            if (cleanUpQuery) {
                // cancel table program
                try {
                    // the cancellation happens in the refresh thread in order to keep the main
                    // thread
                    // responsive at all times; esp. if the cluster is not available
                    client.getExecutor()
                            .cancelQuery(client.getSessionId(), resultDescriptor.getResultId());
                } catch (SqlExecutionException e) {
                    // ignore further exceptions
                }
            }
        }
    }
}
