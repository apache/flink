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
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.utils.PrintUtils;
import org.apache.flink.types.Row;

import org.jline.keymap.KeyMap;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.InfoCmp.Capability;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.client.cli.CliUtils.TIME_FORMATTER;
import static org.apache.flink.table.client.cli.CliUtils.formatTwoLineHelpOptions;
import static org.apache.flink.table.client.cli.CliUtils.normalizeColumn;
import static org.apache.flink.table.client.cli.CliUtils.repeatChar;
import static org.jline.keymap.KeyMap.ctrl;
import static org.jline.keymap.KeyMap.esc;
import static org.jline.keymap.KeyMap.key;

/** CLI view for retrieving and displaying a table. */
public class CliTableResultView extends CliResultView<CliTableResultView.ResultTableOperation> {

    private int pageCount;
    private int page;
    private LocalTime lastRetrieval;
    private int previousResultsPage;

    private static final int DEFAULT_REFRESH_INTERVAL = 3; // every 1s
    private static final int MIN_REFRESH_INTERVAL = 1; // every 100ms
    private static final int LAST_PAGE = 0;

    public CliTableResultView(CliClient client, ResultDescriptor resultDescriptor) {
        super(client, resultDescriptor);

        refreshInterval = DEFAULT_REFRESH_INTERVAL;
        pageCount = 1;
        page = LAST_PAGE;

        previousResults = Collections.emptyList();
        previousResultsPage = 1;
        results = Collections.emptyList();
    }

    // --------------------------------------------------------------------------------------------

    @Override
    protected String[] getRow(String[] resultRow) {
        return resultRow;
    }

    @Override
    protected int computeColumnWidth(int idx) {
        return MAX_COLUMN_WIDTH;
    }

    @Override
    protected void refresh() {
        // take snapshot
        TypedResult<Integer> result;
        try {
            result =
                    client.getExecutor()
                            .snapshotResult(
                                    client.getSessionId(),
                                    resultDescriptor.getResultId(),
                                    getVisibleMainHeight());
        } catch (SqlExecutionException e) {
            close(e);
            return;
        }

        // stop retrieval if job is done
        if (result.getType() == TypedResult.ResultType.EOS) {
            stopRetrieval(false);
        }
        // update page
        else if (result.getType() == TypedResult.ResultType.PAYLOAD) {
            int newPageCount = result.getPayload();
            pageCount = newPageCount;
            if (page > newPageCount) {
                page = LAST_PAGE;
            }
            updatePage();
        }

        lastRetrieval = LocalTime.now();

        // reset view
        resetAllParts();
    }

    @Override
    protected KeyMap<ResultTableOperation> getKeys() {
        final KeyMap<ResultTableOperation> keys = new KeyMap<>();
        keys.setAmbiguousTimeout(200); // make ESC quicker
        keys.bind(ResultTableOperation.QUIT, "q", "Q", esc(), ctrl('c'));
        keys.bind(
                ResultTableOperation.REFRESH,
                "r",
                "R",
                key(client.getTerminal(), Capability.key_f5));
        keys.bind(ResultTableOperation.UP, "w", "W", key(client.getTerminal(), Capability.key_up));
        keys.bind(
                ResultTableOperation.DOWN,
                "s",
                "S",
                key(client.getTerminal(), Capability.key_down));
        keys.bind(
                ResultTableOperation.LEFT,
                "a",
                "A",
                key(client.getTerminal(), Capability.key_left));
        keys.bind(
                ResultTableOperation.RIGHT,
                "d",
                "D",
                key(client.getTerminal(), Capability.key_right));
        keys.bind(ResultTableOperation.OPEN, "o", "O", "\r");
        keys.bind(ResultTableOperation.GOTO, "g", "G");
        keys.bind(ResultTableOperation.NEXT, "n", "N");
        keys.bind(ResultTableOperation.PREV, "p", "P");
        keys.bind(
                ResultTableOperation.LAST, "l", "L", key(client.getTerminal(), Capability.key_end));
        keys.bind(ResultTableOperation.INC_REFRESH, "+");
        keys.bind(ResultTableOperation.DEC_REFRESH, "-");
        return keys;
    }

    @Override
    protected void evaluate(ResultTableOperation operation, String binding) {
        switch (operation) {
            case QUIT:
                close();
                break;
            case REFRESH:
                refresh();
                break;
            case UP:
                selectRowUp();
                break;
            case DOWN:
                selectRowDown();
                break;
            case OPEN:
                openRow();
                break;
            case GOTO:
                gotoPage();
                break;
            case NEXT:
                gotoNextPage();
                break;
            case PREV:
                gotoPreviousPage();
                break;
            case LAST:
                gotoLastPage();
                break;
            case LEFT:
                scrollLeft();
                break;
            case RIGHT:
                scrollRight();
                break;
            case INC_REFRESH:
                increaseRefreshInterval();
                break;
            case DEC_REFRESH:
                decreaseRefreshInterval(MIN_REFRESH_INTERVAL);
                break;
        }
    }

    @Override
    protected String getTitle() {
        return CliStrings.RESULT_TITLE + " (" + CliStrings.RESULT_TABLE + ")";
    }

    @Override
    protected List<AttributedString> computeHeaderLines() {
        final AttributedStringBuilder statusLine = new AttributedStringBuilder();
        statusLine.style(AttributedStyle.INVERSE);
        // left
        final String left;
        if (isRetrieving()) {
            left =
                    CliStrings.DEFAULT_MARGIN
                            + CliStrings.RESULT_REFRESH_INTERVAL
                            + ' '
                            + REFRESH_INTERVALS.get(refreshInterval).f0;
        } else {
            left = CliStrings.DEFAULT_MARGIN + CliStrings.RESULT_STOPPED;
        }
        // middle
        final StringBuilder middleBuilder = new StringBuilder();
        middleBuilder.append(CliStrings.RESULT_PAGE);
        middleBuilder.append(' ');
        if (page == LAST_PAGE) {
            middleBuilder.append(CliStrings.RESULT_LAST_PAGE);
        } else {
            middleBuilder.append(page);
        }
        middleBuilder.append(CliStrings.RESULT_PAGE_OF);
        middleBuilder.append(pageCount);
        final String middle = middleBuilder.toString();
        // right
        final String right;
        if (lastRetrieval == null) {
            right =
                    CliStrings.RESULT_LAST_REFRESH
                            + ' '
                            + CliStrings.RESULT_REFRESH_UNKNOWN
                            + CliStrings.DEFAULT_MARGIN;
        } else {
            right =
                    CliStrings.RESULT_LAST_REFRESH
                            + ' '
                            + lastRetrieval.format(TIME_FORMATTER)
                            + CliStrings.DEFAULT_MARGIN;
        }
        // all together
        final int totalLeftSpace = getWidth() - middle.length();
        final int leftSpace = totalLeftSpace / 2 - left.length();
        statusLine.append(left);
        repeatChar(statusLine, ' ', leftSpace);
        statusLine.append(middle);
        final int rightSpacing = getWidth() - statusLine.length() - right.length();
        repeatChar(statusLine, ' ', rightSpacing);
        statusLine.append(right);

        return Arrays.asList(statusLine.toAttributedString(), AttributedString.EMPTY);
    }

    @Override
    protected List<AttributedString> computeMainHeaderLines() {
        final AttributedStringBuilder schemaHeader = new AttributedStringBuilder();

        resultDescriptor
                .getResultSchema()
                .getColumnNames()
                .forEach(
                        s -> {
                            schemaHeader.append(' ');
                            schemaHeader.style(AttributedStyle.DEFAULT.underline());
                            normalizeColumn(schemaHeader, s, MAX_COLUMN_WIDTH);
                            schemaHeader.style(AttributedStyle.DEFAULT);
                        });

        return Collections.singletonList(schemaHeader.toAttributedString());
    }

    @Override
    protected List<AttributedString> computeFooterLines() {
        return formatTwoLineHelpOptions(getWidth(), getHelpOptions());
    }

    // --------------------------------------------------------------------------------------------

    private void updatePage() {
        // retrieve page
        final int retrievalPage = page == LAST_PAGE ? pageCount : page;
        final List<Row> rows;
        try {
            rows =
                    client.getExecutor()
                            .retrieveResultPage(resultDescriptor.getResultId(), retrievalPage);
        } catch (SqlExecutionException e) {
            close(e);
            return;
        }

        // convert page
        final List<String[]> stringRows =
                rows.stream().map(PrintUtils::rowToString).collect(Collectors.toList());

        // update results
        if (previousResultsPage == retrievalPage) {
            // only use the previous results if the current page number has not changed
            // this allows for updated results when the key space remains constant
            previousResults = results;
        } else {
            previousResults = null;
            previousResultsPage = retrievalPage;
        }

        results = stringRows;

        // check if selected row is still valid
        if (selectedRow != NO_ROW_SELECTED) {
            if (selectedRow >= results.size()) {
                selectedRow = NO_ROW_SELECTED;
            }
        }

        // reset view
        resetAllParts();
    }

    private List<Tuple2<String, String>> getHelpOptions() {
        final List<Tuple2<String, String>> options = new ArrayList<>();

        options.add(Tuple2.of("Q", CliStrings.RESULT_QUIT));
        options.add(Tuple2.of("R", CliStrings.RESULT_REFRESH));

        options.add(Tuple2.of("+", CliStrings.RESULT_INC_REFRESH));
        options.add(Tuple2.of("-", CliStrings.RESULT_DEC_REFRESH));

        options.add(Tuple2.of("G", CliStrings.RESULT_GOTO));
        options.add(Tuple2.of("L", CliStrings.RESULT_LAST));

        options.add(Tuple2.of("N", CliStrings.RESULT_NEXT));
        options.add(Tuple2.of("P", CliStrings.RESULT_PREV));

        options.add(Tuple2.of("O", CliStrings.RESULT_OPEN));

        return options;
    }

    private void gotoPage() {
        final CliInputView view =
                new CliInputView(
                        client,
                        CliStrings.INPUT_ENTER_PAGE + " [1 to " + pageCount + "]",
                        (s) -> {
                            // validate input
                            final int newPage;
                            try {
                                newPage = Integer.parseInt(s);
                            } catch (NumberFormatException e) {
                                return false;
                            }
                            return newPage > 0 && newPage <= pageCount;
                        });
        view.open(); // enter view
        if (view.getResult() != null) {
            page = Integer.parseInt(view.getResult());
            updatePage();
        }
    }

    private void gotoNextPage() {
        final int curPageIndex = page == LAST_PAGE ? pageCount : page;
        if (curPageIndex < pageCount) {
            page = curPageIndex + 1;
        }
        updatePage();
    }

    private void gotoPreviousPage() {
        final int curPageIndex = page == LAST_PAGE ? pageCount : page;
        if (curPageIndex > 1) {
            page = curPageIndex - 1;
        }
        updatePage();
    }

    private void gotoLastPage() {
        page = LAST_PAGE;
        updatePage();
    }

    // --------------------------------------------------------------------------------------------

    /** Available operations for this view. */
    public enum ResultTableOperation {
        QUIT, // leave view
        REFRESH, // refresh current table page
        UP, // row selection up
        DOWN, // row selection down
        OPEN, // shows a full row
        GOTO, // enter table page number
        NEXT, // next table page
        PREV, // previous table page
        LAST, // last table page
        LEFT, // scroll left if row is large
        RIGHT, // scroll right if row is large
        INC_REFRESH, // increase refresh rate
        DEC_REFRESH, // decrease refresh rate
    }
}
