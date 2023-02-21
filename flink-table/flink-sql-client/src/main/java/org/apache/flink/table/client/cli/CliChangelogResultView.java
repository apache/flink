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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.client.gateway.result.ChangelogResult;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.utils.print.PrintStyle;

import org.jline.keymap.KeyMap;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.InfoCmp.Capability;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.flink.table.client.cli.CliUtils.TIME_FORMATTER;
import static org.apache.flink.table.client.cli.CliUtils.formatTwoLineHelpOptions;
import static org.apache.flink.table.client.cli.CliUtils.normalizeColumn;
import static org.apache.flink.table.client.cli.CliUtils.repeatChar;
import static org.jline.keymap.KeyMap.ctrl;
import static org.jline.keymap.KeyMap.esc;
import static org.jline.keymap.KeyMap.key;

/** CLI view for retrieving and displaying a changelog stream. */
public class CliChangelogResultView
        extends CliResultView<CliChangelogResultView.ResultChangelogOperation> {

    private static final int DEFAULT_MAX_ROW_COUNT = 1000;
    private static final int DEFAULT_REFRESH_INTERVAL = 0; // as fast as possible
    private static final int DEFAULT_REFRESH_INTERVAL_PLAIN = 3; // every 1s
    private static final int MIN_REFRESH_INTERVAL = 0; // every 100ms

    private LocalTime lastRetrieval;
    private int scrolling;
    private final ChangelogResult collectResult;

    public CliChangelogResultView(Terminal terminal, ResultDescriptor resultDescriptor) {
        this(terminal, resultDescriptor, resultDescriptor.createResult());
    }

    @VisibleForTesting
    public CliChangelogResultView(
            Terminal terminal, ResultDescriptor resultDescriptor, ChangelogResult collectResult) {
        super(
                terminal,
                resultDescriptor,
                PrintStyle.tableauWithTypeInferredColumnWidths(
                        resultDescriptor.getResultSchema(),
                        resultDescriptor.getRowDataStringConverter(),
                        resultDescriptor.maxColumnWidth(),
                        false,
                        true));
        this.collectResult = collectResult;
        if (TerminalUtils.isPlainTerminal(terminal)) {
            refreshInterval = DEFAULT_REFRESH_INTERVAL_PLAIN;
        } else {
            refreshInterval = DEFAULT_REFRESH_INTERVAL;
        }
        previousResults = null;
        // rows are always appended at the tail and deleted from the head of the list
        results = new LinkedList<>();
    }

    // --------------------------------------------------------------------------------------------

    @Override
    protected String[] getRow(String[] resultRow) {
        return Arrays.copyOfRange(resultRow, 1, resultRow.length);
    }

    @Override
    void cleanUpQuery() {
        collectResult.close();
    }

    @Override
    protected void display() {
        // scroll down before displaying
        if (scrolling > 0) {
            selectedRow = NO_ROW_SELECTED;
        }
        scrollDown(scrolling);
        scrolling = 0;

        super.display();
    }

    @Override
    protected void refresh() {
        // retrieve change record
        final TypedResult<List<RowData>> result;
        try {
            result = collectResult.retrieveChanges();
        } catch (SqlExecutionException e) {
            close(e);
            return;
        }

        // do nothing if result is empty
        switch (result.getType()) {
            case EMPTY:
                // do nothing
                break;
                // stop retrieval if job is done
            case EOS:
                stopRetrieval(false);
                break;
            default:
                List<RowData> changes = result.getPayload();

                for (RowData change : changes) {
                    // convert row
                    final String[] row = tableauStyle.rowFieldsToString(change);

                    // update results

                    // formatting and printing of rows is expensive in the current implementation,
                    // therefore we limit the maximum number of lines shown in changelog mode to
                    // keep the CLI responsive
                    if (results.size() >= DEFAULT_MAX_ROW_COUNT) {
                        results.remove(0);
                    }
                    results.add(row);

                    scrolling++;
                }
                break;
        }

        // reset view
        resetAllParts();

        lastRetrieval = LocalTime.now();
    }

    @Override
    protected KeyMap<ResultChangelogOperation> getKeys() {
        final KeyMap<ResultChangelogOperation> keys = new KeyMap<>();
        keys.setAmbiguousTimeout(200); // make ESC quicker
        keys.bind(ResultChangelogOperation.QUIT, "q", "Q", esc(), ctrl('c'));
        keys.bind(ResultChangelogOperation.REFRESH, "r", "R", key(terminal, Capability.key_f5));
        keys.bind(ResultChangelogOperation.UP, "w", "W", key(terminal, Capability.key_up));
        keys.bind(ResultChangelogOperation.DOWN, "s", "S", key(terminal, Capability.key_down));
        keys.bind(ResultChangelogOperation.LEFT, "a", "A", key(terminal, Capability.key_left));
        keys.bind(ResultChangelogOperation.RIGHT, "d", "D", key(terminal, Capability.key_right));
        keys.bind(ResultChangelogOperation.OPEN, "o", "O", "\r");
        keys.bind(ResultChangelogOperation.INC_REFRESH, "+");
        keys.bind(ResultChangelogOperation.DEC_REFRESH, "-");
        return keys;
    }

    @Override
    protected void evaluate(ResultChangelogOperation operation, String binding) {
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
        return CliStrings.RESULT_TITLE + " (" + CliStrings.RESULT_CHANGELOG + ")";
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
        final int middleSpace = getWidth() - left.length() - right.length();
        statusLine.append(left);
        repeatChar(statusLine, ' ', middleSpace);
        statusLine.append(right);

        return Arrays.asList(statusLine.toAttributedString(), AttributedString.EMPTY);
    }

    @Override
    protected List<AttributedString> computeMainHeaderLines() {
        // add change column
        List<String> columnNames = new ArrayList<>(columnWidths.length);
        columnNames.add("op");
        columnNames.addAll(resultDescriptor.getResultSchema().getColumnNames());

        final AttributedStringBuilder schemaHeader = new AttributedStringBuilder();
        IntStream.range(0, columnNames.size())
                .forEach(
                        idx -> {
                            schemaHeader.style(AttributedStyle.DEFAULT);
                            schemaHeader.append(' ');
                            schemaHeader.style(AttributedStyle.DEFAULT.underline());
                            normalizeColumn(schemaHeader, columnNames.get(idx), columnWidths[idx]);
                        });

        return Collections.singletonList(schemaHeader.toAttributedString());
    }

    @Override
    protected List<AttributedString> computeFooterLines() {
        return formatTwoLineHelpOptions(getWidth(), getHelpOptions());
    }

    // --------------------------------------------------------------------------------------------

    private List<Tuple2<String, String>> getHelpOptions() {
        final List<Tuple2<String, String>> options = new ArrayList<>();

        options.add(Tuple2.of("Q", CliStrings.RESULT_QUIT));
        options.add(Tuple2.of("R", CliStrings.RESULT_REFRESH));

        options.add(Tuple2.of("+", CliStrings.RESULT_INC_REFRESH));
        options.add(Tuple2.of("-", CliStrings.RESULT_DEC_REFRESH));

        options.add(Tuple2.of("O", CliStrings.RESULT_OPEN));

        return options;
    }

    // --------------------------------------------------------------------------------------------

    /** Available operations for this view. */
    public enum ResultChangelogOperation {
        QUIT, // leave view
        REFRESH, // refresh
        UP, // row selection up
        DOWN, // row selection down
        OPEN, // shows a full row
        LEFT, // scroll left if row is large
        RIGHT, // scroll right if row is large
        INC_REFRESH, // increase refresh rate
        DEC_REFRESH, // decrease refresh rate
    }
}
