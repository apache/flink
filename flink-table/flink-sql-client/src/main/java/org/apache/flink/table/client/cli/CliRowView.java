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

import org.jline.keymap.KeyMap;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.InfoCmp.Capability;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.jline.keymap.KeyMap.esc;
import static org.jline.keymap.KeyMap.key;

/** CLI view for visualizing a row. */
public class CliRowView extends CliView<CliRowView.RowOperation, Void> {

    private String[] columnNames;

    private String[] columnTypes;

    private String[] row;

    public CliRowView(Terminal terminal, String[] columnNames, String[] columnTypes, String[] row) {
        super(terminal);

        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.row = row;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    protected void init() {
        // nothing to do
    }

    @Override
    protected KeyMap<RowOperation> getKeys() {
        final KeyMap<RowOperation> keys = new KeyMap<>();
        keys.setAmbiguousTimeout(200); // make ESC quicker
        keys.bind(RowOperation.QUIT, "q", "Q", esc());
        keys.bind(RowOperation.UP, "w", "W", key(terminal, Capability.key_up));
        keys.bind(RowOperation.DOWN, "s", "S", key(terminal, Capability.key_down));
        keys.bind(RowOperation.LEFT, "a", "A", key(terminal, Capability.key_left));
        keys.bind(RowOperation.RIGHT, "d", "D", key(terminal, Capability.key_right));
        return keys;
    }

    @Override
    protected void evaluate(RowOperation operation, String binding) {
        switch (operation) {
            case QUIT:
                close();
                break;
            case UP:
                scrollUp();
                break;
            case DOWN:
                scrollDown();
                break;
            case LEFT:
                scrollLeft();
                break;
            case RIGHT:
                scrollRight();
                break;
        }
    }

    @Override
    protected String getTitle() {
        return CliStrings.ROW_HEADER;
    }

    @Override
    protected List<AttributedString> computeHeaderLines() {
        return Collections.singletonList(AttributedString.EMPTY);
    }

    @Override
    protected List<AttributedString> computeFooterLines() {
        return Collections.singletonList(CliStrings.ROW_QUIT);
    }

    @Override
    protected List<AttributedString> computeMainHeaderLines() {
        return Collections.emptyList();
    }

    @Override
    protected List<AttributedString> computeMainLines() {
        final List<AttributedString> lines = new ArrayList<>();

        final AttributedStringBuilder sb = new AttributedStringBuilder();
        IntStream.range(0, row.length)
                .forEach(
                        i -> {
                            final String name = columnNames[i];
                            final String type = columnTypes[i];

                            sb.setLength(0);
                            sb.append(CliStrings.DEFAULT_MARGIN);
                            sb.style(AttributedStyle.BOLD);
                            sb.append(name);
                            sb.append(" (");
                            sb.append(type);
                            sb.append(')');
                            sb.append(':');
                            lines.add(sb.toAttributedString());

                            sb.setLength(0);
                            sb.append(CliStrings.DEFAULT_MARGIN);
                            sb.style(AttributedStyle.DEFAULT);
                            sb.append(row[i]);
                            lines.add(sb.toAttributedString());

                            lines.add(AttributedString.EMPTY);
                        });

        return lines;
    }

    @Override
    protected void cleanUp() {
        // nothing to do
    }

    // --------------------------------------------------------------------------------------------

    /** Available operations for this view. */
    public enum RowOperation {
        QUIT, // leave row view
        UP,
        DOWN,
        LEFT,
        RIGHT
    }
}
