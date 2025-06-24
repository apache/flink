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
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static org.apache.flink.table.client.cli.CliInputView.InputOperation.BACKSPACE;
import static org.apache.flink.table.client.cli.CliInputView.InputOperation.ENTER;
import static org.apache.flink.table.client.cli.CliInputView.InputOperation.INSERT;
import static org.apache.flink.table.client.cli.CliInputView.InputOperation.LEFT;
import static org.apache.flink.table.client.cli.CliInputView.InputOperation.QUIT;
import static org.apache.flink.table.client.cli.CliInputView.InputOperation.RIGHT;
import static org.jline.keymap.KeyMap.del;
import static org.jline.keymap.KeyMap.esc;
import static org.jline.keymap.KeyMap.key;

/** CLI view for entering a string. */
public class CliInputView extends CliView<CliInputView.InputOperation, String> {

    private final String inputTitle;
    private final Predicate<String> validation;
    private final StringBuilder currentInput;
    private int cursorPos;
    private boolean isError;

    public CliInputView(Terminal terminal, String inputTitle, Predicate<String> validation) {
        super(terminal);

        this.inputTitle = inputTitle;
        this.validation = validation;
        currentInput = new StringBuilder();
        cursorPos = 0;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    protected void init() {
        // nothing to do
    }

    @Override
    protected KeyMap<InputOperation> getKeys() {
        final KeyMap<InputOperation> keys = new KeyMap<>();
        keys.setUnicode(INSERT);
        keys.setAmbiguousTimeout(200); // make ESC quicker
        for (char i = 32; i < 256; i++) {
            keys.bind(INSERT, Character.toString(i));
        }
        keys.bind(LEFT, key(terminal, Capability.key_left));
        keys.bind(RIGHT, key(terminal, Capability.key_right));
        keys.bind(BACKSPACE, del());

        if (TerminalUtils.isPlainTerminal(terminal)) {
            keys.bind(ENTER, "\r", "$");
            keys.bind(QUIT, key(terminal, Capability.key_exit), "!");
        } else {
            keys.bind(ENTER, "\r");
            keys.bind(QUIT, esc());
        }
        return keys;
    }

    @Override
    protected void evaluate(InputOperation operation, String binding) {
        switch (operation) {
            case QUIT:
                close();
                break;
            case INSERT:
                insert(binding);
                break;
            case ENTER:
                submit();
                break;
            case LEFT:
                moveCursorLeft();
                break;
            case RIGHT:
                moveCursorRight();
                break;
            case BACKSPACE:
                deleteLeft();
                break;
        }
    }

    @Override
    protected String getTitle() {
        return CliStrings.INPUT_TITLE;
    }

    @Override
    protected List<AttributedString> computeHeaderLines() {
        return Collections.emptyList();
    }

    @Override
    protected List<AttributedString> computeFooterLines() {
        return Collections.singletonList(CliStrings.INPUT_HELP);
    }

    @Override
    protected List<AttributedString> computeMainHeaderLines() {
        return Collections.emptyList();
    }

    @Override
    protected List<AttributedString> computeMainLines() {
        final List<AttributedString> lines = new ArrayList<>();

        // space
        IntStream.range(0, getVisibleMainHeight() / 2 - 2)
                .forEach((i) -> lines.add(AttributedString.EMPTY));

        // title
        lines.add(new AttributedString(CliStrings.DEFAULT_MARGIN + inputTitle));

        // input line
        final AttributedStringBuilder inputLine = new AttributedStringBuilder();
        inputLine.append(CliStrings.DEFAULT_MARGIN + "> ");
        final String input = currentInput.toString();
        // add string left of cursor
        inputLine.append(currentInput.substring(0, cursorPos));
        inputLine.style(AttributedStyle.DEFAULT.inverse().blink());
        if (cursorPos < input.length()) {
            inputLine.append(input.charAt(cursorPos));
            inputLine.style(AttributedStyle.DEFAULT);
            inputLine.append(input.substring(cursorPos + 1, input.length()));
        } else {
            inputLine.append(' '); // show the cursor at the end
        }

        lines.add(inputLine.toAttributedString());

        // isError
        if (isError) {
            final AttributedStringBuilder errorLine = new AttributedStringBuilder();
            errorLine.style(AttributedStyle.DEFAULT.foreground(AttributedStyle.RED));
            errorLine.append(CliStrings.DEFAULT_MARGIN + CliStrings.INPUT_ERROR);
            lines.add(AttributedString.EMPTY);
            lines.add(errorLine.toAttributedString());
        }

        return lines;
    }

    @Override
    protected void cleanUp() {
        // nothing to do
    }

    // --------------------------------------------------------------------------------------------

    private void insert(String binding) {
        currentInput.insert(cursorPos, binding);
        cursorPos += binding.length();

        // reset view
        resetMainPart();
    }

    private void deleteLeft() {
        if (cursorPos > 0) {
            currentInput.deleteCharAt(cursorPos - 1);
            cursorPos--;
        }

        // reset view
        resetMainPart();
    }

    private void moveCursorLeft() {
        if (cursorPos > 0) {
            cursorPos--;
        }

        // reset view
        resetMainPart();
    }

    private void moveCursorRight() {
        if (cursorPos < currentInput.length()) {
            cursorPos++;
        }

        // reset view
        resetMainPart();
    }

    private void submit() {
        isError = false;
        final String s = currentInput.toString();
        // input cancelled
        if (s.isEmpty()) {
            close();
        }
        // validate and return
        else if (validation.test(s)) {
            close(s);
        }
        // show error
        else {
            isError = true;
            // reset view
            resetMainPart();
        }
    }

    // --------------------------------------------------------------------------------------------

    /** Available operations for this view. */
    public enum InputOperation {
        QUIT, // leave input view
        INSERT, // input
        ENTER, // apply input
        LEFT, // cursor navigation
        RIGHT, // cursor navigation
        BACKSPACE, // delete left
    }
}
