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
import org.apache.flink.table.client.gateway.SqlExecutionException;

import org.jline.keymap.BindingReader;
import org.jline.keymap.KeyMap;
import org.jline.terminal.Attributes;
import org.jline.terminal.Attributes.LocalFlag;
import org.jline.terminal.Terminal;
import org.jline.terminal.Terminal.Signal;
import org.jline.terminal.Terminal.SignalHandler;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.InfoCmp;
import org.jline.utils.InfoCmp.Capability;

import java.io.IOError;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.table.client.cli.CliUtils.repeatChar;

/**
 * Framework for a CLI view with header, footer, and main part that is scrollable.
 *
 * @param <OP> supported list of operations
 */
public abstract class CliView<OP extends Enum<OP>, OUT> {

    private static final int PLAIN_TERMINAL_WIDTH = 80;

    private static final int PLAIN_TERMINAL_HEIGHT = 30;

    protected final Terminal terminal;

    protected int offsetX;

    protected int offsetY;

    private volatile boolean isRunning;

    private Thread inputThread;

    private int width;

    private int height;

    private final BindingReader keyReader;

    private AttributedString titleLine;

    private List<AttributedString> headerLines;

    private List<AttributedString> mainHeaderLines; // vertically scrollable

    private List<AttributedString> mainLines;

    private List<AttributedString> footerLines;

    private int totalMainWidth;

    private SqlExecutionException executionException;

    private OUT result;

    public CliView(Terminal terminal) {
        this.terminal = terminal;

        this.keyReader = new BindingReader(terminal.reader());
    }

    public void open() {
        isRunning = true;
        inputThread = Thread.currentThread();

        // prepare terminal
        final Tuple2<Attributes, Map<Signal, SignalHandler>> prev = prepareTerminal();
        ensureTerminalFullScreen();
        updateSize();

        init();

        synchronized (this) {
            display();
        }

        final KeyMap<OP> keys = getKeys();

        while (isRunning) {

            final OP operation;
            try {
                operation = keyReader.readBinding(keys, null, true);
            } catch (IOError e) {
                break;
            }

            // refresh loop
            if (operation == null) {
                continue;
            }

            synchronized (this) {
                try {
                    evaluate(operation, keyReader.getLastBinding());
                } catch (SqlExecutionException e) {
                    // in case the evaluate method did not use the close method
                    close(e);
                }

                if (isRunning) {
                    // ensure full-screen again in case a sub-view has been opened in evaluate
                    ensureTerminalFullScreen();

                    display();
                }
            }
        }

        cleanUp();

        // clean terminal
        restoreTerminal(prev);
        unsetTerminalFullScreen();

        if (executionException != null) {
            throw executionException;
        }
    }

    public OUT getResult() {
        return result;
    }

    // --------------------------------------------------------------------------------------------

    protected boolean isRunning() {
        return isRunning;
    }

    protected void close() {
        if (isRunning) {
            isRunning = false;
            // break the input loop if this method is called from another thread
            if (Thread.currentThread() != inputThread) {
                inputThread.interrupt();
            }
        }
    }

    protected void close(SqlExecutionException e) {
        executionException = e;
        close();
    }

    protected void close(OUT result) {
        this.result = result;
        isRunning = false;
    }

    protected void display() {
        // cache
        final List<AttributedString> headerLines = getHeaderLines();
        final List<AttributedString> mainHeaderLines = getMainHeaderLines();
        final List<AttributedString> mainLines = getMainLines();
        final List<AttributedString> footerLines = getFooterLines();
        final int visibleMainHeight = getVisibleMainHeight();
        final int totalMainWidth = getTotalMainWidth();

        // create output
        clearTerminal();

        final List<String> lines = new ArrayList<>();

        // title part
        terminal.writer().println(computeTitleLine().toAnsi());

        // header part
        headerLines.forEach(l -> terminal.writer().println(l.toAnsi()));

        // main part
        // update vertical offset
        if (visibleMainHeight > mainLines.size()) {
            offsetY = 0; // enough space
        } else {
            offsetY = Math.min(mainLines.size() - visibleMainHeight, offsetY); // bound offset
        }
        // update horizontal offset
        if (width > totalMainWidth) {
            offsetX = 0; // enough space
        } else {
            offsetX = Math.min(totalMainWidth - width, offsetX); // bound offset
        }
        // create window
        final List<AttributedString> windowedMainLines =
                mainLines.subList(offsetY, Math.min(mainLines.size(), offsetY + visibleMainHeight));
        // print window
        Stream.concat(mainHeaderLines.stream(), windowedMainLines.stream())
                .forEach(
                        l -> {
                            if (offsetX < l.length()) {
                                final AttributedString windowX =
                                        l.substring(offsetX, Math.min(l.length(), offsetX + width));
                                terminal.writer().println(windowX.toAnsi());
                            } else {
                                terminal.writer().println(); // nothing to show for this line
                            }
                        });

        // footer part
        final int emptyHeight =
                height
                        - 1
                        - headerLines.size()
                        - // -1 = title
                        windowedMainLines.size()
                        - mainHeaderLines.size()
                        - footerLines.size();
        // padding
        IntStream.range(0, emptyHeight).forEach(i -> terminal.writer().println());
        // footer
        IntStream.range(0, footerLines.size())
                .forEach(
                        (i) -> {
                            final AttributedString l = footerLines.get(i);
                            if (i == footerLines.size() - 1) {
                                terminal.writer().print(l.toAnsi());
                            } else {
                                terminal.writer().println(l.toAnsi());
                            }
                        });

        terminal.flush();
    }

    protected void scrollLeft() {
        if (offsetX > 0) {
            offsetX -= 1;
        }
    }

    protected void scrollRight() {
        final int maxOffset = Math.max(0, getTotalMainWidth() - width);
        if (offsetX < maxOffset) {
            offsetX += 1;
        }
    }

    protected void scrollUp() {
        if (offsetY > 0) {
            offsetY -= 1;
        }
    }

    protected void scrollDown() {
        scrollDown(1);
    }

    protected void scrollDown(int n) {
        final int maxOffset = Math.max(0, getMainLines().size() - getVisibleMainHeight());
        offsetY = Math.min(maxOffset, offsetY + n);
    }

    protected int getVisibleMainHeight() {
        // -1 = title line
        return height
                - 1
                - getHeaderLines().size()
                - getMainHeaderLines().size()
                - getFooterLines().size();
    }

    protected List<AttributedString> getHeaderLines() {
        if (headerLines == null) {
            headerLines = computeHeaderLines();
        }
        return headerLines;
    }

    protected List<AttributedString> getMainHeaderLines() {
        if (mainHeaderLines == null) {
            mainHeaderLines = computeMainHeaderLines();
            totalMainWidth = computeTotalMainWidth();
        }
        return mainHeaderLines;
    }

    protected List<AttributedString> getMainLines() {
        if (mainLines == null) {
            mainLines = computeMainLines();
            totalMainWidth = computeTotalMainWidth();
        }
        return mainLines;
    }

    protected List<AttributedString> getFooterLines() {
        if (footerLines == null) {
            footerLines = computeFooterLines();
        }
        return footerLines;
    }

    protected int getTotalMainWidth() {
        if (totalMainWidth <= 0) {
            totalMainWidth = computeTotalMainWidth();
        }
        return totalMainWidth;
    }

    protected AttributedString getTitleLine() {
        if (titleLine == null) {
            titleLine = computeTitleLine();
        }
        return titleLine;
    }

    /** Must be called when values in one or more parts have changed. */
    protected void resetAllParts() {
        titleLine = null;
        headerLines = null;
        mainHeaderLines = null;
        mainLines = null;
        footerLines = null;
        totalMainWidth = 0;
    }

    /** Must be called when values in the main part (main header or main) have changed. */
    protected void resetMainPart() {
        mainHeaderLines = null;
        mainLines = null;
        totalMainWidth = 0;
    }

    protected int getWidth() {
        return width;
    }

    protected int getHeight() {
        return height;
    }

    public void clearTerminal() {
        if (TerminalUtils.isPlainTerminal(terminal)) {
            for (int i = 0; i < 200; i++) { // large number of empty lines
                terminal.writer().println();
            }
        } else {
            terminal.puts(InfoCmp.Capability.clear_screen);
        }
    }

    // --------------------------------------------------------------------------------------------

    public int getTerminalWidth() {
        if (TerminalUtils.isPlainTerminal(terminal)) {
            return PLAIN_TERMINAL_WIDTH;
        }
        return terminal.getWidth();
    }

    public int getTerminalHeight() {
        if (TerminalUtils.isPlainTerminal(terminal)) {
            return PLAIN_TERMINAL_HEIGHT;
        }
        return terminal.getHeight();
    }

    private void updateSize() {
        width = getTerminalWidth();
        height = getTerminalHeight();
        totalMainWidth = width;
        resetAllParts();
    }

    private void ensureTerminalFullScreen() {
        terminal.puts(Capability.enter_ca_mode);
        terminal.puts(Capability.keypad_xmit);
        terminal.puts(Capability.cursor_invisible);
    }

    private Tuple2<Attributes, Map<Signal, SignalHandler>> prepareTerminal() {
        final Attributes prevAttributes = terminal.getAttributes();
        // adopted from org.jline.builtins.Nano
        // see also
        // https://en.wikibooks.org/wiki/Serial_Programming/termios#Basic_Configuration_of_a_Serial_Interface

        // no line processing
        // canonical mode off, echo off, echo newline off, extended input processing off
        Attributes newAttr = new Attributes(prevAttributes);
        newAttr.setLocalFlags(
                EnumSet.of(LocalFlag.ICANON, LocalFlag.ECHO, LocalFlag.IEXTEN), false);
        // turn off input processing
        newAttr.setInputFlags(
                EnumSet.of(
                        Attributes.InputFlag.IXON,
                        Attributes.InputFlag.ICRNL,
                        Attributes.InputFlag.INLCR),
                false);
        // one input byte is enough to return from read, inter-character timer off
        newAttr.setControlChar(Attributes.ControlChar.VMIN, 1);
        newAttr.setControlChar(Attributes.ControlChar.VTIME, 0);
        newAttr.setControlChar(Attributes.ControlChar.VINTR, 0);
        terminal.setAttributes(newAttr);

        final Map<Signal, SignalHandler> prevSignals = new HashMap<>();
        prevSignals.put(Signal.WINCH, terminal.handle(Signal.WINCH, this::handleSignal));
        prevSignals.put(Signal.INT, terminal.handle(Signal.INT, this::handleSignal));
        prevSignals.put(Signal.QUIT, terminal.handle(Signal.QUIT, this::handleSignal));

        return Tuple2.of(prevAttributes, prevSignals);
    }

    private void restoreTerminal(Tuple2<Attributes, Map<Signal, SignalHandler>> prev) {
        terminal.setAttributes(prev.f0);
        prev.f1.forEach(terminal::handle);
    }

    private void unsetTerminalFullScreen() {
        terminal.puts(Capability.exit_ca_mode);
        terminal.puts(Capability.keypad_local);
        terminal.puts(Capability.cursor_visible);
    }

    private int computeTotalMainWidth() {
        final List<AttributedString> mainLines = getMainLines();
        final List<AttributedString> mainHeaderLines = getMainHeaderLines();
        final int max1 = mainLines.stream().mapToInt(AttributedString::length).max().orElse(0);
        final int max2 =
                mainHeaderLines.stream().mapToInt(AttributedString::length).max().orElse(0);
        return Math.max(max1, max2);
    }

    private AttributedString computeTitleLine() {
        final String title = getTitle();
        final AttributedStringBuilder titleLine = new AttributedStringBuilder();
        titleLine.style(AttributedStyle.INVERSE);
        final int totalMargin = width - title.length();
        final int margin = totalMargin / 2;
        repeatChar(titleLine, ' ', margin);
        titleLine.append(title);
        repeatChar(titleLine, ' ', margin + (totalMargin % 2));
        return titleLine.toAttributedString();
    }

    private void handleSignal(Signal signal) {
        synchronized (this) {
            switch (signal) {
                case INT:
                    close(new SqlExecutionException("Forced interrupt."));
                    break;
                case QUIT:
                    close(new SqlExecutionException("Forced cancellation."));
                    break;
                case WINCH:
                    updateSize();
                    if (isRunning) {
                        display();
                    }
                    break;
            }
        }
    }

    // --------------------------------------------------------------------------------------------

    /** Starts threads if necessary. */
    protected abstract void init();

    protected abstract KeyMap<OP> getKeys();

    protected abstract void evaluate(OP operation, String binding);

    protected abstract String getTitle();

    protected abstract List<AttributedString> computeHeaderLines();

    protected abstract List<AttributedString> computeMainHeaderLines();

    protected abstract List<AttributedString> computeMainLines();

    protected abstract List<AttributedString> computeFooterLines();

    protected abstract void cleanUp();
}
