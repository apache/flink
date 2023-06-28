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
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.cli.parser.SqlClientSyntaxHighlighter;
import org.apache.flink.table.client.cli.parser.SqlCommandParserImpl;
import org.apache.flink.table.client.cli.parser.SqlMultiLineParser;
import org.apache.flink.table.client.config.SqlClientOptions;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SqlExecutionException;

import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.MaskingCallback;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.LineReaderImpl;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Supplier;

/** SQL CLI client. */
public class CliClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CliClient.class);
    public static final Supplier<Terminal> DEFAULT_TERMINAL_FACTORY =
            TerminalUtils::createDefaultTerminal;
    private static final String NEWLINE_PROMPT =
            new AttributedStringBuilder()
                    .style(AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN))
                    .append("Flink SQL")
                    .style(AttributedStyle.DEFAULT)
                    .append("> ")
                    .toAnsi();
    private static final String SHOW_LINE_NUMBERS_PATTERN = "%N%M> ";

    private final Executor executor;

    private final Path historyFilePath;

    private final @Nullable MaskingCallback inputTransformer;

    private final Supplier<Terminal> terminalFactory;

    private Terminal terminal;

    private boolean isRunning;

    /**
     * Creates a CLI instance with a custom terminal. Make sure to close the CLI instance afterwards
     * using {@link #close()}.
     */
    @VisibleForTesting
    public CliClient(
            Supplier<Terminal> terminalFactory,
            Executor executor,
            Path historyFilePath,
            @Nullable MaskingCallback inputTransformer) {
        this.terminalFactory = terminalFactory;
        this.executor = executor;
        this.inputTransformer = inputTransformer;
        this.historyFilePath = historyFilePath;
    }

    /**
     * Creates a CLI instance with a prepared terminal. Make sure to close the CLI instance
     * afterwards using {@link #close()}.
     */
    public CliClient(Supplier<Terminal> terminalFactory, Executor executor, Path historyFilePath) {
        this(terminalFactory, executor, historyFilePath, null);
    }

    /** Closes the CLI instance. */
    public void close() {
        if (terminal != null) {
            closeTerminal();
        }
    }

    /** Opens the interactive CLI shell. */
    public void executeInInteractiveMode() {
        executeInInteractiveMode(null);
    }

    @VisibleForTesting
    void executeInInteractiveMode(LineReader lineReader) {
        try {
            terminal = terminalFactory.get();
            executeInteractive(lineReader);
        } finally {
            closeTerminal();
        }
    }

    /** Opens the non-interactive CLI shell. */
    public void executeInNonInteractiveMode(String content) {
        try {
            terminal = terminalFactory.get();
            executeFile(content, terminal.output(), ExecutionMode.NON_INTERACTIVE_EXECUTION);
        } finally {
            closeTerminal();
        }
    }

    /** Initialize the Cli Client with the content. */
    public boolean executeInitialization(String content) {
        try {
            OutputStream outputStream = new ByteArrayOutputStream(256);
            terminal = TerminalUtils.createDumbTerminal(outputStream);
            boolean success = executeFile(content, outputStream, ExecutionMode.INITIALIZATION);
            LOG.info(outputStream.toString());
            return success;
        } finally {
            closeTerminal();
        }
    }

    // --------------------------------------------------------------------------------------------

    /** Mode of the execution. */
    public enum ExecutionMode {
        INTERACTIVE_EXECUTION,

        NON_INTERACTIVE_EXECUTION,

        INITIALIZATION
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Execute statement from the user input and prints status information and/or errors on the
     * terminal.
     */
    private void executeInteractive(LineReader inputLineReader) {
        // make space from previous output and test the writer
        terminal.writer().println();
        terminal.writer().flush();

        // print welcome
        terminal.writer().append(CliStrings.MESSAGE_WELCOME);

        LineReader lineReader =
                inputLineReader == null
                        ? createLineReader(terminal, ExecutionMode.INTERACTIVE_EXECUTION)
                        : inputLineReader;
        getAndExecuteStatements(lineReader, false);
    }

    private boolean getAndExecuteStatements(LineReader lineReader, boolean exitOnFailure) {
        // begin reading loop
        isRunning = true;
        String line;

        SqlMultiLineParser parser = (SqlMultiLineParser) lineReader.getParser();
        while (isRunning) {
            // make some space to previous command
            terminal.writer().append("\n");
            terminal.flush();
            try {
                // read a statement from terminal and parse it
                line = lineReader.readLine(NEWLINE_PROMPT, null, inputTransformer, null);
                lineReader.setVariable(
                        LineReader.SECONDARY_PROMPT_PATTERN,
                        (executor.getSessionConfig().get(SqlClientOptions.DISPLAY_SHOW_LINE_NUMBERS)
                                ? SHOW_LINE_NUMBERS_PATTERN
                                : LineReaderImpl.DEFAULT_SECONDARY_PROMPT_PATTERN));
                if (line.trim().isEmpty()) {
                    continue;
                }

                Printer printer = parser.getPrinter();
                boolean success = print(printer);
                if (exitOnFailure && !success) {
                    return false;
                }
            } catch (UserInterruptException e) {
                // user cancelled line with Ctrl+C
            } catch (EndOfFileException | IOError e) {
                // user cancelled application with Ctrl+D or kill
                break;
            } catch (SqlExecutionException e) {
                // print the detailed information on about the parse errors in the terminal.
                printExecutionException(e);
                if (exitOnFailure) {
                    return false;
                }
            } catch (Throwable t) {
                throw new SqlClientException("Could not read from command line.", t);
            }
        }
        // finish all statements.
        return true;
    }

    /**
     * Execute content from Sql file and prints status information and/or errors on the terminal.
     *
     * @param content SQL file content
     */
    private boolean executeFile(String content, OutputStream outputStream, ExecutionMode mode) {
        terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_EXECUTE_FILE).toAnsi());

        // append line delimiter
        try (InputStream inputStream =
                        new ByteArrayInputStream(
                                SqlMultiLineParser.formatSqlFile(content).getBytes());
                Terminal dumbTerminal =
                        TerminalUtils.createDumbTerminal(inputStream, outputStream)) {
            LineReader lineReader = createLineReader(dumbTerminal, mode);
            return getAndExecuteStatements(lineReader, true);
        } catch (Throwable e) {
            printExecutionException(e);
            return false;
        }
    }

    private boolean print(Printer printer) {
        try {
            final Thread thread = Thread.currentThread();
            final Terminal.SignalHandler previousHandler =
                    terminal.handle(Terminal.Signal.INT, (signal) -> thread.interrupt());
            try {
                printer.print(terminal);
                if (printer.isQuitCommand()) {
                    isRunning = false;
                }
            } finally {
                terminal.handle(Terminal.Signal.INT, previousHandler);
            }
        } catch (SqlExecutionException e) {
            printExecutionException(e);
            return false;
        }
        return true;
    }

    // --------------------------------------------------------------------------------------------
    // Utils
    // --------------------------------------------------------------------------------------------

    private void printExecutionException(Throwable t) {
        final String errorMessage = CliStrings.MESSAGE_SQL_EXECUTION_ERROR;
        LOG.warn(errorMessage, t);
        boolean isVerbose = executor.getSessionConfig().get(SqlClientOptions.VERBOSE);
        terminal.writer().println(CliStrings.messageError(errorMessage, t, isVerbose).toAnsi());
        terminal.flush();
    }

    private void closeTerminal() {
        try {
            terminal.close();
            terminal = null;
        } catch (IOException e) {
            // ignore
        }
    }

    private LineReader createLineReader(Terminal terminal, ExecutionMode mode) {
        SqlMultiLineParser parser =
                new SqlMultiLineParser(new SqlCommandParserImpl(), executor, mode);

        // initialize line lineReader
        LineReaderBuilder builder =
                LineReaderBuilder.builder()
                        .terminal(terminal)
                        .appName(CliStrings.CLI_NAME)
                        .parser(parser);

        if (mode == ExecutionMode.INTERACTIVE_EXECUTION) {
            builder.completer(new SqlCompleter(executor));
            builder.highlighter(new SqlClientSyntaxHighlighter(executor));
        }
        LineReader lineReader = builder.build();

        // this option is disabled for now for correct backslash escaping
        // a "SELECT '\'" query should return a string with a backslash
        lineReader.option(LineReader.Option.DISABLE_EVENT_EXPANSION, true);
        // set strict "typo" distance between words when doing code completion
        lineReader.setVariable(LineReader.ERRORS, 1);
        // perform code completion case insensitive
        lineReader.option(LineReader.Option.CASE_INSENSITIVE, true);
        // set history file path
        if (Files.exists(historyFilePath) || CliUtils.createFile(historyFilePath)) {
            String msg = "Command history file path: " + historyFilePath;
            // print it in the command line as well as log file
            terminal.writer().println(msg);
            LOG.info(msg);
            lineReader.setVariable(LineReader.HISTORY_FILE, historyFilePath);
        } else {
            String msg = "Unable to create history file: " + historyFilePath;
            terminal.writer().println(msg);
            LOG.warn(msg);
        }
        return lineReader;
    }
}
