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
import org.apache.flink.api.common.JobID;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.SqlParserEOFException;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.cli.parser.ClientParser;
import org.apache.flink.table.client.cli.parser.SqlMultiLineParser;
import org.apache.flink.table.client.cli.parser.StatementType;
import org.apache.flink.table.client.config.SqlClientOptions;
import org.apache.flink.table.client.gateway.ClientResult;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.utils.print.PrintStyle;

import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.MaskingCallback;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.InfoCmp;
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

import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_DML_SYNC;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_EXECUTE_STATEMENT;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_FINISH_STATEMENT;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_STATEMENT_SUBMITTED;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_SUBMITTING_STATEMENT;
import static org.apache.flink.table.client.config.ResultMode.TABLEAU;
import static org.apache.flink.table.client.config.SqlClientOptions.EXECUTION_RESULT_MODE;

/** SQL CLI client. */
public class CliClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CliClient.class);
    public static final Supplier<Terminal> DEFAULT_TERMINAL_FACTORY =
            TerminalUtils::createDefaultTerminal;

    private final Executor executor;

    private final Path historyFilePath;

    private static final String NEWLINE_PROMPT =
            new AttributedStringBuilder()
                    .style(AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN))
                    .append("Flink SQL")
                    .style(AttributedStyle.DEFAULT)
                    .append("> ")
                    .toAnsi();

    private final @Nullable MaskingCallback inputTransformer;

    private final Supplier<Terminal> terminalFactory;

    private Terminal terminal;

    private boolean isRunning;

    private final SqlMultiLineParser parser;

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
        this.parser = new SqlMultiLineParser(new ClientParser());
    }

    /**
     * Creates a CLI instance with a prepared terminal. Make sure to close the CLI instance
     * afterwards using {@link #close()}.
     */
    public CliClient(Supplier<Terminal> terminalFactory, Executor executor, Path historyFilePath) {
        this(terminalFactory, executor, historyFilePath, null);
    }

    public Terminal getTerminal() {
        return terminal;
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

    /** Closes the CLI instance. */
    public void close() {
        if (terminal != null) {
            closeTerminal();
        }
    }

    /** Opens the interactive CLI shell. */
    public void executeInInteractiveMode() {
        try {
            terminal = terminalFactory.get();
            executeInteractive();
        } finally {
            closeTerminal();
        }
    }

    public void executeInNonInteractiveMode(String content) {
        try {
            terminal = terminalFactory.get();
            executeFile(content, terminal.output(), ExecutionMode.NON_INTERACTIVE_EXECUTION);
        } finally {
            closeTerminal();
        }
    }

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

    enum ExecutionMode {
        INTERACTIVE_EXECUTION,

        NON_INTERACTIVE_EXECUTION,

        INITIALIZATION
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Execute statement from the user input and prints status information and/or errors on the
     * terminal.
     */
    private void executeInteractive() {
        // make space from previous output and test the writer
        terminal.writer().println();
        terminal.writer().flush();

        // print welcome
        terminal.writer().append(CliStrings.MESSAGE_WELCOME);

        LineReader lineReader = createLineReader(terminal, true);
        getAndExecuteStatements(lineReader, ExecutionMode.INTERACTIVE_EXECUTION);
    }

    private boolean getAndExecuteStatements(LineReader lineReader, ExecutionMode mode) {
        // begin reading loop
        boolean exitOnFailure = !mode.equals(ExecutionMode.INTERACTIVE_EXECUTION);
        isRunning = true;
        String buffer = "";
        String prompt = NEWLINE_PROMPT;
        String line = "";

        while (isRunning) {
            try {
                // read a statement from terminal and parse it
                line = lineReader.readLine(prompt, null, inputTransformer, null);
                if (line.trim().isEmpty()) {
                    continue;
                }
                if (parser.getStatementType().isPresent()) {
                    line = buffer + line;
                    boolean success = executeStatement(line, mode);
                    if (exitOnFailure && !success) {
                        return false;
                    }
                }
            } catch (UserInterruptException e) {
                // user cancelled line with Ctrl+C
            } catch (EndOfFileException | IOError e) {
                // user cancelled application with Ctrl+D or kill
                break;
            } catch (SqlParserEOFException e) {
                prompt = null;
                buffer = line;
                continue;
            } catch (Throwable t) {
                throw new SqlClientException("Could not read from command line.", t);
            }
            // clear the buffer
            buffer = "";
            prompt = NEWLINE_PROMPT;
            printNewLine();
        }
        // finish all statements.
        return true;
    }

    private void printNewLine() {
        // make some space to previous command
        terminal.writer().append("\n");
        terminal.flush();
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
            LineReader lineReader = createLineReader(dumbTerminal, false);
            return getAndExecuteStatements(lineReader, mode);
        } catch (Throwable e) {
            printExecutionException(e);
            return false;
        }
    }

    private boolean executeStatement(String statement, ExecutionMode executionMode) {
        try {
            final Thread thread = Thread.currentThread();
            final Terminal.SignalHandler previousHandler =
                    terminal.handle(Terminal.Signal.INT, (signal) -> thread.interrupt());
            try {
                if (executionMode == ExecutionMode.INITIALIZATION) {
                    executor.configureSession(statement);
                    printInfo(MESSAGE_EXECUTE_STATEMENT);
                } else {
                    executeInExecutionMode(statement, executionMode);
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

    private void executeInExecutionMode(String statement, ExecutionMode executionMode) {
        StatementType statementType =
                parser.getStatementType()
                        .orElseThrow(
                                () -> new SqlExecutionException("The statement should be parsed."));
        switch (statementType) {
            case QUIT:
                callQuit();
                break;
            case CLEAR:
                callClear();
                break;
            case HELP:
                callHelp();
                break;
            default:
                call(statement, executionMode);
        }
    }

    private void callQuit() {
        printInfo(CliStrings.MESSAGE_QUIT);
        isRunning = false;
    }

    private void callClear() {
        clearTerminal();
    }

    private void callHelp() {
        terminal.writer().println(CliStrings.MESSAGE_HELP);
        terminal.flush();
    }

    private void call(String statement, ExecutionMode executionMode) {
        try (ClientResult result = executor.executeStatement(statement)) {
            if (result.isQueryResult()) {
                printQuery(result, executionMode);
            } else if (result.getJobId() != null) {
                printJob(result.getJobId());
            } else {
                print(result);
            }
        }
    }

    private void printQuery(ClientResult queryResult, ExecutionMode executionMode) {
        final ResultDescriptor resultDesc =
                new ResultDescriptor(queryResult, executor.getSessionConfig());
        if (executionMode.equals(ExecutionMode.NON_INTERACTIVE_EXECUTION)
                && !resultDesc.isTableauMode()) {
            throw new SqlExecutionException(
                    String.format(
                            "In non-interactive mode, it only supports to use %s as value of %s when execute query. Please add 'SET %s=%s;' in the sql file.",
                            TABLEAU,
                            EXECUTION_RESULT_MODE.key(),
                            EXECUTION_RESULT_MODE.key(),
                            TABLEAU));
        }

        if (resultDesc.isTableauMode()) {
            try (CliTableauResultView tableauResultView =
                    new CliTableauResultView(terminal, resultDesc)) {
                tableauResultView.displayResults();
            }
        } else {
            final CliResultView<?> view;
            if (resultDesc.isMaterialized()) {
                view = new CliTableResultView(terminal, resultDesc);
            } else {
                view = new CliChangelogResultView(terminal, resultDesc);
            }

            // enter view
            view.open();

            // view left
            printInfo(CliStrings.MESSAGE_RESULT_QUIT);
        }
    }

    private void printJob(JobID jobID) {
        if (executor.getSessionConfig().get(TABLE_DML_SYNC)) {
            printInfo(MESSAGE_FINISH_STATEMENT);
        } else {
            terminal.writer()
                    .println(CliStrings.messageInfo(MESSAGE_SUBMITTING_STATEMENT).toAnsi());
            terminal.writer().println(CliStrings.messageInfo(MESSAGE_STATEMENT_SUBMITTED).toAnsi());
            terminal.writer().println(String.format("Job ID: %s\n", jobID));
            terminal.flush();
        }
    }

    private void print(ClientResult result) {
        if (result.getResultKind() == ResultKind.SUCCESS) {
            // print more meaningful message than tableau OK result
            printInfo(MESSAGE_EXECUTE_STATEMENT);
        } else {
            // print tableau if result has content
            PrintStyle.tableauWithDataInferredColumnWidths(
                            result.getResultSchema(),
                            result.getRowDataToStringConverter(),
                            Integer.MAX_VALUE,
                            true,
                            false)
                    .print(result, terminal.writer());
        }
    }

    // --------------------------------------------------------------------------------------------

    private void printExecutionException(Throwable t) {
        final String errorMessage = CliStrings.MESSAGE_SQL_EXECUTION_ERROR;
        LOG.warn(errorMessage, t);
        boolean isVerbose = executor.getSessionConfig().get(SqlClientOptions.VERBOSE);
        terminal.writer().println(CliStrings.messageError(errorMessage, t, isVerbose).toAnsi());
        terminal.flush();
    }

    private void printInfo(String message) {
        terminal.writer().println(CliStrings.messageInfo(message).toAnsi());
        terminal.flush();
    }

    // --------------------------------------------------------------------------------------------

    private void closeTerminal() {
        try {
            terminal.close();
            terminal = null;
        } catch (IOException e) {
            // ignore
        }
    }

    private LineReader createLineReader(Terminal terminal, boolean enableSqlCompleter) {
        // initialize line lineReader
        LineReaderBuilder builder =
                LineReaderBuilder.builder()
                        .terminal(terminal)
                        .appName(CliStrings.CLI_NAME)
                        .parser(parser);

        if (enableSqlCompleter) {
            builder.completer(new SqlCompleter(executor));
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
