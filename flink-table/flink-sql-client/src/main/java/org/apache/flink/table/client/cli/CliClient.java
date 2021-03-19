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
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.client.SqlClient;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.cli.SqlCommandParser.SqlCommandCall;
import org.apache.flink.table.client.config.YamlConfigUtils;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.utils.PrintUtils;

import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.MaskingCallback;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.InfoCmp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOError;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_SET;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_SET_DEPRECATED_KEY;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_SET_REMOVED_KEY;
import static org.apache.flink.util.Preconditions.checkState;

/** SQL CLI client. */
public class CliClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CliClient.class);

    private final Executor executor;

    private final String sessionId;

    private final Terminal terminal;

    private final LineReader lineReader;

    private final String prompt;

    private final @Nullable MaskingCallback inputTransformer;

    private boolean isRunning;

    private static final int PLAIN_TERMINAL_WIDTH = 80;

    private static final int PLAIN_TERMINAL_HEIGHT = 30;

    private static final int SOURCE_MAX_SIZE = 50_000;

    /**
     * Creates a CLI instance with a custom terminal. Make sure to close the CLI instance afterwards
     * using {@link #close()}.
     */
    @VisibleForTesting
    public CliClient(
            Terminal terminal,
            String sessionId,
            Executor executor,
            Path historyFilePath,
            @Nullable MaskingCallback inputTransformer) {
        this.terminal = terminal;
        this.sessionId = sessionId;
        this.executor = executor;
        this.inputTransformer = inputTransformer;

        // make space from previous output and test the writer
        terminal.writer().println();
        terminal.writer().flush();

        // initialize line lineReader
        lineReader =
                LineReaderBuilder.builder()
                        .terminal(terminal)
                        .appName(CliStrings.CLI_NAME)
                        .parser(new SqlMultiLineParser())
                        .completer(new SqlCompleter(sessionId, executor))
                        .build();
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
            System.out.println(msg);
            LOG.info(msg);
            lineReader.setVariable(LineReader.HISTORY_FILE, historyFilePath);
        } else {
            String msg = "Unable to create history file: " + historyFilePath;
            System.out.println(msg);
            LOG.warn(msg);
        }

        // create prompt
        prompt =
                new AttributedStringBuilder()
                        .style(AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN))
                        .append("Flink SQL")
                        .style(AttributedStyle.DEFAULT)
                        .append("> ")
                        .toAnsi();
    }

    /**
     * Creates a CLI instance with a prepared terminal. Make sure to close the CLI instance
     * afterwards using {@link #close()}.
     */
    public CliClient(String sessionId, Executor executor, Path historyFilePath) {
        this(createDefaultTerminal(), sessionId, executor, historyFilePath, null);
    }

    public Terminal getTerminal() {
        return terminal;
    }

    public String getSessionId() {
        return this.sessionId;
    }

    public void clearTerminal() {
        if (isPlainTerminal()) {
            for (int i = 0; i < 200; i++) { // large number of empty lines
                terminal.writer().println();
            }
        } else {
            terminal.puts(InfoCmp.Capability.clear_screen);
        }
    }

    public boolean isPlainTerminal() {
        // check if terminal width can be determined
        // e.g. IntelliJ IDEA terminal supports only a plain terminal
        return terminal.getWidth() == 0 && terminal.getHeight() == 0;
    }

    public int getWidth() {
        if (isPlainTerminal()) {
            return PLAIN_TERMINAL_WIDTH;
        }
        return terminal.getWidth();
    }

    public int getHeight() {
        if (isPlainTerminal()) {
            return PLAIN_TERMINAL_HEIGHT;
        }
        return terminal.getHeight();
    }

    public Executor getExecutor() {
        return executor;
    }

    /** Opens the interactive CLI shell. */
    public void open() {
        isRunning = true;

        // print welcome
        terminal.writer().append(CliStrings.MESSAGE_WELCOME);

        // begin reading loop
        while (isRunning) {
            // make some space to previous command
            terminal.writer().append("\n");
            terminal.flush();

            final String line;
            try {
                line = lineReader.readLine(prompt, null, inputTransformer, null);
            } catch (UserInterruptException e) {
                // user cancelled line with Ctrl+C
                continue;
            } catch (EndOfFileException | IOError e) {
                // user cancelled application with Ctrl+D or kill
                break;
            } catch (Throwable t) {
                throw new SqlClientException("Could not read from command line.", t);
            }
            if (line == null) {
                continue;
            }
            final Optional<SqlCommandCall> cmdCall = parseCommand(line);
            cmdCall.ifPresent(this::callCommand);
        }
    }

    /** Closes the CLI instance. */
    public void close() {
        try {
            terminal.close();
        } catch (IOException e) {
            throw new SqlClientException("Unable to close terminal.", e);
        }
    }

    /**
     * Submits a SQL update statement and prints status information and/or errors on the terminal.
     *
     * @param statement SQL update statement
     * @return flag to indicate if the submission was successful or not
     */
    public boolean submitUpdate(String statement) {
        terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_WILL_EXECUTE).toAnsi());
        terminal.writer().println(new AttributedString(statement).toString());
        terminal.flush();

        final Optional<SqlCommandCall> parsedStatement = parseCommand(statement);
        // only support INSERT INTO/OVERWRITE
        return parsedStatement
                .map(
                        cmdCall -> {
                            switch (cmdCall.command) {
                                case INSERT_INTO:
                                case INSERT_OVERWRITE:
                                    return callInsert(cmdCall);
                                default:
                                    printError(CliStrings.MESSAGE_UNSUPPORTED_SQL);
                                    return false;
                            }
                        })
                .orElse(false);
    }

    // --------------------------------------------------------------------------------------------

    private Optional<SqlCommandCall> parseCommand(String line) {
        final SqlCommandCall parsedLine;
        try {
            parsedLine = SqlCommandParser.parse(executor.getSqlParser(sessionId), line);
        } catch (SqlExecutionException e) {
            printExecutionException(e);
            return Optional.empty();
        }
        return Optional.of(parsedLine);
    }

    private void callCommand(SqlCommandCall cmdCall) {
        switch (cmdCall.command) {
            case QUIT:
                callQuit();
                break;
            case CLEAR:
                callClear();
                break;
            case RESET:
                callReset();
                break;
            case SET:
                callSet(cmdCall);
                break;
            case HELP:
                callHelp();
                break;
            case SHOW_CATALOGS:
            case SHOW_DATABASES:
            case SHOW_TABLES:
            case SHOW_VIEWS:
            case SHOW_FUNCTIONS:
            case SHOW_MODULES:
            case SHOW_PARTITIONS:
            case SHOW_CURRENT_CATALOG:
            case SHOW_CURRENT_DATABASE:
                callShow(cmdCall);
                break;
            case USE_CATALOG:
                callUseCatalog(cmdCall);
                break;
            case USE:
                callUseDatabase(cmdCall);
                break;
            case DESC:
            case DESCRIBE:
                callDescribe(cmdCall);
                break;
            case EXPLAIN:
                callExplain(cmdCall);
                break;
            case SELECT:
                callSelect(cmdCall);
                break;
            case INSERT_INTO:
            case INSERT_OVERWRITE:
                callInsert(cmdCall);
                break;
            case CREATE_TABLE:
                callDdl(cmdCall.operands[0], CliStrings.MESSAGE_TABLE_CREATED);
                break;
            case DROP_TABLE:
                callDdl(cmdCall.operands[0], CliStrings.MESSAGE_TABLE_REMOVED);
                break;
            case CREATE_VIEW:
                callDdl(cmdCall.operands[0], CliStrings.MESSAGE_VIEW_CREATED);
                break;
            case DROP_VIEW:
                callDdl(cmdCall.operands[0], CliStrings.MESSAGE_VIEW_REMOVED);
                break;
            case ALTER_VIEW:
                callDdl(
                        cmdCall.operands[0],
                        CliStrings.MESSAGE_ALTER_VIEW_SUCCEEDED,
                        CliStrings.MESSAGE_ALTER_VIEW_FAILED);
                break;
            case CREATE_FUNCTION:
                callDdl(cmdCall.operands[0], CliStrings.MESSAGE_FUNCTION_CREATED);
                break;
            case DROP_FUNCTION:
                callDdl(cmdCall.operands[0], CliStrings.MESSAGE_FUNCTION_REMOVED);
                break;
            case ALTER_FUNCTION:
                callDdl(
                        cmdCall.operands[0],
                        CliStrings.MESSAGE_ALTER_FUNCTION_SUCCEEDED,
                        CliStrings.MESSAGE_ALTER_FUNCTION_FAILED);
                break;
            case SOURCE:
                callSource(cmdCall);
                break;
            case CREATE_DATABASE:
                callDdl(cmdCall.operands[0], CliStrings.MESSAGE_DATABASE_CREATED);
                break;
            case DROP_DATABASE:
                callDdl(cmdCall.operands[0], CliStrings.MESSAGE_DATABASE_REMOVED);
                break;
            case ALTER_DATABASE:
                callDdl(
                        cmdCall.operands[0],
                        CliStrings.MESSAGE_ALTER_DATABASE_SUCCEEDED,
                        CliStrings.MESSAGE_ALTER_DATABASE_FAILED);
                break;
            case ALTER_TABLE:
                callDdl(
                        cmdCall.operands[0],
                        CliStrings.MESSAGE_ALTER_TABLE_SUCCEEDED,
                        CliStrings.MESSAGE_ALTER_TABLE_FAILED);
                break;
            case CREATE_CATALOG:
                callDdl(cmdCall.operands[0], CliStrings.MESSAGE_CATALOG_CREATED);
                break;
            case DROP_CATALOG:
                callDdl(cmdCall.operands[0], CliStrings.MESSAGE_CATALOG_REMOVED);
                break;
            case LOAD_MODULE:
                callDdl(
                        cmdCall.operands[0],
                        CliStrings.MESSAGE_LOAD_MODULE_SUCCEEDED,
                        CliStrings.MESSAGE_LOAD_MODULE_FAILED);
                break;
            case UNLOAD_MODULE:
                callDdl(
                        cmdCall.operands[0],
                        CliStrings.MESSAGE_UNLOAD_MODULE_SUCCEEDED,
                        CliStrings.MESSAGE_UNLOAD_MODULE_FAILED);
                break;
            case USE_MODULES:
                callDdl(
                        cmdCall.operands[0],
                        CliStrings.MESSAGE_USE_MODULES_SUCCEEDED,
                        CliStrings.MESSAGE_USE_MODULES_FAILED);
                break;
            default:
                throw new SqlClientException("Unsupported command: " + cmdCall.command);
        }
    }

    private void callQuit() {
        printInfo(CliStrings.MESSAGE_QUIT);
        isRunning = false;
    }

    private void callClear() {
        clearTerminal();
    }

    private void callReset() {
        try {
            executor.resetSessionProperties(sessionId);
        } catch (SqlExecutionException e) {
            printExecutionException(e);
            return;
        }
        printInfo(CliStrings.MESSAGE_RESET);
    }

    private void callSet(SqlCommandCall cmdCall) {
        // show all properties
        if (cmdCall.operands.length == 0) {
            final Map<String, String> properties;
            try {
                properties = executor.getSessionProperties(sessionId);
            } catch (SqlExecutionException e) {
                printExecutionException(e);
                return;
            }
            if (properties.isEmpty()) {
                terminal.writer()
                        .println(CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY).toAnsi());
            } else {
                List<String> prettyEntries = YamlConfigUtils.getPropertiesInPretty(properties);
                prettyEntries.forEach(entry -> terminal.writer().println(entry));
            }
        }
        // set a property
        else {
            String key = cmdCall.operands[0].trim();
            String value = cmdCall.operands[1].trim();
            try {
                executor.setSessionProperty(sessionId, key, value);
            } catch (SqlExecutionException e) {
                printExecutionException(e);
                return;
            }
            if (YamlConfigUtils.isRemovedKey(key)) {
                terminal.writer()
                        .println(CliStrings.messageWarning(MESSAGE_SET_REMOVED_KEY).toAnsi());
            } else {
                if (YamlConfigUtils.isDeprecatedKey(key)) {
                    terminal.writer()
                            .println(
                                    CliStrings.messageWarning(
                                                    String.format(
                                                            MESSAGE_SET_DEPRECATED_KEY,
                                                            key,
                                                            YamlConfigUtils
                                                                    .getOptionNameWithDeprecatedKey(
                                                                            key)))
                                            .toAnsi());
                }
                terminal.writer().println(CliStrings.messageInfo(MESSAGE_SET).toAnsi());
            }
        }
        terminal.flush();
    }

    private void callHelp() {
        terminal.writer().println(CliStrings.MESSAGE_HELP);
        terminal.flush();
    }

    private void callShow(SqlCommandCall cmdCall) {
        getResultAsTableauForm(
                cmdCall.operands.length == 0 ? cmdCall.command.toString() : cmdCall.operands[0]);
    }

    private void callUseCatalog(SqlCommandCall cmdCall) {
        try {
            executor.executeSql(sessionId, "USE CATALOG `" + cmdCall.operands[0] + "`");
        } catch (SqlExecutionException e) {
            printExecutionException(e);
            return;
        }
        printInfo(CliStrings.MESSAGE_CATALOG_CHANGED);
        terminal.flush();
    }

    private void callUseDatabase(SqlCommandCall cmdCall) {
        try {
            executor.executeSql(sessionId, "USE `" + cmdCall.operands[0] + "`");
        } catch (SqlExecutionException e) {
            printExecutionException(e);
            return;
        }
        printInfo(CliStrings.MESSAGE_DATABASE_CHANGED);
        terminal.flush();
    }

    private void callDescribe(SqlCommandCall cmdCall) {
        getResultAsTableauForm("DESCRIBE " + cmdCall.operands[0]);
    }

    private void getResultAsTableauForm(String statement) {
        final TableResult result;
        try {
            result = executor.executeSql(sessionId, statement);
        } catch (SqlExecutionException e) {
            printExecutionException(e);
            return;
        }
        PrintUtils.printAsTableauForm(
                result.getResolvedSchema(),
                result.collect(),
                terminal.writer(),
                Integer.MAX_VALUE,
                "",
                false,
                false);
        terminal.flush();
    }

    private void callExplain(SqlCommandCall cmdCall) {
        final String explanation;
        try {
            TableResult tableResult = executor.executeSql(sessionId, cmdCall.operands[0]);
            explanation = tableResult.collect().next().getField(0).toString();
        } catch (SqlExecutionException e) {
            printExecutionException(e);
            return;
        }
        terminal.writer().println(explanation);
        terminal.flush();
    }

    private void callSelect(SqlCommandCall cmdCall) {
        final ResultDescriptor resultDesc;
        try {
            resultDesc = executor.executeQuery(sessionId, cmdCall.operands[0]);
        } catch (SqlExecutionException e) {
            printExecutionException(e);
            return;
        }

        if (resultDesc.isTableauMode()) {
            try (CliTableauResultView tableauResultView =
                    new CliTableauResultView(terminal, executor, sessionId, resultDesc)) {
                tableauResultView.displayResults();
            } catch (SqlExecutionException e) {
                printExecutionException(e);
            }
        } else {
            final CliResultView view;
            if (resultDesc.isMaterialized()) {
                view = new CliTableResultView(this, resultDesc);
            } else {
                view = new CliChangelogResultView(this, resultDesc);
            }

            // enter view
            try {
                view.open();

                // view left
                printInfo(CliStrings.MESSAGE_RESULT_QUIT);
            } catch (SqlExecutionException e) {
                printExecutionException(e);
            }
        }
    }

    private boolean callInsert(SqlCommandCall cmdCall) {
        printInfo(CliStrings.MESSAGE_SUBMITTING_STATEMENT);

        try {
            final TableResult tableResult = executor.executeSql(sessionId, cmdCall.operands[0]);
            checkState(tableResult.getJobClient().isPresent());
            terminal.writer()
                    .println(
                            CliStrings.messageInfo(CliStrings.MESSAGE_STATEMENT_SUBMITTED)
                                    .toAnsi());
            // keep compatibility with before
            terminal.writer()
                    .println(
                            String.format(
                                    "Job ID: %s\n",
                                    tableResult.getJobClient().get().getJobID().toString()));
            terminal.flush();
        } catch (SqlExecutionException e) {
            printExecutionException(e);
            return false;
        }
        return true;
    }

    private void callSource(SqlCommandCall cmdCall) {
        final String pathString = cmdCall.operands[0];

        // load file
        final String stmt;
        try {
            final Path path = Paths.get(pathString);
            byte[] encoded = Files.readAllBytes(path);
            stmt = new String(encoded, Charset.defaultCharset());
        } catch (IOException e) {
            printExecutionException(e);
            return;
        }

        // limit the output a bit
        if (stmt.length() > SOURCE_MAX_SIZE) {
            printExecutionError(CliStrings.MESSAGE_MAX_SIZE_EXCEEDED);
            return;
        }

        terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_WILL_EXECUTE).toAnsi());
        terminal.writer().println(new AttributedString(stmt).toString());
        terminal.flush();

        // try to run it
        final Optional<SqlCommandCall> call = parseCommand(stmt);
        call.ifPresent(this::callCommand);
    }

    private void callDdl(String ddl, String successMessage) {
        callDdl(ddl, successMessage, null);
    }

    private void callDdl(String ddl, String successMessage, String errorMessage) {
        try {
            executor.executeSql(sessionId, ddl);
            printInfo(successMessage);
        } catch (SqlExecutionException e) {
            printExecutionException(errorMessage, e);
        }
    }

    // --------------------------------------------------------------------------------------------

    private void printExecutionException(Throwable t) {
        printExecutionException(null, t);
    }

    private void printExecutionException(String message, Throwable t) {
        final String finalMessage;
        if (message == null) {
            finalMessage = CliStrings.MESSAGE_SQL_EXECUTION_ERROR;
        } else {
            finalMessage = CliStrings.MESSAGE_SQL_EXECUTION_ERROR + ' ' + message;
        }
        printException(finalMessage, t);
    }

    private void printExecutionError(String message) {
        terminal.writer()
                .println(
                        CliStrings.messageError(CliStrings.MESSAGE_SQL_EXECUTION_ERROR, message)
                                .toAnsi());
        terminal.flush();
    }

    private void printException(String message, Throwable t) {
        LOG.warn(message, t);
        terminal.writer().println(CliStrings.messageError(message, t).toAnsi());
        terminal.flush();
    }

    private void printError(String message) {
        terminal.writer().println(CliStrings.messageError(message).toAnsi());
        terminal.flush();
    }

    private void printInfo(String message) {
        terminal.writer().println(CliStrings.messageInfo(message).toAnsi());
        terminal.flush();
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Internal flag to use {@link System#in} and {@link System#out} stream to construct {@link
     * Terminal} for tests. This allows tests can easily mock input stream when startup {@link
     * SqlClient}.
     */
    protected static boolean useSystemInOutStream = false;

    private static Terminal createDefaultTerminal() {
        try {
            if (useSystemInOutStream) {
                return TerminalBuilder.builder()
                        .name(CliStrings.CLI_NAME)
                        .streams(System.in, System.out)
                        .build();
            } else {
                return TerminalBuilder.builder().name(CliStrings.CLI_NAME).build();
            }
        } catch (IOException e) {
            throw new SqlClientException("Error opening command line interface.", e);
        }
    }
}
