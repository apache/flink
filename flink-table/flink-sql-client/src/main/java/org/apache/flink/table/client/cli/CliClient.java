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
import org.apache.flink.table.client.config.ResultMode;
import org.apache.flink.table.client.config.SqlClientOptions;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.command.ClearOperation;
import org.apache.flink.table.operations.command.HelpOperation;
import org.apache.flink.table.operations.command.QuitOperation;
import org.apache.flink.table.operations.command.ResetOperation;
import org.apache.flink.table.operations.command.SetOperation;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_DML_SYNC;
import static org.apache.flink.table.api.internal.TableResultImpl.TABLE_RESULT_OK;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_DEPRECATED_KEY;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_EXECUTE_STATEMENT;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_FINISH_STATEMENT;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_REMOVED_KEY;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_RESET_KEY;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_SET_KEY;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_STATEMENT_SUBMITTED;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_WAIT_EXECUTE;
import static org.apache.flink.table.client.config.ResultMode.TABLEAU;
import static org.apache.flink.table.client.config.SqlClientOptions.EXECUTION_RESULT_MODE;
import static org.apache.flink.table.client.config.YamlConfigUtils.getOptionNameWithDeprecatedKey;
import static org.apache.flink.table.client.config.YamlConfigUtils.getPropertiesInPretty;
import static org.apache.flink.table.client.config.YamlConfigUtils.isDeprecatedKey;
import static org.apache.flink.table.client.config.YamlConfigUtils.isRemovedKey;
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

            String line;
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

            executeStatement(line, ExecutionMode.INTERACTIVE);
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
     * Submits content from Sql file and prints status information and/or errors on the terminal.
     *
     * @param content SQL file content
     * @return flag to indicate if the submission was successful or not
     */
    public boolean executeSqlFile(String content) {
        terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_WILL_EXECUTE).toAnsi());

        for (String statement : CliStatementSplitter.splitContent(content)) {
            terminal.writer().println(new AttributedString(statement).toString());
            terminal.flush();

            if (!executeStatement(statement, ExecutionMode.NON_INTERACTIVE)) {
                // cancel execution when meet error or ctrl + C;
                return false;
            }
        }
        return true;
    }

    // --------------------------------------------------------------------------------------------

    enum ExecutionMode {
        INTERACTIVE,

        NON_INTERACTIVE;
    }

    // --------------------------------------------------------------------------------------------

    private boolean executeStatement(String statement, ExecutionMode executionMode) {
        try {
            final Optional<Operation> operation = parseCommand(statement);
            operation.ifPresent(op -> callOperation(op, executionMode));
        } catch (SqlExecutionException e) {
            printExecutionException(e);
            return false;
        }
        return true;
    }

    private void validate(Operation operation, ExecutionMode executionMode) {
        ResultMode mode = executor.getSessionConfig(sessionId).get(EXECUTION_RESULT_MODE);
        if (operation instanceof QueryOperation
                && executionMode.equals(ExecutionMode.NON_INTERACTIVE)
                && !mode.equals(TABLEAU)) {
            throw new IllegalArgumentException(
                    String.format(
                            "In non-interactive mode, it only supports to use %s as value of %s when execute query. Please add 'SET %s=%s;' in the sql file.",
                            TABLEAU,
                            EXECUTION_RESULT_MODE.key(),
                            EXECUTION_RESULT_MODE.key(),
                            TABLEAU));
        }
    }

    private Optional<Operation> parseCommand(String stmt) {
        // normalize
        stmt = stmt.trim();
        // remove ';' at the end
        if (stmt.endsWith(";")) {
            stmt = stmt.substring(0, stmt.length() - 1).trim();
        }

        Operation operation = executor.parseStatement(sessionId, stmt);
        return Optional.of(operation);
    }

    private void callOperation(Operation operation, ExecutionMode mode) {

        validate(operation, mode);

        if (operation instanceof QuitOperation) {
            // QUIT/EXIT
            callQuit();
        } else if (operation instanceof ClearOperation) {
            // CLEAR
            callClear();
        } else if (operation instanceof HelpOperation) {
            // HELP
            callHelp();
        } else if (operation instanceof SetOperation) {
            // SET
            callSet((SetOperation) operation);
        } else if (operation instanceof ResetOperation) {
            // RESET
            callReset((ResetOperation) operation);
        } else if (operation instanceof CatalogSinkModifyOperation) {
            // INSERT INTO/OVERWRITE
            callInsert((CatalogSinkModifyOperation) operation);
        } else if (operation instanceof QueryOperation) {
            // SELECT
            callSelect((QueryOperation) operation);
        } else {
            // fallback to default implementation
            executeOperation(operation);
        }
    }

    private void callQuit() {
        printInfo(CliStrings.MESSAGE_QUIT);
        isRunning = false;
    }

    private void callClear() {
        clearTerminal();
    }

    private void callReset(ResetOperation resetOperation) {
        // reset all session properties
        if (!resetOperation.getKey().isPresent()) {
            executor.resetSessionProperties(sessionId);
            printInfo(CliStrings.MESSAGE_RESET);
        }
        // reset a session property
        else {
            String key = resetOperation.getKey().get();
            executor.resetSessionProperty(sessionId, key);
            printSetResetConfigKeyMessage(key, MESSAGE_RESET_KEY);
        }
    }

    private void callSet(SetOperation setOperation) {
        // set a property
        if (setOperation.getKey().isPresent() && setOperation.getValue().isPresent()) {
            String key = setOperation.getKey().get().trim();
            String value = setOperation.getValue().get().trim();
            executor.setSessionProperty(sessionId, key, value);
            printSetResetConfigKeyMessage(key, MESSAGE_SET_KEY);
        }
        // show all properties
        else {
            final Map<String, String> properties = executor.getSessionConfigMap(sessionId);
            if (properties.isEmpty()) {
                terminal.writer()
                        .println(CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY).toAnsi());
            } else {
                List<String> prettyEntries = getPropertiesInPretty(properties);
                prettyEntries.forEach(entry -> terminal.writer().println(entry));
            }
            terminal.flush();
        }
    }

    private void callHelp() {
        terminal.writer().println(CliStrings.MESSAGE_HELP);
        terminal.flush();
    }

    private void callSelect(QueryOperation operation) {
        final ResultDescriptor resultDesc = executor.executeQuery(sessionId, operation);

        if (resultDesc.isTableauMode()) {
            try (CliTableauResultView tableauResultView =
                    new CliTableauResultView(terminal, executor, sessionId, resultDesc)) {
                tableauResultView.displayResults();
            }
        } else {
            final CliResultView<?> view;
            if (resultDesc.isMaterialized()) {
                view = new CliTableResultView(this, resultDesc);
            } else {
                view = new CliChangelogResultView(this, resultDesc);
            }

            // enter view
            view.open();

            // view left
            printInfo(CliStrings.MESSAGE_RESULT_QUIT);
        }
    }

    private void callInsert(CatalogSinkModifyOperation operation) {
        printInfo(CliStrings.MESSAGE_SUBMITTING_STATEMENT);

        boolean sync = executor.getSessionConfig(sessionId).get(TABLE_DML_SYNC);
        if (sync) {
            printInfo(MESSAGE_WAIT_EXECUTE);
        }
        TableResult tableResult = executor.executeOperation(sessionId, operation);
        checkState(tableResult.getJobClient().isPresent());
        if (sync) {
            terminal.writer().println(CliStrings.messageInfo(MESSAGE_FINISH_STATEMENT).toAnsi());
        } else {
            terminal.writer().println(CliStrings.messageInfo(MESSAGE_STATEMENT_SUBMITTED).toAnsi());
            terminal.writer()
                    .println(
                            String.format(
                                    "Job ID: %s\n",
                                    tableResult.getJobClient().get().getJobID().toString()));
        }
        terminal.flush();
    }

    private void executeOperation(Operation operation) {
        TableResult result = executor.executeOperation(sessionId, operation);
        if (TABLE_RESULT_OK == result) {
            // print more meaningful message than tableau OK result
            printInfo(MESSAGE_EXECUTE_STATEMENT);
        } else {
            // print tableau if result has content
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
    }

    // --------------------------------------------------------------------------------------------

    private void printExecutionException(Throwable t) {
        final String errorMessage = CliStrings.MESSAGE_SQL_EXECUTION_ERROR;
        LOG.warn(errorMessage, t);
        boolean isVerbose = executor.getSessionConfig(sessionId).get(SqlClientOptions.VERBOSE);
        terminal.writer().println(CliStrings.messageError(errorMessage, t, isVerbose).toAnsi());
        terminal.flush();
    }

    private void printInfo(String message) {
        terminal.writer().println(CliStrings.messageInfo(message).toAnsi());
        terminal.flush();
    }

    private void printWarning(String message) {
        terminal.writer().println(CliStrings.messageWarning(message).toAnsi());
        terminal.flush();
    }

    private void printSetResetConfigKeyMessage(String key, String message) {
        boolean isRemovedKey = isRemovedKey(key);
        boolean isDeprecatedKey = isDeprecatedKey(key);

        // print warning information if the given key is removed or deprecated
        if (isRemovedKey || isDeprecatedKey) {
            String warningMsg =
                    isRemovedKey
                            ? MESSAGE_REMOVED_KEY
                            : String.format(
                                    MESSAGE_DEPRECATED_KEY,
                                    key,
                                    getOptionNameWithDeprecatedKey(key));
            printWarning(warningMsg);
        }

        // when the key is not removed, need to print normal message
        if (!isRemovedKey) {
            terminal.writer().println(CliStrings.messageInfo(message).toAnsi());
            terminal.flush();
        }
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
