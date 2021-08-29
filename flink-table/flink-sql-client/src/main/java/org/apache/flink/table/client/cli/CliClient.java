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
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.config.ResultMode;
import org.apache.flink.table.client.config.SqlClientOptions;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.operations.BeginStatementSetOperation;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.EndStatementSetOperation;
import org.apache.flink.table.operations.ExplainOperation;
import org.apache.flink.table.operations.LoadModuleOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.ShowCreateTableOperation;
import org.apache.flink.table.operations.UnloadModuleOperation;
import org.apache.flink.table.operations.UseOperation;
import org.apache.flink.table.operations.command.AddJarOperation;
import org.apache.flink.table.operations.command.ClearOperation;
import org.apache.flink.table.operations.command.HelpOperation;
import org.apache.flink.table.operations.command.QuitOperation;
import org.apache.flink.table.operations.command.RemoveJarOperation;
import org.apache.flink.table.operations.command.ResetOperation;
import org.apache.flink.table.operations.command.SetOperation;
import org.apache.flink.table.operations.command.ShowJarsOperation;
import org.apache.flink.table.operations.ddl.AlterOperation;
import org.apache.flink.table.operations.ddl.CreateOperation;
import org.apache.flink.table.operations.ddl.DropOperation;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.table.utils.PrintUtils;

import org.apache.commons.collections.CollectionUtils;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.MaskingCallback;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.InfoCmp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_DML_SYNC;
import static org.apache.flink.table.api.internal.TableResultImpl.TABLE_RESULT_OK;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_EXECUTE_STATEMENT;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_FINISH_STATEMENT;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_RESET_KEY;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_SET_KEY;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_STATEMENT_SET_END_CALL_ERROR;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_STATEMENT_SET_SQL_EXECUTION_ERROR;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_STATEMENT_SUBMITTED;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_WAIT_EXECUTE;
import static org.apache.flink.table.client.config.ResultMode.TABLEAU;
import static org.apache.flink.table.client.config.SqlClientOptions.EXECUTION_RESULT_MODE;
import static org.apache.flink.util.Preconditions.checkState;

/** SQL CLI client. */
public class CliClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CliClient.class);
    public static final Supplier<Terminal> DEFAULT_TERMINAL_FACTORY =
            TerminalUtils::createDefaultTerminal;

    private final Executor executor;

    private final String sessionId;

    private final Path historyFilePath;

    private final String prompt;

    private final @Nullable MaskingCallback inputTransformer;

    private final Supplier<Terminal> terminalFactory;

    private Terminal terminal;

    private boolean isRunning;

    private boolean isStatementSetMode;

    private List<ModifyOperation> statementSetOperations;

    private static final int PLAIN_TERMINAL_WIDTH = 80;

    private static final int PLAIN_TERMINAL_HEIGHT = 30;

    /**
     * Creates a CLI instance with a custom terminal. Make sure to close the CLI instance afterwards
     * using {@link #close()}.
     */
    @VisibleForTesting
    public CliClient(
            Supplier<Terminal> terminalFactory,
            String sessionId,
            Executor executor,
            Path historyFilePath,
            @Nullable MaskingCallback inputTransformer) {
        this.terminalFactory = terminalFactory;
        this.sessionId = sessionId;
        this.executor = executor;
        this.inputTransformer = inputTransformer;
        this.historyFilePath = historyFilePath;

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
    public CliClient(
            Supplier<Terminal> terminalFactory,
            String sessionId,
            Executor executor,
            Path historyFilePath) {
        this(terminalFactory, sessionId, executor, historyFilePath, null);
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
            executeFile(content, ExecutionMode.NON_INTERACTIVE_EXECUTION);
        } finally {
            closeTerminal();
        }
    }

    public boolean executeInitialization(String content) {
        try {
            OutputStream outputStream = new ByteArrayOutputStream(256);
            terminal = TerminalUtils.createDumbTerminal(outputStream);
            boolean success = executeFile(content, ExecutionMode.INITIALIZATION);
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
        isRunning = true;
        LineReader lineReader = createLineReader(terminal);

        // make space from previous output and test the writer
        terminal.writer().println();
        terminal.writer().flush();

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

            executeStatement(line, ExecutionMode.INTERACTIVE_EXECUTION);
        }
    }

    /**
     * Execute content from Sql file and prints status information and/or errors on the terminal.
     *
     * @param content SQL file content
     */
    private boolean executeFile(String content, ExecutionMode mode) {
        terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_EXECUTE_FILE).toAnsi());

        for (String statement : CliStatementSplitter.splitContent(content)) {
            terminal.writer()
                    .println(new AttributedString(String.format("%s%s", prompt, statement)));
            terminal.flush();

            if (!executeStatement(statement, mode)) {
                // cancel execution when meet error or ctrl + C;
                return false;
            }
        }
        return true;
    }

    private boolean executeStatement(String statement, ExecutionMode executionMode) {
        try {
            final Optional<Operation> operation = parseCommand(statement);
            operation.ifPresent(
                    op -> {
                        final Thread thread = Thread.currentThread();
                        final Terminal.SignalHandler previousHandler =
                                terminal.handle(
                                        Terminal.Signal.INT, (signal) -> thread.interrupt());
                        try {
                            callOperation(op, executionMode);
                        } finally {
                            terminal.handle(Terminal.Signal.INT, previousHandler);
                        }
                    });
        } catch (SqlExecutionException e) {
            printExecutionException(e);
            return false;
        }
        return true;
    }

    private void validate(Operation operation, ExecutionMode executionMode) {
        if (executionMode.equals(ExecutionMode.INITIALIZATION)) {
            if (!(operation instanceof SetOperation)
                    && !(operation instanceof ResetOperation)
                    && !(operation instanceof CreateOperation)
                    && !(operation instanceof DropOperation)
                    && !(operation instanceof UseOperation)
                    && !(operation instanceof AlterOperation)
                    && !(operation instanceof LoadModuleOperation)
                    && !(operation instanceof UnloadModuleOperation)
                    && !(operation instanceof AddJarOperation)
                    && !(operation instanceof RemoveJarOperation)) {
                throw new SqlExecutionException(
                        "Unsupported operation in sql init file: " + operation.asSummaryString());
            }
        } else if (executionMode.equals(ExecutionMode.NON_INTERACTIVE_EXECUTION)) {
            ResultMode mode = executor.getSessionConfig(sessionId).get(EXECUTION_RESULT_MODE);
            if (operation instanceof QueryOperation && !mode.equals(TABLEAU)) {
                throw new SqlExecutionException(
                        String.format(
                                "In non-interactive mode, it only supports to use %s as value of %s when execute query. Please add 'SET %s=%s;' in the sql file.",
                                TABLEAU,
                                EXECUTION_RESULT_MODE.key(),
                                EXECUTION_RESULT_MODE.key(),
                                TABLEAU));
            }
        }

        // check the current operation is allowed in STATEMENT SET.
        if (isStatementSetMode) {
            if (!(operation instanceof CatalogSinkModifyOperation
                    || operation instanceof EndStatementSetOperation)) {
                // It's up to invoker of the executeStatement to determine whether to continue
                // execution
                throw new SqlExecutionException(MESSAGE_STATEMENT_SET_SQL_EXECUTION_ERROR);
            }
        }
    }

    private Optional<Operation> parseCommand(String stmt) {
        // normalize
        stmt = stmt.trim();
        // remove ';' at the end
        if (stmt.endsWith(";")) {
            stmt = stmt.substring(0, stmt.length() - 1).trim();
        }

        // meet bad case, e.g ";\n"
        if (stmt.trim().isEmpty()) {
            return Optional.empty();
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
        } else if (operation instanceof ExplainOperation) {
            // EXPLAIN
            callExplain((ExplainOperation) operation);
        } else if (operation instanceof BeginStatementSetOperation) {
            // BEGIN STATEMENT SET
            callBeginStatementSet();
        } else if (operation instanceof EndStatementSetOperation) {
            // END
            callEndStatementSet();
        } else if (operation instanceof AddJarOperation) {
            // ADD JAR
            callAddJar((AddJarOperation) operation);
        } else if (operation instanceof RemoveJarOperation) {
            // REMOVE JAR
            callRemoveJar((RemoveJarOperation) operation);
        } else if (operation instanceof ShowJarsOperation) {
            // SHOW JARS
            callShowJars();
        } else if (operation instanceof ShowCreateTableOperation) {
            // SHOW CREATE TABLE
            callShowCreateTable((ShowCreateTableOperation) operation);
        } else {
            // fallback to default implementation
            executeOperation(operation);
        }
    }

    private void callAddJar(AddJarOperation operation) {
        String jarPath = operation.getPath();
        executor.addJar(sessionId, jarPath);
        printInfo(CliStrings.MESSAGE_ADD_JAR_STATEMENT);
    }

    private void callRemoveJar(RemoveJarOperation operation) {
        String jarPath = operation.getPath();
        executor.removeJar(sessionId, jarPath);
        printInfo(CliStrings.MESSAGE_REMOVE_JAR_STATEMENT);
    }

    private void callShowJars() {
        List<String> jars = executor.listJars(sessionId);
        if (CollectionUtils.isEmpty(jars)) {
            terminal.writer().println("Empty set");
        } else {
            jars.forEach(jar -> terminal.writer().println(jar));
        }
        terminal.flush();
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
            printInfo(MESSAGE_RESET_KEY);
        }
    }

    private void callSet(SetOperation setOperation) {
        // set a property
        if (setOperation.getKey().isPresent() && setOperation.getValue().isPresent()) {
            String key = setOperation.getKey().get().trim();
            String value = setOperation.getValue().get().trim();
            executor.setSessionProperty(sessionId, key, value);
            printInfo(MESSAGE_SET_KEY);
        }
        // show all properties
        else {
            final Map<String, String> properties = executor.getSessionConfigMap(sessionId);
            if (properties.isEmpty()) {
                terminal.writer()
                        .println(CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY).toAnsi());
            } else {
                List<String> prettyEntries = new ArrayList<>();
                for (String key : properties.keySet()) {
                    prettyEntries.add(
                            String.format(
                                    "'%s' = '%s'",
                                    EncodingUtils.escapeSingleQuotes(key),
                                    EncodingUtils.escapeSingleQuotes(properties.get(key))));
                }
                prettyEntries.sort(String::compareTo);
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
        if (isStatementSetMode) {
            statementSetOperations.add(operation);
            printInfo(CliStrings.MESSAGE_ADD_STATEMENT_TO_STATEMENT_SET);
        } else {
            callInserts(Collections.singletonList(operation));
        }
    }

    private void callInserts(List<ModifyOperation> operations) {
        printInfo(CliStrings.MESSAGE_SUBMITTING_STATEMENT);

        boolean sync = executor.getSessionConfig(sessionId).get(TABLE_DML_SYNC);
        if (sync) {
            printInfo(MESSAGE_WAIT_EXECUTE);
        }
        TableResult tableResult = executor.executeModifyOperations(sessionId, operations);
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

    public void callExplain(ExplainOperation operation) {
        printRawContent(operation);
    }

    public void callShowCreateTable(ShowCreateTableOperation operation) {
        printRawContent(operation);
    }

    public void printRawContent(Operation operation) {
        TableResult tableResult = executor.executeOperation(sessionId, operation);
        // show raw content instead of tableau style
        final String explanation =
                Objects.requireNonNull(tableResult.collect().next().getField(0)).toString();
        terminal.writer().println(explanation);
        terminal.flush();
    }

    private void callBeginStatementSet() {
        isStatementSetMode = true;
        statementSetOperations = new ArrayList<>();
        printInfo(CliStrings.MESSAGE_BEGIN_STATEMENT_SET);
    }

    private void callEndStatementSet() {
        if (isStatementSetMode) {
            isStatementSetMode = false;
            if (!statementSetOperations.isEmpty()) {
                callInserts(statementSetOperations);
            } else {
                printInfo(CliStrings.MESSAGE_NO_STATEMENT_IN_STATEMENT_SET);
            }
            statementSetOperations = null;
        } else {
            throw new SqlExecutionException(MESSAGE_STATEMENT_SET_END_CALL_ERROR);
        }
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
                    false,
                    CliUtils.getSessionTimeZone(executor.getSessionConfig(sessionId)));
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

    // --------------------------------------------------------------------------------------------

    private void closeTerminal() {
        try {
            terminal.close();
            terminal = null;
        } catch (IOException e) {
            // ignore
        }
    }

    private LineReader createLineReader(Terminal terminal) {
        // initialize line lineReader
        LineReader lineReader =
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
