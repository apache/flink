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
import org.apache.flink.table.client.cli.SqlCommandParser.SqlCommandCall;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ProgramTargetDescriptor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.utils.PrintUtils;
import org.apache.flink.util.CollectionUtil;

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

import java.io.IOError;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * SQL CLI client.
 */
public class CliClient implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(CliClient.class);

	private final Executor executor;

	private final String sessionId;

	private final Terminal terminal;

	private final LineReader lineReader;

	private final String prompt;

	private boolean isRunning;

	private static final int PLAIN_TERMINAL_WIDTH = 80;

	private static final int PLAIN_TERMINAL_HEIGHT = 30;

	private static final int SOURCE_MAX_SIZE = 50_000;

	/**
	 * Creates a CLI instance with a custom terminal. Make sure to close the CLI instance
	 * afterwards using {@link #close()}.
	 */
	@VisibleForTesting
	public CliClient(Terminal terminal, String sessionId, Executor executor, Path historyFilePath) {
		this.terminal = terminal;
		this.sessionId = sessionId;
		this.executor = executor;

		// make space from previous output and test the writer
		terminal.writer().println();
		terminal.writer().flush();

		// initialize line lineReader
		lineReader = LineReaderBuilder.builder()
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
		prompt = new AttributedStringBuilder()
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
		this(createDefaultTerminal(), sessionId, executor, historyFilePath);
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

	/**
	 * Opens the interactive CLI shell.
	 */
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
				line = lineReader.readLine(prompt, null, (MaskingCallback) null, null);
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

	/**
	 * Closes the CLI instance.
	 */
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
		return parsedStatement.map(cmdCall -> {
			switch (cmdCall.command) {
				case INSERT_INTO:
				case INSERT_OVERWRITE:
					return callInsert(cmdCall);
				default:
					printError(CliStrings.MESSAGE_UNSUPPORTED_SQL);
					return false;
			}
		}).orElse(false);
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
				callShowCatalogs();
				break;
			case SHOW_CURRENT_CATALOG:
				callShowCurrentCatalog();
				break;
			case SHOW_DATABASES:
				callShowDatabases();
				break;
			case SHOW_CURRENT_DATABASE:
				callShowCurrentDatabase();
				break;
			case SHOW_TABLES:
				callShowTables();
				break;
			case SHOW_FUNCTIONS:
				callShowFunctions();
				break;
			case SHOW_MODULES:
				callShowModules();
				break;
			case SHOW_PARTITIONS:
				callShowPartitions(cmdCall);
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
				callDdl(cmdCall.operands[0], CliStrings.MESSAGE_ALTER_VIEW_SUCCEEDED, CliStrings.MESSAGE_ALTER_VIEW_FAILED);
				break;
			case CREATE_FUNCTION:
				callDdl(cmdCall.operands[0], CliStrings.MESSAGE_FUNCTION_CREATED);
				break;
			case DROP_FUNCTION:
				callDdl(cmdCall.operands[0], CliStrings.MESSAGE_FUNCTION_REMOVED);
				break;
			case ALTER_FUNCTION:
				callDdl(cmdCall.operands[0], CliStrings.MESSAGE_ALTER_FUNCTION_SUCCEEDED,
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
				callDdl(cmdCall.operands[0], CliStrings.MESSAGE_ALTER_DATABASE_SUCCEEDED,
						CliStrings.MESSAGE_ALTER_DATABASE_FAILED);
				break;
			case ALTER_TABLE:
				callDdl(cmdCall.operands[0], CliStrings.MESSAGE_ALTER_TABLE_SUCCEEDED,
						CliStrings.MESSAGE_ALTER_TABLE_FAILED);
				break;
			case CREATE_CATALOG:
				callDdl(cmdCall.operands[0], CliStrings.MESSAGE_CATALOG_CREATED);
				break;
			case DROP_CATALOG:
				callDdl(cmdCall.operands[0], CliStrings.MESSAGE_CATALOG_REMOVED);
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
				terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY).toAnsi());
			} else {
				properties
					.entrySet()
					.stream()
					.map((e) -> e.getKey() + "=" + e.getValue())
					.sorted()
					.forEach((p) -> terminal.writer().println(p));
			}
		}
		// set a property
		else {
			try {
				executor.setSessionProperty(sessionId, cmdCall.operands[0], cmdCall.operands[1].trim());
			} catch (SqlExecutionException e) {
				printExecutionException(e);
				return;
			}
			terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_SET).toAnsi());
		}
		terminal.flush();
	}

	private void callHelp() {
		terminal.writer().println(CliStrings.MESSAGE_HELP);
		terminal.flush();
	}

	private void callShowCatalogs() {
		final List<String> catalogs;
		try {
			catalogs = getShowResult("CATALOGS");
		} catch (SqlExecutionException e) {
			printExecutionException(e);
			return;
		}
		if (catalogs.isEmpty()) {
			terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY).toAnsi());
		} else {
			catalogs.forEach((v) -> terminal.writer().println(v));
		}
		terminal.flush();
	}

	private void callShowCurrentCatalog() {
		String currentCatalog;
		try {
			currentCatalog = executor.executeSql(sessionId, "SHOW CURRENT CATALOG").collect().next().toString();
		} catch (SqlExecutionException e) {
			printExecutionException(e);
			return;
		}
		terminal.writer().println(currentCatalog);
		terminal.flush();
	}

	private void callShowDatabases() {
		final List<String> dbs;
		try {
			dbs = getShowResult("DATABASES");
		} catch (SqlExecutionException e) {
			printExecutionException(e);
			return;
		}
		if (dbs.isEmpty()) {
			terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY).toAnsi());
		} else {
			dbs.forEach((v) -> terminal.writer().println(v));
		}
		terminal.flush();
	}

	private void callShowCurrentDatabase() {
		String currentDatabase;
		try {
			currentDatabase = executor.executeSql(sessionId, "SHOW CURRENT DATABASE").collect().next().toString();
		} catch (SqlExecutionException e) {
			printExecutionException(e);
			return;
		}
		terminal.writer().println(currentDatabase);
		terminal.flush();
	}

	private void callShowTables() {
		final List<String> tables;
		try {
			tables = getShowResult("TABLES");
		} catch (SqlExecutionException e) {
			printExecutionException(e);
			return;
		}
		if (tables.isEmpty()) {
			terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY).toAnsi());
		} else {
			tables.forEach((v) -> terminal.writer().println(v));
		}
		terminal.flush();
	}

	private void callShowFunctions() {
		final List<String> functions;
		try {
			functions = getShowResult("FUNCTIONS");
		} catch (SqlExecutionException e) {
			printExecutionException(e);
			return;
		}
		if (functions.isEmpty()) {
			terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY).toAnsi());
		} else {
			Collections.sort(functions);
			functions.forEach((v) -> terminal.writer().println(v));
		}
		terminal.flush();
	}

	private List<String> getShowResult(String objectToShow) {
		TableResult tableResult = executor.executeSql(sessionId, "SHOW " + objectToShow);
		return CollectionUtil.iteratorToList(tableResult.collect())
				.stream()
				.map(r -> checkNotNull(r.getField(0)).toString())
				.collect(Collectors.toList());
	}

	private List<String> getShowResult(SqlCommandCall cmdCall) {
		TableResult tableResult = executor.executeSql(sessionId, cmdCall.operands[0]);
		return CollectionUtil.iteratorToList(tableResult.collect())
			.stream()
			.map(r -> checkNotNull(r.getField(0)).toString())
			.collect(Collectors.toList());
	}

	private void callShowModules() {
		final List<String> modules;
		try {
			modules = executor.listModules(sessionId);
		} catch (SqlExecutionException e) {
			printExecutionException(e);
			return;
		}
		if (modules.isEmpty()) {
			terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY).toAnsi());
		} else {
			// modules are already in the loaded order
			modules.forEach((v) -> terminal.writer().println(v));
		}
		terminal.flush();
	}

	private void callShowPartitions(SqlCommandCall cmdCall) {
		final List<String> partitions;
		try {
			partitions = getShowResult(cmdCall);
		} catch (SqlExecutionException e) {
			printExecutionException(e);
			return;
		}
		if (partitions.isEmpty()) {
			terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY).toAnsi());
		} else {
			partitions.forEach((v) -> terminal.writer().println(v));
		}
		terminal.flush();
	}

	private void callUseCatalog(SqlCommandCall cmdCall) {
		try {
			executor.executeSql(sessionId, "USE CATALOG " + cmdCall.operands[0]);
		} catch (SqlExecutionException e) {
			printExecutionException(e);
			return;
		}
		terminal.flush();
	}

	private void callUseDatabase(SqlCommandCall cmdCall) {
		try {
			executor.executeSql(sessionId, "USE " + cmdCall.operands[0]);
		} catch (SqlExecutionException e) {
			printExecutionException(e);
			return;
		}
		terminal.flush();
	}

	private void callDescribe(SqlCommandCall cmdCall) {
		final TableResult tableResult;
		try {
			tableResult = executor.executeSql(sessionId, "DESCRIBE " + cmdCall.operands[0]);
		} catch (SqlExecutionException e) {
			printExecutionException(e);
			return;
		}
		PrintUtils.printAsTableauForm(
				tableResult.getTableSchema(),
				tableResult.collect(),
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
			try (CliTableauResultView tableauResultView = new CliTableauResultView(
					terminal, executor, sessionId, resultDesc)) {
				if (resultDesc.isMaterialized()) {
					tableauResultView.displayBatchResults();
				} else {
					tableauResultView.displayStreamResults();
				}
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
			final ProgramTargetDescriptor programTarget = executor.executeUpdate(sessionId, cmdCall.operands[0]);
			terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_STATEMENT_SUBMITTED).toAnsi());
			terminal.writer().println(programTarget.toString());
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
		terminal.writer().println(CliStrings.messageError(CliStrings.MESSAGE_SQL_EXECUTION_ERROR, message).toAnsi());
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

	private static Terminal createDefaultTerminal() {
		try {
			return TerminalBuilder.builder()
				.name(CliStrings.CLI_NAME)
				.build();
		} catch (IOException e) {
			throw new SqlClientException("Error opening command line interface.", e);
		}
	}
}
