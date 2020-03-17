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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.cli.SqlCommandParser.SqlCommandCall;
import org.apache.flink.table.client.config.entries.ViewEntry;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ProgramTargetDescriptor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;

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

/**
 * SQL CLI client.
 */
public class CliClient {

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
	public CliClient(Terminal terminal, String sessionId, Executor executor) {
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
	public CliClient(String sessionId, Executor executor) {
		this(createDefaultTerminal(), sessionId, executor);
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
		final Optional<SqlCommandCall> parsedLine = SqlCommandParser.parse(line);
		if (!parsedLine.isPresent()) {
			printError(CliStrings.MESSAGE_UNKNOWN_SQL);
		}
		return parsedLine;
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
			case SHOW_DATABASES:
				callShowDatabases();
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
				callCreateTable(cmdCall);
				break;
			case DROP_TABLE:
				callDropTable(cmdCall);
				break;
			case CREATE_VIEW:
				callCreateView(cmdCall);
				break;
			case DROP_VIEW:
				callDropView(cmdCall);
				break;
			case SOURCE:
				callSource(cmdCall);
				break;
			case CREATE_DATABASE:
				callCreateDatabase(cmdCall);
				break;
			case DROP_DATABASE:
				callDropDatabase(cmdCall);
				break;
			case ALTER_DATABASE:
				callAlterDatabase(cmdCall);
				break;
			case ALTER_TABLE:
				callAlterTable(cmdCall);
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
		executor.resetSessionProperties(sessionId);
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
			executor.setSessionProperty(sessionId, cmdCall.operands[0], cmdCall.operands[1].trim());
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
			catalogs = executor.listCatalogs(sessionId);
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

	private void callShowDatabases() {
		final List<String> dbs;
		try {
			dbs = executor.listDatabases(sessionId);
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

	private void callShowTables() {
		final List<String> tables;
		try {
			tables = executor.listTables(sessionId);
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
			functions = executor.listFunctions(sessionId);
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

	private void callUseCatalog(SqlCommandCall cmdCall) {
		try {
			executor.useCatalog(sessionId, cmdCall.operands[0]);
		} catch (SqlExecutionException e) {
			printExecutionException(e);
			return;
		}
		terminal.flush();
	}

	private void callUseDatabase(SqlCommandCall cmdCall) {
		try {
			executor.useDatabase(sessionId, cmdCall.operands[0]);
		} catch (SqlExecutionException e) {
			printExecutionException(e);
			return;
		}
		terminal.flush();
	}

	private void callDescribe(SqlCommandCall cmdCall) {
		final TableSchema schema;
		try {
			schema = executor.getTableSchema(sessionId, cmdCall.operands[0]);
		} catch (SqlExecutionException e) {
			printExecutionException(e);
			return;
		}
		terminal.writer().println(schema.toString());
		terminal.flush();
	}

	private void callExplain(SqlCommandCall cmdCall) {
		final String explanation;
		try {
			explanation = executor.explainStatement(sessionId, cmdCall.operands[0]);
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

	private void callCreateTable(SqlCommandCall cmdCall) {
		try {
			executor.createTable(sessionId, cmdCall.operands[0]);
			printInfo(CliStrings.MESSAGE_TABLE_CREATED);
		} catch (SqlExecutionException e) {
			printExecutionException(e);
			return;
		}
	}

	private void callDropTable(SqlCommandCall cmdCall) {
		try {
			executor.dropTable(sessionId, cmdCall.operands[0]);
			printInfo(CliStrings.MESSAGE_TABLE_REMOVED);
		} catch (SqlExecutionException e) {
			printExecutionException(e);
		}
	}

	private void callCreateView(SqlCommandCall cmdCall) {
		final String name = cmdCall.operands[0];
		final String query = cmdCall.operands[1];

		final ViewEntry previousView = executor.listViews(sessionId).get(name);
		if (previousView != null) {
			printExecutionError(CliStrings.MESSAGE_VIEW_ALREADY_EXISTS);
			return;
		}

		try {
			// perform and validate change
			executor.addView(sessionId, name, query);
			printInfo(CliStrings.MESSAGE_VIEW_CREATED);
		} catch (SqlExecutionException e) {
			// rollback change
			executor.removeView(sessionId, name);
			printExecutionException(e);
		}
	}

	private void callDropView(SqlCommandCall cmdCall) {
		final String name = cmdCall.operands[0];
		final ViewEntry view = executor.listViews(sessionId).get(name);
		if (view == null) {
			printExecutionError(CliStrings.MESSAGE_VIEW_NOT_FOUND);
			return;
		}

		try {
			// perform and validate change
			executor.removeView(sessionId, name);
			printInfo(CliStrings.MESSAGE_VIEW_REMOVED);
		} catch (SqlExecutionException e) {
			// rollback change
			executor.addView(sessionId, view.getName(), view.getQuery());
			printExecutionException(CliStrings.MESSAGE_VIEW_NOT_REMOVED, e);
		}
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

	private void callCreateDatabase(SqlCommandCall cmdCall) {
		final String createDatabaseStmt = cmdCall.operands[0];
		try {
			executor.executeUpdate(sessionId, createDatabaseStmt);
			printInfo(CliStrings.MESSAGE_DATABASE_CREATED);
		} catch (SqlExecutionException e) {
			printExecutionException(e);
		}
	}

	private void callDropDatabase(SqlCommandCall cmdCall) {
		final String dropDatabaseStmt = cmdCall.operands[0];
		try {
			executor.executeUpdate(sessionId, dropDatabaseStmt);
			printInfo(CliStrings.MESSAGE_DATABASE_REMOVED);
		} catch (SqlExecutionException e) {
			printExecutionException(e);
		}
	}

	private void callAlterDatabase(SqlCommandCall cmdCall) {
		final String alterDatabaseStmt = cmdCall.operands[0];
		try {
			executor.executeUpdate(sessionId, alterDatabaseStmt);
			printInfo(CliStrings.MESSAGE_DATABASE_ALTER_SUCCEEDED);
		} catch (SqlExecutionException e) {
			printExecutionException(CliStrings.MESSAGE_DATABASE_ALTER_FAILED, e);
		}
	}

	private void callAlterTable(SqlCommandCall cmdCall) {
		final String alterTableStmt = cmdCall.operands[0];
		try {
			executor.executeUpdate(sessionId, alterTableStmt);
			printInfo(CliStrings.MESSAGE_ALTER_TABLE_SUCCEEDED);
		} catch (SqlExecutionException e) {
			printExecutionException(CliStrings.MESSAGE_ALTER_TABLE_FAILED, e);
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
