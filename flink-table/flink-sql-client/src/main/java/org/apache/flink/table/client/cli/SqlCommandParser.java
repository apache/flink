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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.DescribeTableOperation;
import org.apache.flink.table.operations.ExplainOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.ShowCatalogsOperation;
import org.apache.flink.table.operations.ShowDatabasesOperation;
import org.apache.flink.table.operations.ShowFunctionsOperation;
import org.apache.flink.table.operations.ShowTablesOperation;
import org.apache.flink.table.operations.UseCatalogOperation;
import org.apache.flink.table.operations.UseDatabaseOperation;
import org.apache.flink.table.operations.ddl.AlterCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.AlterDatabaseOperation;
import org.apache.flink.table.operations.ddl.AlterTableOperation;
import org.apache.flink.table.operations.ddl.CreateCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateCatalogOperation;
import org.apache.flink.table.operations.ddl.CreateDatabaseOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.ddl.CreateTempSystemFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateViewOperation;
import org.apache.flink.table.operations.ddl.DropCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.DropCatalogOperation;
import org.apache.flink.table.operations.ddl.DropDatabaseOperation;
import org.apache.flink.table.operations.ddl.DropTableOperation;
import org.apache.flink.table.operations.ddl.DropTempSystemFunctionOperation;
import org.apache.flink.table.operations.ddl.DropViewOperation;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Simple parser for determining the type of command and its parameters.
 */
public final class SqlCommandParser {

	private SqlCommandParser() {
		// private
	}

	public static Optional<SqlCommandCall> parse(Parser sqlParser, String stmt) {
		// normalize
		stmt = stmt.trim();
		// remove ';' at the end
		if (stmt.endsWith(";")) {
			stmt = stmt.substring(0, stmt.length() - 1).trim();
		}

		// parse statement via sql parser first
		Optional<SqlCommandCall> callOpt = parseBySqlParser(sqlParser, stmt);
		if (callOpt.isPresent()) {
			return callOpt;
		} else {
			return parseByRegexMatching(stmt);
		}
	}

	private static Optional<SqlCommandCall> parseBySqlParser(Parser sqlParser, String stmt) {
		List<Operation> operations;
		try {
			operations = sqlParser.parse(stmt);
		} catch (Throwable e) {
			if (e instanceof ValidationException) {
				// can be parsed via sql parser, but is not validated.
				// throw exception directly
				throw new SqlExecutionException("Invalidate SQL statement.", e);
			}
			return Optional.empty();
		}
		if (operations.size() != 1) {
			throw new SqlExecutionException("Only single statement is supported now.");
		}

		final SqlCommand cmd;
		String[] operands = new String[] { stmt };
		Operation operation = operations.get(0);
		if (operation instanceof CatalogSinkModifyOperation) {
			boolean overwrite = ((CatalogSinkModifyOperation) operation).isOverwrite();
			cmd = overwrite ? SqlCommand.INSERT_OVERWRITE : SqlCommand.INSERT_INTO;
		} else if (operation instanceof CreateTableOperation) {
			cmd = SqlCommand.CREATE_TABLE;
		} else if (operation instanceof DropTableOperation) {
			cmd = SqlCommand.DROP_TABLE;
		} else if (operation instanceof AlterTableOperation) {
			cmd = SqlCommand.ALTER_TABLE;
		} else if (operation instanceof CreateViewOperation) {
			cmd = SqlCommand.CREATE_VIEW;
			CreateViewOperation op = (CreateViewOperation) operation;
			operands = new String[] { op.getViewIdentifier().asSerializableString(),
					op.getCatalogView().getOriginalQuery() };
		} else if (operation instanceof DropViewOperation) {
			cmd = SqlCommand.DROP_VIEW;
			operands = new String[] { ((DropViewOperation) operation).getViewIdentifier().asSerializableString() };
		} else if (operation instanceof CreateDatabaseOperation) {
			cmd = SqlCommand.CREATE_DATABASE;
		} else if (operation instanceof DropDatabaseOperation) {
			cmd = SqlCommand.DROP_DATABASE;
		} else if (operation instanceof AlterDatabaseOperation) {
			cmd = SqlCommand.ALTER_DATABASE;
		} else if (operation instanceof CreateCatalogOperation) {
			cmd = SqlCommand.CREATE_CATALOG;
		} else if (operation instanceof DropCatalogOperation) {
			cmd = SqlCommand.DROP_CATALOG;
		} else if (operation instanceof UseCatalogOperation) {
			cmd = SqlCommand.USE_CATALOG;
			operands = new String[] { String.format("`%s`", ((UseCatalogOperation) operation).getCatalogName()) };
		} else if (operation instanceof UseDatabaseOperation) {
			cmd = SqlCommand.USE;
			UseDatabaseOperation op = ((UseDatabaseOperation) operation);
			operands = new String[] { String.format("`%s`.`%s`", op.getCatalogName(), op.getDatabaseName()) };
		} else if (operation instanceof ShowCatalogsOperation) {
			cmd = SqlCommand.SHOW_CATALOGS;
			operands = new String[0];
		} else if (operation instanceof ShowDatabasesOperation) {
			cmd = SqlCommand.SHOW_DATABASES;
			operands = new String[0];
		} else if (operation instanceof ShowTablesOperation) {
			cmd = SqlCommand.SHOW_TABLES;
			operands = new String[0];
		} else if (operation instanceof ShowFunctionsOperation) {
			cmd = SqlCommand.SHOW_FUNCTIONS;
			operands = new String[0];
		} else if (operation instanceof CreateCatalogFunctionOperation ||
				operation instanceof CreateTempSystemFunctionOperation) {
			cmd = SqlCommand.CREATE_FUNCTION;
		} else if (operation instanceof DropCatalogFunctionOperation ||
				operation instanceof DropTempSystemFunctionOperation) {
			cmd = SqlCommand.DROP_FUNCTION;
		} else if (operation instanceof AlterCatalogFunctionOperation) {
			cmd = SqlCommand.ALTER_FUNCTION;
		} else if (operation instanceof ExplainOperation) {
			cmd = SqlCommand.EXPLAIN;
		} else if (operation instanceof DescribeTableOperation) {
			cmd = SqlCommand.DESCRIBE;
			operands = new String[] { ((DescribeTableOperation) operation).getSqlIdentifier().asSerializableString() };
		} else if (operation instanceof QueryOperation) {
			cmd = SqlCommand.SELECT;
		} else {
			cmd = null;
		}

		return cmd == null ? Optional.empty() : Optional.of(new SqlCommandCall(cmd, operands));
	}

	private static Optional<SqlCommandCall> parseByRegexMatching(String stmt) {
		// parse statement via regex matching
		for (SqlCommand cmd : SqlCommand.values()) {
			if (cmd.hasRegexPattern()) {
				final Matcher matcher = cmd.pattern.matcher(stmt);
				if (matcher.matches()) {
					final String[] groups = new String[matcher.groupCount()];
					for (int i = 0; i < groups.length; i++) {
						groups[i] = matcher.group(i + 1);
					}
					return cmd.operandConverter.apply(groups)
							.map((operands) -> {
								String[] newOperands = operands;
								if (cmd == SqlCommand.EXPLAIN) {
									// convert `explain xx` to `explain plan for xx`
									// which can execute through executeSql method
									newOperands = new String[] { "EXPLAIN PLAN FOR " + operands[0] };
								}
								return new SqlCommandCall(cmd, newOperands);
							});
				}
			}
		}
		return Optional.empty();
	}

	// --------------------------------------------------------------------------------------------

	private static final Function<String[], Optional<String[]>> NO_OPERANDS =
		(operands) -> Optional.of(new String[0]);

	private static final Function<String[], Optional<String[]>> SINGLE_OPERAND =
		(operands) -> Optional.of(new String[]{operands[0]});

	private static final int DEFAULT_PATTERN_FLAGS = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;

	/**
	 * Supported SQL commands.
	 */
	enum SqlCommand {
		QUIT(
			"(QUIT|EXIT)",
			NO_OPERANDS),

		CLEAR(
			"CLEAR",
			NO_OPERANDS),

		HELP(
			"HELP",
			NO_OPERANDS),

		SHOW_CATALOGS,

		SHOW_DATABASES,

		SHOW_TABLES,

		SHOW_FUNCTIONS,

		// FLINK-17396
		SHOW_MODULES(
			"SHOW\\s+MODULES",
			NO_OPERANDS),

		USE_CATALOG,

		USE,

		CREATE_CATALOG,

		DROP_CATALOG,

		DESC(
			"DESC\\s+(.*)",
			SINGLE_OPERAND),

		DESCRIBE,

		// supports both `explain xx` and `explain plan for xx` now
		// TODO should keep `explain xx` ?
		EXPLAIN(
			"EXPLAIN\\s+(.*)",
			SINGLE_OPERAND),

		CREATE_DATABASE,

		DROP_DATABASE,

		ALTER_DATABASE,

		CREATE_TABLE,

		DROP_TABLE,

		ALTER_TABLE,

		CREATE_VIEW,

		DROP_VIEW,

		CREATE_FUNCTION,

		DROP_FUNCTION,

		ALTER_FUNCTION,

		SELECT,

		INSERT_INTO,

		INSERT_OVERWRITE,

		SET(
			"SET(\\s+(\\S+)\\s*=(.*))?", // whitespace is only ignored on the left side of '='
			(operands) -> {
				if (operands.length < 3) {
					return Optional.empty();
				} else if (operands[0] == null) {
					return Optional.of(new String[0]);
				}
				return Optional.of(new String[]{operands[1], operands[2]});
			}),

		RESET(
			"RESET",
			NO_OPERANDS),

		SOURCE(
			"SOURCE\\s+(.*)",
			SINGLE_OPERAND);

		public final @Nullable Pattern pattern;
		public final @Nullable Function<String[], Optional<String[]>> operandConverter;

		SqlCommand() {
			this.pattern = null;
			this.operandConverter = null;
		}

		SqlCommand(String matchingRegex, Function<String[], Optional<String[]>> operandConverter) {
			this.pattern = Pattern.compile(matchingRegex, DEFAULT_PATTERN_FLAGS);
			this.operandConverter = operandConverter;
		}

		@Override
		public String toString() {
			return super.toString().replace('_', ' ');
		}

		public boolean hasOperands() {
			return operandConverter != NO_OPERANDS;
		}

		public boolean hasRegexPattern() {
			return pattern != null;
		}
	}

	/**
	 * Call of SQL command with operands and command type.
	 */
	public static class SqlCommandCall {
		public final SqlCommand command;
		public final String[] operands;

		public SqlCommandCall(SqlCommand command, String[] operands) {
			this.command = command;
			this.operands = operands;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			SqlCommandCall that = (SqlCommandCall) o;
			return command == that.command && Arrays.equals(operands, that.operands);
		}

		@Override
		public int hashCode() {
			int result = Objects.hash(command);
			result = 31 * result + Arrays.hashCode(operands);
			return result;
		}

		@Override
		public String toString() {
			return command + "(" + Arrays.toString(operands) + ")";
		}
	}
}
