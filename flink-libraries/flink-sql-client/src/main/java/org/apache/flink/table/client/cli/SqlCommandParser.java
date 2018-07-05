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

import java.util.Optional;

/**
 * Simple parser for determining the type of command and its parameters.
 */
public final class SqlCommandParser {

	private SqlCommandParser() {
		// private
	}

	public static Optional<SqlCommandCall> parse(String stmt) {
		String trimmed = stmt.trim();
		// remove ';' at the end because many people type it intuitively
		if (trimmed.endsWith(";")) {
			trimmed = trimmed.substring(0, trimmed.length() - 1);
		}
		for (SqlCommand cmd : SqlCommand.values()) {
			int pos = 0;
			int tokenCount = 0;
			for (String token : trimmed.split("\\s")) {
				pos += token.length() + 1; // include space character
				// check for content
				if (token.length() > 0) {
					// match
					if (tokenCount < cmd.tokens.length && token.equalsIgnoreCase(cmd.tokens[tokenCount])) {
						if (tokenCount == cmd.tokens.length - 1) {
							final SqlCommandCall call = new SqlCommandCall(
								cmd,
								splitOperands(cmd, trimmed, trimmed.substring(Math.min(pos, trimmed.length())))
							);
							return Optional.of(call);
						}
					} else {
						// next sql command
						break;
					}
					tokenCount++; // check next token
				}
			}
		}
		return Optional.empty();
	}

	private static String[] splitOperands(SqlCommand cmd, String originalCall, String operands) {
		switch (cmd) {
			case SET:
				final int delimiter = operands.indexOf('=');
				if (delimiter < 0) {
					return new String[] {};
				} else {
					return new String[] {operands.substring(0, delimiter), operands.substring(delimiter + 1)};
				}
			case SELECT:
			case INSERT_INTO:
				return new String[] {originalCall};
			default:
				return new String[] {operands};
		}
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Supported SQL commands.
	 */
	enum SqlCommand {
		QUIT("quit"),
		EXIT("exit"),
		CLEAR("clear"),
		HELP("help"),
		SHOW_TABLES("show tables"),
		SHOW_FUNCTIONS("show functions"),
		DESCRIBE("describe"),
		EXPLAIN("explain"),
		SELECT("select"),
		INSERT_INTO("insert into"),
		SET("set"),
		RESET("reset"),
		SOURCE("source");

		public final String command;
		public final String[] tokens;

		SqlCommand(String command) {
			this.command = command;
			this.tokens = command.split(" ");
		}

		@Override
		public String toString() {
			return command.toUpperCase();
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
	}
}
