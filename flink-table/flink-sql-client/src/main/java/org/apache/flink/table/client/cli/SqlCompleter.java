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

import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SqlExecutionException;

import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.utils.AttributedString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * SQL code completer.
 */
public class SqlCompleter implements Completer {

	private static final Logger LOG = LoggerFactory.getLogger(SqlCompleter.class);

	public static final String[] COMMAND_HINTS = getCommandHints();

	private String sessionId;

	private Executor executor;

	public SqlCompleter(String sessionId, Executor executor) {
		this.sessionId = sessionId;
		this.executor = executor;
	}

	public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
		String statement = line.line();

		// remove ';' at the end
		if (statement.endsWith(";")) {
			statement = statement.substring(0, statement.length() - 1);
		}

		// handle SQL client specific commands
		final String statementNormalized = statement.toUpperCase().trim();
		for (String commandHint : COMMAND_HINTS) {
			if (commandHint.startsWith(statementNormalized) && line.cursor() < commandHint.length()) {
				candidates.add(createCandidate(commandHint));
			}
		}

		// fallback to Table API hinting
		try {
			executor.completeStatement(sessionId, statement, line.cursor())
				.forEach(hint -> candidates.add(createCandidate(hint)));
		} catch (SqlExecutionException e) {
			LOG.debug("Could not complete statement at " + line.cursor() + ":" + statement, e);
		}
	}

	private Candidate createCandidate(String hint) {
		return new Candidate(AttributedString.stripAnsi(hint), hint, null, null, null, null, true);
	}

	private static String[] getCommandHints() {
		final SqlCommandParser.SqlCommand[] commands = SqlCommandParser.SqlCommand.values();
		final String[] hints = new String[commands.length];
		for (int i = 0; i < commands.length; i++) {
			final SqlCommandParser.SqlCommand command = commands[i];
			// add final ";" for convenience if no operands can follow
			if (command.hasOperands()) {
				hints[i] = command.toString();
			} else {
				hints[i] = command.toString() + ";";
			}
		}
		return hints;
	}
}
