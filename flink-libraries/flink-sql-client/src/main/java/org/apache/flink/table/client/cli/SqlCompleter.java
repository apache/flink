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
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.local.LocalExecutor;

import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.utils.AttributedString;

import java.util.List;

/**
 * SQL Completer.
 */
public class SqlCompleter implements Completer {
	private SessionContext context;
	private Executor executor;

	public SqlCompleter(SessionContext context, Executor executor) {
		this.context = context;
		this.executor = executor;
	}

	public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
		if (executor instanceof LocalExecutor) {
			try {
				LocalExecutor localExecutor = (LocalExecutor) executor;
				List<String> hints = localExecutor.getCompletionHints(context, line.line(), line.cursor());
				hints.forEach(hint->
					candidates.add(new Candidate(AttributedString.stripAnsi(hint), hint , null, null, null, null, true)));
			} catch (Exception e) {
				//ignore completer exception;
			}
		}
	}

}
