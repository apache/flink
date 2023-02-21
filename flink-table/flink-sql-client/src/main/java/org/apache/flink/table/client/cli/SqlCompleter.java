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

/** SQL code completer. */
public class SqlCompleter implements Completer {

    private static final Logger LOG = LoggerFactory.getLogger(SqlCompleter.class);
    private final Executor executor;

    public SqlCompleter(Executor executor) {
        this.executor = executor;
    }

    public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
        String statement = line.line();
        try {
            executor.completeStatement(statement, line.cursor())
                    .forEach(hint -> candidates.add(createCandidate(hint)));
        } catch (SqlExecutionException e) {
            LOG.debug("Could not complete statement at " + line.cursor() + ":" + statement, e);
        }
    }

    private Candidate createCandidate(String hint) {
        return new Candidate(AttributedString.stripAnsi(hint), hint, null, null, null, null, true);
    }
}
