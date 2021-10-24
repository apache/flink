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

import org.jline.reader.Buffer;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

import java.util.List;

/** Completer which aggregates SQL and command completers. */
public class FlinkSqlCompleter implements Completer {

    private final SqlCompleter sqlCompleter;
    private final CliCommandAggregateCompleter cliCommandAggregateCompleter;

    public FlinkSqlCompleter(String sessionId, Executor executor) {
        sqlCompleter = new SqlCompleter(sessionId, executor);
        cliCommandAggregateCompleter = new CliCommandAggregateCompleter(sessionId, executor);
    }

    @Override
    public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
        int i = 0;
        Buffer buffer = reader.getBuffer();
        while (i < buffer.length() && Character.isWhitespace(buffer.atChar(i))) {
            i++;
        }
        StringBuilder firstWord = new StringBuilder();
        while (i < buffer.length() && !Character.isWhitespace(buffer.atChar(i))) {
            firstWord.append((char) buffer.atChar(i));
            i++;
        }
        String string = firstWord.toString();

        final Completer completer =
                (string.isEmpty() || CliStrings.SQLCliCommandsDescriptions.isCommand(string))
                        ? cliCommandAggregateCompleter
                        : sqlCompleter;
        completer.complete(reader, line, candidates);
    }
}
