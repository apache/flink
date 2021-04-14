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

package org.apache.flink.table.client;

import org.apache.flink.table.client.cli.CliClient;
import org.apache.flink.table.client.cli.CliOptions;
import org.apache.flink.table.client.gateway.Executor;

import org.jline.terminal.Terminal;

import java.nio.file.Path;

/** Mocked Sql Client that hijacks System.in and System.out. */
public class MockedSqlClient extends SqlClient {

    private final Terminal terminal;

    public MockedSqlClient(boolean isEmbedded, CliOptions options, Terminal terminal) {
        super(isEmbedded, options);
        this.terminal = terminal;
    }

    public static void mockedMain(String[] args, Terminal terminal) {
        startClient(
                args, (isEmbedded, options) -> new MockedSqlClient(isEmbedded, options, terminal));
    }

    @Override
    protected CliClient createCliClient(String sessionId, Executor executor, Path historyFilePath) {
        return new CliClient(() -> terminal, sessionId, executor, historyFilePath, null);
    }
}
