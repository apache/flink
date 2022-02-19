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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.operations.Operation;

import java.util.Optional;

/** SqlClient command parser. */
@Internal
interface SqlCommandParser {

    /**
     * Parses given command statement.
     *
     * @param command the sql client command to evaluate.
     * @return the optional value of {@link Operation} parsed. It would be empty when the command is
     *     "" or ";".
     * @throws SqlExecutionException if any error happen while parsing or validating the command.
     */
    Optional<Operation> parseCommand(String command) throws SqlExecutionException;
}
