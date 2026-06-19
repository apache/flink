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

package org.apache.flink.table.operations.ddl;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.StatementSetOperation;
import org.apache.flink.util.Preconditions;

/** Operation to describe an {@code COMPILE PLAN} statement. */
@Internal
public class CompilePlanOperation implements Operation {

    private final String filePath;
    private final boolean ifNotExists;
    private final Operation operation;

    public CompilePlanOperation(String filePath, boolean ifNotExists, Operation operation) {
        Preconditions.checkArgument(
                operation instanceof StatementSetOperation || operation instanceof ModifyOperation,
                "child operation of CompileOperation must be either a ModifyOperation or a StatementSetOperation");
        this.filePath = filePath;
        this.ifNotExists = ifNotExists;
        this.operation = operation;
    }

    public String getFilePath() {
        return filePath;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public Operation getOperation() {
        return operation;
    }

    @Override
    public String asSummaryString() {
        return String.format(
                ifNotExists ? "COMPILE PLAN '%s' IF NOT EXISTS FOR %s" : "COMPILE PLAN '%s' FOR %s",
                filePath,
                operation.asSummaryString());
    }
}
