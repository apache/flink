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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A {@link Operation} that describes the statement set, e.g.
 *
 * <pre>
 * STATEMENT SET BEGIN
 *   INSERT INTO xxxx;
 *   INSERT INTO xxxx;
 * END;
 * </pre>
 */
@Internal
public final class StatementSetOperation implements Operation {

    private final List<ModifyOperation> operations;

    public StatementSetOperation(List<ModifyOperation> operations) {
        this.operations = Preconditions.checkNotNull(operations);
    }

    @Override
    public String asSummaryString() {
        return String.format(
                "StatementSet[%s]",
                operations.stream()
                        .map(Operation::asSummaryString)
                        .collect(Collectors.joining(",")));
    }

    public List<ModifyOperation> getOperations() {
        return operations;
    }
}
