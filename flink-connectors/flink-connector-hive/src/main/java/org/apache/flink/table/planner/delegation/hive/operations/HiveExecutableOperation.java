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

package org.apache.flink.table.planner.delegation.hive.operations;

import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.operations.ExecutableOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.delegation.hive.HiveOperationExecutor;

/**
 * An {@link ExecutableOperation} for Hive dialect. It'll wrap the actual {@link Operation}, and
 * delegate the execution logic of the wrapped operation to {@link HiveOperationExecutor}.
 *
 * <p>Todo: It's just a temporary way just for simplify, should refactor it to make each of the
 * extra {@link Operation}s customized for Hive execute it's own logic in it's own inner `execute`
 * method in FLINK-31607
 */
public class HiveExecutableOperation implements ExecutableOperation {

    private final Operation innerOperation;

    public HiveExecutableOperation(Operation innerOperation) {
        this.innerOperation = innerOperation;
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        HiveOperationExecutor operationExecutor = new HiveOperationExecutor(ctx);
        return operationExecutor.executeOperation(innerOperation).get();
    }

    public Operation getInnerOperation() {
        return innerOperation;
    }

    @Override
    public String asSummaryString() {
        return innerOperation.asSummaryString();
    }
}
