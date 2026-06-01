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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.types.Row;

import javax.annotation.Nullable;

/**
 * Per-invocation context for an eval() or onTimer() call in the test harness. partitionKey is
 * always set. For eval(), row and tableArgumentName are set. For onTimer(), firingTimer is set.
 */
class InvocationContext {
    final Row partitionKey;
    @Nullable final Row row;
    @Nullable final String tableArgumentName;
    @Nullable final Timer firingTimer;

    private InvocationContext(
            Row partitionKey,
            @Nullable Row row,
            @Nullable String tableArgumentName,
            @Nullable Timer firingTimer) {
        this.partitionKey = partitionKey;
        this.row = row;
        this.tableArgumentName = tableArgumentName;
        this.firingTimer = firingTimer;
    }

    static InvocationContext forEval(Row partitionKey, Row row, String tableArgumentName) {
        return new InvocationContext(partitionKey, row, tableArgumentName, null);
    }

    static InvocationContext forTimer(Timer timer) {
        return new InvocationContext(timer.partitionKey, null, null, timer);
    }

    boolean isTimerInvocation() {
        return firingTimer != null;
    }

    boolean isEvalInvocation() {
        return tableArgumentName != null;
    }
}
