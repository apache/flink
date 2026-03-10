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

package org.apache.flink.table.runtime.operators.join.deltajoin;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.table.data.RowData;

import java.util.Map;

/**
 * A {@link OpenContext} defines shared resources between {@link StreamingDeltaJoinOperator} and two
 * {@link AsyncDeltaJoinRunner}.
 */
public class DeltaJoinOpenContext implements OpenContext {

    private final DeltaJoinCache cache;
    private final MailboxExecutor mailboxExecutor;
    private final Map<Integer, AsyncFunction<RowData, Object>> lookupFunctions;

    public DeltaJoinOpenContext(
            DeltaJoinCache cache,
            MailboxExecutor mailboxExecutor,
            Map<Integer, AsyncFunction<RowData, Object>> lookupFunctions) {
        this.cache = cache;
        this.mailboxExecutor = mailboxExecutor;
        this.lookupFunctions = lookupFunctions;
    }

    public DeltaJoinCache getCache() {
        return cache;
    }

    public MailboxExecutor getMailboxExecutor() {
        return mailboxExecutor;
    }

    public Map<Integer, AsyncFunction<RowData, Object>> getLookupFunctions() {
        return lookupFunctions;
    }
}
