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

package org.apache.flink.table.planner.plan.nodes.exec.spec;

import org.apache.flink.table.planner.hint.JoinStrategy;
import org.apache.flink.table.planner.hint.LookupJoinHintOptions;

import org.apache.calcite.rel.hint.RelHint;

import java.util.HashMap;
import java.util.Map;

/** Test util for lookup hint. */
public class LookupJoinHintTestUtil {
    public static RelHint completeLookupHint = getLookupJoinHint(true, true);
    public static RelHint lookupHintWithAsync = getLookupJoinHint(true, false);
    public static RelHint lookupHintWithRetry = getLookupJoinHint(false, true);
    public static RelHint lookupHintWithTableOnly = getLookupJoinHint(false, false);

    public static RelHint getLookupJoinHint(String table, boolean withAsync, boolean withRetry) {
        Map<String, String> kvOptions = getLookupJoinHintOptions(table, withAsync, withRetry);
        return RelHint.builder(JoinStrategy.LOOKUP.getJoinHintName())
                .hintOptions(kvOptions)
                .build();
    }

    public static RelHint getLookupJoinHint(boolean withAsync, boolean withRetry) {
        return getLookupJoinHint("TestTable", withAsync, withRetry);
    }

    public static Map<String, String> getLookupJoinHintOptions(
            String table, boolean withAsync, boolean withRetry) {
        Map<String, String> kvOptions = new HashMap<>();
        kvOptions.put(LookupJoinHintOptions.LOOKUP_TABLE.key(), table);
        if (withAsync) {
            addAsyncOptions(kvOptions);
        }
        if (withRetry) {
            addRetryOptions(kvOptions);
        }
        return kvOptions;
    }

    public static void addAsyncOptions(Map<String, String> kvOptions) {
        kvOptions.put(LookupJoinHintOptions.ASYNC_LOOKUP.key(), "true");
        kvOptions.put(LookupJoinHintOptions.ASYNC_CAPACITY.key(), "1000");
        kvOptions.put(LookupJoinHintOptions.ASYNC_OUTPUT_MODE.key(), "allow_unordered");
        kvOptions.put(LookupJoinHintOptions.ASYNC_TIMEOUT.key(), "300 s");
    }

    public static void addRetryOptions(Map<String, String> kvOptions) {
        kvOptions.put(LookupJoinHintOptions.RETRY_PREDICATE.key(), "lookup_miss");
        kvOptions.put(LookupJoinHintOptions.RETRY_STRATEGY.key(), "fixed_delay");
        kvOptions.put(LookupJoinHintOptions.FIXED_DELAY.key(), "155 ms");
        kvOptions.put(LookupJoinHintOptions.MAX_ATTEMPTS.key(), "10");
    }
}
