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

package org.apache.flink.table.client.cli.utils;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.ChangelogFunction;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

/**
 * Process Table Function used by {@code function.q} to exercise {@code DESCRIBE FUNCTION EXTENDED}.
 *
 * <p>Carries: a state entry with TTL (via {@link StateHint}), a table arg with multiple traits (via
 * {@link ArgumentHint}), an {@code onTimer} method, and an implementation of {@link
 * ChangelogFunction} so that the {@code emits updates} and {@code uses timers} flags both light up.
 * The bodies are no-ops since only the introspection output is asserted.
 */
public class DescribeFunctionTestPtf extends ProcessTableFunction<Long>
        implements ChangelogFunction {

    public void eval(
            @StateHint(type = @DataTypeHint("ROW<count BIGINT>"), ttl = "1 d") Row state,
            @ArgumentHint({ArgumentTrait.SET_SEMANTIC_TABLE, ArgumentTrait.OPTIONAL_PARTITION_BY})
                    Row input) {}

    public void onTimer(Row state) {}

    @Override
    public ChangelogMode getChangelogMode(ChangelogContext changelogContext) {
        return ChangelogMode.insertOnly();
    }
}
