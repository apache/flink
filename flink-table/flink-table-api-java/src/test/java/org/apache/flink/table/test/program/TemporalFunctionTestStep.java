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

package org.apache.flink.table.test.program;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.TemporalTableFunction;

/** Test step for registering a (temporary) (system or catalog) function. */
public final class TemporalFunctionTestStep implements TestStep {

    /** Whether function should be persisted in a catalog or not. */
    enum FunctionBehavior {
        SYSTEM,
        CATALOG
    }

    public final FunctionBehavior behavior;
    public final String name;
    public final String table;
    public final Expression timeAttribute;
    public final Expression primaryKey;

    TemporalFunctionTestStep(
            FunctionBehavior behavior,
            String name,
            String table,
            Expression timeAttribute,
            Expression primaryKey) {
        this.behavior = behavior;
        this.name = name;
        this.table = table;
        this.timeAttribute = timeAttribute;
        this.primaryKey = primaryKey;
    }

    @Override
    public TestKind getKind() {
        return TestKind.TEMPORAL_FUNCTION;
    }

    public void apply(TableEnvironment env) {
        TemporalTableFunction function =
                env.from(table).createTemporalTableFunction(timeAttribute, primaryKey);
        if (behavior == FunctionBehavior.SYSTEM) {
            env.createTemporarySystemFunction(name, function);
        } else {
            env.createTemporaryFunction(name, function);
        }
    }
}
