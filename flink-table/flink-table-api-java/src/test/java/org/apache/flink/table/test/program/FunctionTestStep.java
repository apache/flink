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
import org.apache.flink.table.functions.UserDefinedFunction;

/** Test step for registering a (temporary) (system or catalog) function. */
public final class FunctionTestStep implements TestStep {

    /** Whether function should be temporary or not. */
    enum FunctionPersistence {
        TEMPORARY,
        PERSISTENT
    }

    /** Whether function should be persisted in a catalog or not. */
    enum FunctionBehavior {
        SYSTEM,
        CATALOG
    }

    public final FunctionPersistence persistence;
    public final FunctionBehavior behavior;
    public final String name;
    public final Class<? extends UserDefinedFunction> function;

    FunctionTestStep(
            FunctionPersistence persistence,
            FunctionBehavior behavior,
            String name,
            Class<? extends UserDefinedFunction> function) {
        this.persistence = persistence;
        this.behavior = behavior;
        this.name = name;
        this.function = function;
    }

    @Override
    public TestKind getKind() {
        return TestKind.FUNCTION;
    }

    public void apply(TableEnvironment env) {
        if (behavior == FunctionBehavior.SYSTEM) {
            if (persistence == FunctionPersistence.TEMPORARY) {
                env.createTemporarySystemFunction(name, function);
            } else {
                throw new UnsupportedOperationException("System functions must be temporary.");
            }
        } else {
            if (persistence == FunctionPersistence.TEMPORARY) {
                env.createTemporaryFunction(name, function);
            } else {
                env.createFunction(name, function);
            }
        }
    }
}
