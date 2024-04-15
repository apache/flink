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

import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.List;

/** Test step for creating a statement set. */
public final class StatementSetTestStep implements TestStep {

    private StatementSet statementSet = null;

    public final List<String> statements;

    StatementSetTestStep(List<String> statements) {
        this.statements = statements;
    }

    @Override
    public TestKind getKind() {
        return TestKind.STATEMENT_SET;
    }

    /** Returns a CompiledPlan for a statement set. */
    public CompiledPlan compiledPlan(TableEnvironment env) {
        statementSet = env.createStatementSet();
        statements.forEach(statementSet::addInsertSql);
        return statementSet.compilePlan();
    }

    public TableResult apply(TableEnvironment env) {
        if (statementSet == null) {
            statementSet = env.createStatementSet();
            statements.forEach(statementSet::addInsertSql);
        }
        return statementSet.execute();
    }
}
