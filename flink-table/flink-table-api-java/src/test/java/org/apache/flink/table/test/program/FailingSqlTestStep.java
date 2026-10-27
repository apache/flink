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
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test step for executing SQL that will fail eventually with either {@link ValidationException}
 * (during planning time) or {@link TableRuntimeException} (during execution time).
 *
 * <p>Note: Not every runner supports generic SQL statements. Sometimes the runner would like to
 * enrich properties e.g. of a CREATE TABLE. Use this step with caution.
 */
public final class FailingSqlTestStep implements TestStep {

    public final String sql;
    public final Class<? extends Exception> expectedException;
    public final String expectedErrorMessage;

    FailingSqlTestStep(
            String sql, Class<? extends Exception> expectedException, String expectedErrorMessage) {
        Preconditions.checkArgument(
                expectedException == ValidationException.class
                        || expectedException == TableRuntimeException.class,
                "Usually a SQL query should fail with either validation or runtime exception. "
                        + "Otherwise this might require an update to the exception design.");
        this.sql = sql;
        this.expectedException = expectedException;
        this.expectedErrorMessage = expectedErrorMessage;
    }

    @Override
    public TestKind getKind() {
        return TestKind.FAILING_SQL;
    }

    public void apply(TableEnvironment env) {
        assertThatThrownBy(() -> env.executeSql(sql).await())
                .satisfies(anyCauseMatches(expectedException, expectedErrorMessage));
    }
}
