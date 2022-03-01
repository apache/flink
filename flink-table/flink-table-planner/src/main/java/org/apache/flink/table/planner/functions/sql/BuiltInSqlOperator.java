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

package org.apache.flink.table.planner.functions.sql;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;

import org.apache.calcite.sql.SqlOperator;

import java.util.Optional;

import static org.apache.flink.table.functions.BuiltInFunctionDefinition.DEFAULT_VERSION;
import static org.apache.flink.table.functions.BuiltInFunctionDefinition.qualifyFunctionName;

/**
 * SQL version of {@link BuiltInFunctionDefinition} in cases where {@link BridgingSqlFunction} does
 * not apply. This is the case when the operator has a special parsing syntax or uses other
 * Calcite-specific features that are not exposed via {@link BuiltInFunctionDefinition} yet.
 *
 * <p>If {@link SqlOperator} were an interface, this interface would extend from it.
 */
@Internal
public interface BuiltInSqlOperator {

    /** @see BuiltInFunctionDefinition#getVersion() */
    Optional<Integer> getVersion();

    /** @see BuiltInFunctionDefinition#isInternal() */
    boolean isInternal();

    /** @see BuiltInFunctionDefinition#getQualifiedName() */
    String getQualifiedName();

    // --------------------------------------------------------------------------------------------

    static Optional<Integer> unwrapVersion(SqlOperator operator) {
        if (operator instanceof BuiltInSqlOperator) {
            final BuiltInSqlOperator builtInSqlOperator = (BuiltInSqlOperator) operator;
            return builtInSqlOperator.isInternal()
                    ? Optional.empty()
                    : builtInSqlOperator.getVersion();
        }
        return Optional.of(DEFAULT_VERSION);
    }

    static boolean unwrapIsInternal(SqlOperator operator) {
        if (operator instanceof BuiltInSqlOperator) {
            return ((BuiltInSqlOperator) operator).isInternal();
        }
        return false;
    }

    static String toQualifiedName(SqlOperator operator) {
        if (operator instanceof BuiltInSqlOperator) {
            final BuiltInSqlOperator builtInSqlOperator = (BuiltInSqlOperator) operator;
            return builtInSqlOperator.getQualifiedName();
        }
        return qualifyFunctionName(operator.getName(), DEFAULT_VERSION);
    }

    static String extractNameFromQualifiedName(String qualifiedName) {
        // supports all various kinds of qualified names
        // $FUNC$1 => FUNC
        // $IS NULL$1 => IS NULL
        // $$CALCITE_INTERNAL$1 => $CALCITE_INTERNAL
        int versionPos = qualifiedName.length() - 1;
        while (Character.isDigit(qualifiedName.charAt(versionPos))) {
            versionPos--;
        }
        return qualifiedName.substring(1, versionPos);
    }
}
