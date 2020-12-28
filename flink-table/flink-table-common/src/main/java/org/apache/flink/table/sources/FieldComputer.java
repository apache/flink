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

package org.apache.flink.table.sources;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedFieldReference;

/**
 * The {@link FieldComputer} interface returns an expression to compute the field of the table
 * schema of a {@link TableSource} from one or more fields of the {@link TableSource}'s return type.
 *
 * @param <T> The result type of the provided expression.
 * @deprecated This interface will not be supported in the new source design around {@link
 *     DynamicTableSource} which only works with the Blink planner. Use the concept of computed
 *     columns instead. See FLIP-95 for more information.
 */
@Deprecated
@PublicEvolving
public interface FieldComputer<T> {

    /**
     * Returns the names of all fields that the expression of the field computer accesses.
     *
     * @return An array with the names of all accessed fields.
     */
    String[] getArgumentFields();

    /**
     * Returns the result type of the expression.
     *
     * @return The result type of the expression.
     */
    TypeInformation<T> getReturnType();

    /**
     * Validates that the fields that the expression references have the correct types.
     *
     * @param argumentFieldTypes The types of the physical input fields.
     */
    void validateArgumentFields(TypeInformation<?>[] argumentFieldTypes);

    /**
     * Returns the {@link Expression} that computes the value of the field.
     *
     * @param fieldAccesses Field access expressions for the argument fields.
     * @return The expression to extract the timestamp from the {@link TableSource} return type.
     */
    Expression getExpression(ResolvedFieldReference[] fieldAccesses);
}
