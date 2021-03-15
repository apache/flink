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

package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.types.DataType;

import java.util.List;

/**
 * Expression that has been fully resolved and validated.
 *
 * <p>Compared to {@link Expression}, resolved expressions do not contain unresolved subexpressions
 * anymore and provide an output data type for the computation result.
 *
 * <p>Instances of this class describe a fully parameterized, immutable expression that can be
 * serialized and persisted.
 *
 * <p>Resolved expression are the output of the API to the planner and are pushed from the planner
 * into interfaces, for example, for predicate push-down.
 */
@PublicEvolving
public interface ResolvedExpression extends Expression {

    /**
     * Returns a string that fully serializes this instance. The serialized string can be used for
     * storing the query in, for example, a {@link org.apache.flink.table.catalog.Catalog} as a
     * view.
     *
     * @return detailed string for persisting in a catalog
     */
    default String asSerializableString() {
        throw new TableException(
                String.format(
                        "Expression '%s' is not string serializable. Currently, only expressions that "
                                + "originated from a SQL expression have a well-defined string representation.",
                        asSummaryString()));
    }

    /** Returns the data type of the computation result. */
    DataType getOutputDataType();

    List<ResolvedExpression> getResolvedChildren();
}
