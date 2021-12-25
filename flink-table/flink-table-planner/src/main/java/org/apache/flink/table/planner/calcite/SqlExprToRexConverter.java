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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.annotation.Internal;

import org.apache.calcite.rex.RexNode;

/** Converts SQL expressions to {@link RexNode}. */
@Internal
public interface SqlExprToRexConverter {

    /**
     * Converts the given SQL expression string to an expanded string with fully qualified function
     * calls and escaped identifiers.
     *
     * <p>E.g. {@code my_udf(f0) + 1} to {@code `my_catalog`.`my_database`.`my_udf`(`f0`) + 1}
     */
    String expand(String expr);

    /**
     * Converts a SQL expression to a {@link RexNode} expression.
     *
     * @param expr a SQL expression e.g. {@code `my_catalog`.`my_database`.`my_udf`(`f0`) + 1}
     */
    RexNode convertToRexNode(String expr);

    /**
     * Converts an array of SQL expressions to an array of {@link RexNode} expressions.
     *
     * @param exprs a SQL expression e.g. {@code `my_catalog`.`my_database`.`my_udf`(`f0`) + 1}
     */
    RexNode[] convertToRexNodes(String[] exprs);
}
