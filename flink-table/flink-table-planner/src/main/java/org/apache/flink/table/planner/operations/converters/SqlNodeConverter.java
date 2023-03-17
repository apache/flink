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

package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.utils.Expander;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;

/** A converter to convert {@link SqlNode} instance into {@link Operation}. */
public interface SqlNodeConverter<S extends SqlNode> {

    /**
     * Convert the given validated {@link SqlNode} into an {@link Operation}.
     *
     * @param node a validated {@link SqlNode}.
     * @param context the utilities and context information to convert
     */
    Operation convertSqlNode(S node, ConvertContext context);

    /** Context of {@link SqlNodeConverter}. */
    interface ConvertContext {

        /** Returns the {@link SqlValidator} in the convert context. */
        SqlValidator getSqlValidator();

        /** Returns the {@link CatalogManager} in the convert context. */
        CatalogManager getCatalogManager();

        /** Converts the given validated {@link SqlNode} into a {@link RelRoot}. */
        RelRoot toRelRoot(SqlNode sqlNode);

        /** Convert the given {@param sqlNode} into a quoted SQL string. */
        String toQuotedSqlString(SqlNode sqlNode);

        /**
         * Expands identifiers in a given SQL string.
         *
         * @see Expander
         */
        String expandSqlIdentifiers(String sql);
    }
}
