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

package org.apache.flink.table.api.constraints;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.ResolvedSchema;

/**
 * Integrity constraints, generally referred to simply as constraints, define the valid states of
 * SQL-data by constraining the values in the base tables.
 *
 * @deprecated See {@link ResolvedSchema} and {@link org.apache.flink.table.catalog.Constraint}.
 */
@Deprecated
@PublicEvolving
public interface Constraint {
    String getName();

    /**
     * Constraints can either be enforced or non-enforced. If a constraint is enforced it will be
     * checked whenever any SQL statement is executed that results in data or schema changes. If the
     * constraint is not enforced the owner of the data is responsible for ensuring data integrity.
     * Flink will rely the information is valid and might use it for query optimisations.
     */
    boolean isEnforced();

    /** Tells what kind of constraint it is, e.g. PRIMARY KEY, UNIQUE, ... */
    ConstraintType getType();

    /** Prints the constraint in a readable way. */
    String asSummaryString();

    /**
     * Type of the constraint.
     *
     * <p>Unique constraints:
     *
     * <ul>
     *   <li>UNIQUE - is satisfied if and only if there do not exist two rows that have same
     *       non-null values in the unique columns
     *   <li>PRIMARY KEY - additionally to UNIQUE constraint, it requires none of the values in
     *       specified columns be a null value. Moreover there can be only a single PRIMARY KEY
     *       defined for a Table.
     * </ul>
     */
    enum ConstraintType {
        PRIMARY_KEY,
        UNIQUE_KEY
    }
}
