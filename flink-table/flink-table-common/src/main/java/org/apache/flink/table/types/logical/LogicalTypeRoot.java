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

package org.apache.flink.table.types.logical;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * An enumeration of logical type roots containing static information about logical data types.
 *
 * <p>A root is an essential description of a {@link LogicalType} without additional parameters. For
 * example, a parameterized logical type {@code DECIMAL(12,3)} possesses all characteristics of its
 * root {@code DECIMAL}. Additionally, a logical type root enables efficient comparision during the
 * evaluation of types.
 *
 * <p>The enumeration is very close to the SQL standard in terms of naming and completeness.
 * However, it reflects just a subset of the evolving standard and contains some extensions (such as
 * {@code NULL}, {@code SYMBOL}, or {@code RAW}).
 *
 * <p>See the type-implementing classes for a more detailed description of each type.
 *
 * <p>Note to implementers: Whenever we perform a match against a type root (e.g. using a
 * switch/case statement), it is recommended to:
 *
 * <ul>
 *   <li>Order the items by the type root definition in this class for easy readability.
 *   <li>Think about the behavior of all type roots for the implementation. A default fallback is
 *       dangerous when introducing a new type root in the future.
 *   <li>In many <b>runtime</b> cases, resolve the indirection of {@link #DISTINCT_TYPE}: {@code
 *       return myMethod(((DistinctType) type).getSourceType)}
 * </ul>
 */
@PublicEvolving
public enum LogicalTypeRoot {
    CHAR(LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.CHARACTER_STRING),

    VARCHAR(LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.CHARACTER_STRING),

    BOOLEAN(LogicalTypeFamily.PREDEFINED),

    BINARY(LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.BINARY_STRING),

    VARBINARY(LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.BINARY_STRING),

    DECIMAL(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.NUMERIC,
            LogicalTypeFamily.EXACT_NUMERIC),

    TINYINT(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.NUMERIC,
            LogicalTypeFamily.INTEGER_NUMERIC,
            LogicalTypeFamily.EXACT_NUMERIC),

    SMALLINT(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.NUMERIC,
            LogicalTypeFamily.INTEGER_NUMERIC,
            LogicalTypeFamily.EXACT_NUMERIC),

    INTEGER(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.NUMERIC,
            LogicalTypeFamily.INTEGER_NUMERIC,
            LogicalTypeFamily.EXACT_NUMERIC),

    BIGINT(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.NUMERIC,
            LogicalTypeFamily.INTEGER_NUMERIC,
            LogicalTypeFamily.EXACT_NUMERIC),

    FLOAT(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.NUMERIC,
            LogicalTypeFamily.APPROXIMATE_NUMERIC),

    DOUBLE(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.NUMERIC,
            LogicalTypeFamily.APPROXIMATE_NUMERIC),

    DATE(LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.DATETIME),

    TIME_WITHOUT_TIME_ZONE(
            LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.DATETIME, LogicalTypeFamily.TIME),

    TIMESTAMP_WITHOUT_TIME_ZONE(
            LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.DATETIME, LogicalTypeFamily.TIMESTAMP),

    TIMESTAMP_WITH_TIME_ZONE(
            LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.DATETIME, LogicalTypeFamily.TIMESTAMP),

    TIMESTAMP_WITH_LOCAL_TIME_ZONE(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.DATETIME,
            LogicalTypeFamily.TIMESTAMP,
            LogicalTypeFamily.EXTENSION),

    INTERVAL_YEAR_MONTH(LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.INTERVAL),

    INTERVAL_DAY_TIME(LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.INTERVAL),

    ARRAY(LogicalTypeFamily.CONSTRUCTED, LogicalTypeFamily.COLLECTION),

    MULTISET(LogicalTypeFamily.CONSTRUCTED, LogicalTypeFamily.COLLECTION),

    MAP(LogicalTypeFamily.CONSTRUCTED, LogicalTypeFamily.EXTENSION),

    ROW(LogicalTypeFamily.CONSTRUCTED),

    DISTINCT_TYPE(LogicalTypeFamily.USER_DEFINED),

    STRUCTURED_TYPE(LogicalTypeFamily.USER_DEFINED),

    NULL(LogicalTypeFamily.EXTENSION),

    RAW(LogicalTypeFamily.EXTENSION),

    SYMBOL(LogicalTypeFamily.EXTENSION),

    UNRESOLVED(LogicalTypeFamily.EXTENSION);

    private final Set<LogicalTypeFamily> families;

    LogicalTypeRoot(LogicalTypeFamily firstFamily, LogicalTypeFamily... otherFamilies) {
        this.families = Collections.unmodifiableSet(EnumSet.of(firstFamily, otherFamilies));
    }

    public Set<LogicalTypeFamily> getFamilies() {
        return families;
    }
}
