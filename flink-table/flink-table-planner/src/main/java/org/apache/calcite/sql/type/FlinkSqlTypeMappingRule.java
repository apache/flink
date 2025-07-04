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

package org.apache.calcite.sql.type;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Rules that determine whether a type is castable from another type. */
public class FlinkSqlTypeMappingRule implements SqlTypeMappingRule {
    private static final FlinkSqlTypeMappingRule INSTANCE;

    private final Map<SqlTypeName, ImmutableSet<SqlTypeName>> map;

    private FlinkSqlTypeMappingRule(Map<SqlTypeName, ImmutableSet<SqlTypeName>> map) {
        this.map = ImmutableMap.copyOf(map);
    }

    public static FlinkSqlTypeMappingRule instance() {
        return Objects.requireNonNull(FLINK_THREAD_PROVIDERS.get(), "flinkThreadProviders");
    }

    public static FlinkSqlTypeMappingRule instance(
            Map<SqlTypeName, ImmutableSet<SqlTypeName>> map) {
        return new FlinkSqlTypeMappingRule(map);
    }

    public Map<SqlTypeName, ImmutableSet<SqlTypeName>> getTypeMapping() {
        return this.map;
    }

    static {
        SqlTypeMappingRules.Builder coerceRules = SqlTypeMappingRules.builder();
        coerceRules.addAll(SqlTypeCoercionRule.lenientInstance().getTypeMapping());
        Map<SqlTypeName, ImmutableSet<SqlTypeName>> map =
                SqlTypeCoercionRule.lenientInstance().getTypeMapping();
        Set<SqlTypeName> rule = new HashSet<>();
        rule.add(SqlTypeName.TINYINT);
        rule.add(SqlTypeName.SMALLINT);
        rule.add(SqlTypeName.INTEGER);
        rule.add(SqlTypeName.BIGINT);
        rule.add(SqlTypeName.DECIMAL);
        rule.add(SqlTypeName.FLOAT);
        rule.add(SqlTypeName.REAL);
        rule.add(SqlTypeName.DOUBLE);
        rule.add(SqlTypeName.CHAR);
        rule.add(SqlTypeName.VARCHAR);
        rule.add(SqlTypeName.BOOLEAN);
        rule.add(SqlTypeName.TIMESTAMP);
        rule.add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        coerceRules.add(SqlTypeName.FLOAT, rule);
        coerceRules.add(SqlTypeName.DOUBLE, rule);
        coerceRules.add(SqlTypeName.DECIMAL, rule);
        INSTANCE = new FlinkSqlTypeMappingRule(coerceRules.map);
    }

    public static final ThreadLocal<@Nullable FlinkSqlTypeMappingRule> FLINK_THREAD_PROVIDERS =
            ThreadLocal.withInitial(() -> INSTANCE);
}
