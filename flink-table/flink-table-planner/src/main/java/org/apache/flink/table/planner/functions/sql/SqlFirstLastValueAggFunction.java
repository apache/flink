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

import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Optionality;

import java.util.List;

/**
 * <code>FIRST_VALUE</code> and <code>LAST_VALUE</code> aggregate functions return the first or the
 * last value in a list of values that are input to the function.
 *
 * <p>NOTE: The difference between this and {@link
 * org.apache.calcite.sql.fun.SqlFirstLastValueAggFunction} is that this can be used without over
 * clause.
 */
public class SqlFirstLastValueAggFunction extends SqlAggFunction {

    public SqlFirstLastValueAggFunction(SqlKind kind) {
        super(
                kind.name(),
                null,
                kind,
                ReturnTypes.ARG0_NULLABLE_IF_EMPTY,
                null,
                OperandTypes.ANY,
                SqlFunctionCategory.NUMERIC,
                false,
                false,
                Optionality.FORBIDDEN);
        Preconditions.checkArgument(kind == SqlKind.FIRST_VALUE || kind == SqlKind.LAST_VALUE);
    }

    // ~ Methods ----------------------------------------------------------------

    @SuppressWarnings("deprecation")
    public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
        return ImmutableList.of(
                typeFactory.createTypeWithNullability(
                        typeFactory.createSqlType(SqlTypeName.ANY), true));
    }

    @SuppressWarnings("deprecation")
    public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
        return typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.ANY), true);
    }
}
