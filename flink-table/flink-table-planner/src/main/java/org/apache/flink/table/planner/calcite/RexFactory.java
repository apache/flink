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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;

import javax.annotation.Nullable;

import java.util.function.Supplier;

/** Planner internal factory for parsing/translating to {@link RexNode}. */
@Internal
public class RexFactory {

    private final FlinkTypeFactory typeFactory;

    private final Supplier<FlinkPlannerImpl> plannerSupplier;

    private final Supplier<SqlDialect> sqlDialectSupplier;

    public RexFactory(
            FlinkTypeFactory typeFactory,
            Supplier<FlinkPlannerImpl> plannerSupplier,
            Supplier<SqlDialect> sqlDialectSupplier) {
        this.typeFactory = typeFactory;
        this.plannerSupplier = plannerSupplier;
        this.sqlDialectSupplier = sqlDialectSupplier;
    }

    /**
     * Creates a new instance of {@link SqlToRexConverter} to convert SQL expression to {@link
     * RexNode}.
     */
    public SqlToRexConverter createSqlToRexConverter(
            RelDataType inputRowType, @Nullable RelDataType outputType) {
        return new SqlToRexConverter(
                plannerSupplier.get(), sqlDialectSupplier.get(), inputRowType, outputType);
    }

    /**
     * Creates a new instance of {@link SqlToRexConverter} to convert SQL expression to {@link
     * RexNode}.
     */
    public SqlToRexConverter createSqlToRexConverter(
            RowType inputRowType, @Nullable LogicalType outputType) {
        final RelDataType convertedInputRowType = typeFactory.buildRelNodeRowType(inputRowType);

        final RelDataType convertedOutputType;
        if (outputType != null) {
            convertedOutputType = typeFactory.createFieldTypeFromLogicalType(outputType);
        } else {
            convertedOutputType = null;
        }

        return createSqlToRexConverter(convertedInputRowType, convertedOutputType);
    }
}
