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

import org.apache.flink.table.planner.plan.FlinkCalciteCatalogReader;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.FrameworkConfig;

import java.util.Collections;
import java.util.Properties;
import java.util.stream.Stream;

/** Standard implementation of {@link SqlExprToRexConverter}. */
public class SqlExprToRexConverterImpl implements SqlExprToRexConverter {

    private final FlinkPlannerImpl planner;

    private final RelDataType tableRowType;

    public SqlExprToRexConverterImpl(
            FrameworkConfig config,
            FlinkTypeFactory typeFactory,
            RelOptCluster cluster,
            RelDataType tableRowType) {
        this.planner =
                new FlinkPlannerImpl(
                        config,
                        (isLenient) -> createEmptyCatalogReader(typeFactory),
                        typeFactory,
                        cluster);
        this.tableRowType = tableRowType;
    }

    @Override
    public RexNode convertToRexNode(String expr) {
        return convertToRexNodes(new String[] {expr})[0];
    }

    @Override
    public RexNode[] convertToRexNodes(String[] exprs) {
        final CalciteParser parser = planner.parser();
        return Stream.of(exprs)
                .map(parser::parseExpression)
                .map(node -> planner.rex(node, tableRowType))
                .toArray(RexNode[]::new);
    }

    // ------------------------------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------------------------------

    private static CalciteCatalogReader createEmptyCatalogReader(FlinkTypeFactory typeFactory) {
        return new FlinkCalciteCatalogReader(
                CalciteSchema.createRootSchema(false),
                Collections.emptyList(),
                typeFactory,
                new CalciteConnectionConfigImpl(new Properties()));
    }
}
