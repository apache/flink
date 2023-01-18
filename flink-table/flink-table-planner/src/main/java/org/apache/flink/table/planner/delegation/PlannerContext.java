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

package org.apache.flink.table.planner.delegation;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexFactory;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.planner.plan.FlinkCalciteCatalogReader;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.tools.FrameworkConfig;

/** Utility Interface implemented by Flink Planner as well as external Planner. */
@PublicEvolving
public interface PlannerContext {

    public RexFactory getRexFactory();

    public FlinkCalciteCatalogReader createCatalogReader(boolean lenientCaseSensitivity);

    public FrameworkConfig createFrameworkConfig();

    public RelOptCluster getCluster();

    public FlinkContext getFlinkContext();

    public FlinkTypeFactory getTypeFactory();

    public FlinkPlannerImpl createFlinkPlanner();

    public FlinkRelBuilder createRelBuilder();

    public CalciteParser createCalciteParser();
}
