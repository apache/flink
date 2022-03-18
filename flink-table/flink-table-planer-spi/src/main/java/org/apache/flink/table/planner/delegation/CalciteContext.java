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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;

/** Context for construct Calcite RelNode. */
public interface CalciteContext {

    CalciteCatalogReader createCatalogReader(
            boolean lenientCaseSensitivity, String currentCatalog, String currentDatabase);

    RelOptCluster getCluster();

    FrameworkConfig createFrameworkConfig();

    RelDataTypeFactory getTypeFactory();

    RelBuilder createRelBuilder(String currentCatalog, String currentDatabase);

    // todo check it can be replaced or not
    RelOptTable.ToRelContext createRelContext(String currentCatalog, String currentDatabase);
}
