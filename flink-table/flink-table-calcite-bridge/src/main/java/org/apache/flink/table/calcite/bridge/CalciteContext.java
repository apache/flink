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

package org.apache.flink.table.calcite.bridge;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.delegation.ParserFactory;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;

/**
 * To provide calcite context which is mainly used to create Calcite's RelNode. The context is
 * provided by Flink's table planner.
 */
@Internal
public interface CalciteContext extends ParserFactory.Context {

    /** Create an instance of {@link CalciteCatalogReader} provided by Flink's table planner. */
    CalciteCatalogReader createCatalogReader(boolean lenientCaseSensitivity);

    /** Return the {@link RelOptCluster} provided by Flink's table planner. */
    RelOptCluster getCluster();

    /** Create an instance of {@link FrameworkConfig} provided by Flink's table planner. */
    FrameworkConfig createFrameworkConfig();

    /** Return the {@link RelDataTypeFactory} provided by Flink's table planner. */
    RelDataTypeFactory getTypeFactory();

    /** Create a builder for relational expressions provided by Flink's table planner. */
    RelBuilder createRelBuilder();

    /**
     * Return the {@link TableConfig} defined in {@link
     * org.apache.flink.table.api.TableEnvironment}.
     */
    TableConfig getTableConfig();

    /**
     * Return the {@link ClassLoader} defined in {@link
     * org.apache.flink.table.api.TableEnvironment}.
     */
    ClassLoader getClassLoader();

    /**
     * Return the {@link FunctionCatalog} defined in {@link
     * org.apache.flink.table.api.TableEnvironment}.
     */
    FunctionCatalog getFunctionCatalog();

    /**
     * Create a new instance of {@link RelOptTable.ToRelContext} provided by Flink's table planner.
     * The {@link RelOptTable.ToRelContext} is used to convert a table into a relational expression.
     */
    RelOptTable.ToRelContext createToRelContext();
}
