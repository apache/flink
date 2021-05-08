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

package org.apache.flink.table.planner.plan.abilities.source;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.SqlExprToRexConverterFactory;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.core.TableScan;

/**
 * A sub-class of {@link FlinkContext} which contains the information for {@link SourceAbilitySpec}
 * to apply the following abilities to {@link DynamicTableSource}.
 *
 * <ul>
 *   <li>filter push down (SupportsFilterPushDown)
 *   <li>project push down (SupportsProjectionPushDown)
 *   <li>partition push down (SupportsPartitionPushDown)
 *   <li>watermark push down (SupportsWatermarkPushDown)
 *   <li>reading metadata (SupportsReadingMetadata)
 * </ul>
 */
public class SourceAbilityContext implements FlinkContext {
    private final RowType sourceRowType;
    private final FlinkContext context;

    public SourceAbilityContext(FlinkContext context, RowType sourceRowType) {
        this.context = context;
        this.sourceRowType = sourceRowType;
    }

    @Override
    public TableConfig getTableConfig() {
        return context.getTableConfig();
    }

    @Override
    public FunctionCatalog getFunctionCatalog() {
        return context.getFunctionCatalog();
    }

    @Override
    public CatalogManager getCatalogManager() {
        return context.getCatalogManager();
    }

    @Override
    public SqlExprToRexConverterFactory getSqlExprToRexConverterFactory() {
        return context.getSqlExprToRexConverterFactory();
    }

    @Override
    public <C> C unwrap(Class<C> clazz) {
        if (clazz.isInstance(this)) {
            return clazz.cast(this);
        } else {
            return null;
        }
    }

    public RowType getSourceRowType() {
        return sourceRowType;
    }

    public static SourceAbilityContext from(TableScan scan) {
        return new SourceAbilityContext(
                ShortcutUtils.unwrapContext(scan),
                FlinkTypeFactory.toLogicalRowType(scan.getRowType()));
    }
}
