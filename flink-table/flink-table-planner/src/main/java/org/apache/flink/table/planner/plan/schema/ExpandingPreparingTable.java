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

package org.apache.flink.table.planner.plan.schema;

import org.apache.flink.table.planner.plan.stats.FlinkStatistic;

import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;

import javax.annotation.Nullable;

/**
 * A common subclass for all tables that are expanded to sub-query (views). It handles recursive
 * expanding of sub-views automatically.
 */
public abstract class ExpandingPreparingTable extends FlinkPreparingTableBase {
    protected ExpandingPreparingTable(
            @Nullable RelOptSchema relOptSchema,
            RelDataType rowType,
            Iterable<String> names,
            FlinkStatistic statistic) {
        super(relOptSchema, rowType, names, statistic);
    }

    /**
     * Converts the table to a {@link RelNode}. Does not need to expand any nested scans of an
     * {@link ExpandingPreparingTable}. Those will be expanded recursively.
     *
     * @return a relational tree
     */
    protected abstract RelNode convertToRel(ToRelContext context);

    @Override
    public final RelNode toRel(RelOptTable.ToRelContext context) {
        return expand(context);
    }

    private RelNode expand(RelOptTable.ToRelContext context) {
        final RelNode rel = convertToRel(context);
        // Expand any views
        return rel.accept(
                new RelShuttleImpl() {
                    @Override
                    public RelNode visit(TableScan scan) {
                        final RelOptTable table = scan.getTable();
                        if (table instanceof ExpandingPreparingTable) {
                            return ((ExpandingPreparingTable) table).expand(context);
                        }
                        return super.visit(scan);
                    }
                });
    }
}
