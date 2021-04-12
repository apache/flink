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

package org.apache.flink.table.planner.catalog;

import org.apache.flink.table.catalog.QueryOperationCatalogView;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.TableSourceQueryOperation;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.plan.schema.ExpandingPreparingTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;

import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/**
 * A bridge between a Flink's specific {@link QueryOperationCatalogView} and a Calcite's {@link
 * org.apache.calcite.plan.RelOptTable}. It implements the conversion from {@link
 * org.apache.flink.table.operations.QueryOperation} to {@link org.apache.calcite.rel.RelNode}.
 */
public class QueryOperationCatalogViewTable extends ExpandingPreparingTable {
    private final QueryOperationCatalogView catalogView;

    /** Creates a QueryOperationCatalogViewTable. */
    private QueryOperationCatalogViewTable(
            RelOptSchema relOptSchema,
            List<String> names,
            RelDataType rowType,
            QueryOperationCatalogView catalogView) {
        super(relOptSchema, rowType, names, FlinkStatistic.UNKNOWN());
        this.catalogView = catalogView;
    }

    public static QueryOperationCatalogViewTable create(
            RelOptSchema schema,
            List<String> names,
            RelDataType rowType,
            QueryOperationCatalogView view) {
        return new QueryOperationCatalogViewTable(schema, names, rowType, view);
    }

    @Override
    public List<String> getQualifiedName() {
        final QueryOperation queryOperation = catalogView.getQueryOperation();
        if (queryOperation instanceof TableSourceQueryOperation) {
            TableSourceQueryOperation tsqo = (TableSourceQueryOperation) queryOperation;
            return explainSourceAsString(tsqo.getTableSource());
        }
        return super.getQualifiedName();
    }

    @Override
    public RelNode convertToRel(RelOptTable.ToRelContext context) {
        FlinkRelBuilder relBuilder =
                FlinkRelBuilder.of(context, context.getCluster(), this.getRelOptSchema());

        return relBuilder.queryOperation(catalogView.getQueryOperation()).build();
    }
}
