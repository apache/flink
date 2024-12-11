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

import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.planner.plan.schema.ExpandingPreparingTable;
import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;

import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelRecordType;

import javax.annotation.Nullable;

import java.util.AbstractList;
import java.util.List;

/**
 * A bridge between a Flink's specific {@link CatalogView} and a Calcite's {@link
 * org.apache.calcite.plan.RelOptTable}. It implements parsing and conversion from sql string to
 * {@link org.apache.calcite.rel.RelNode}.
 */
public class SqlCatalogViewTable extends ExpandingPreparingTable {
    private final CatalogView view;
    private final List<String> viewPath;

    public SqlCatalogViewTable(
            @Nullable RelOptSchema relOptSchema,
            RelDataType rowType,
            Iterable<String> names,
            FlinkStatistic statistic,
            CatalogView view,
            List<String> viewPath) {
        super(relOptSchema, rowType, names, statistic);
        this.view = view;
        this.viewPath = viewPath;
    }

    @Override
    public RelNode convertToRel(ToRelContext context) {
        RelNode original =
                context.expandView(rowType, view.getExpandedQuery(), viewPath, names).project();
        RelDataType castTargetType =
                adaptTimeAttributes(
                        original.getRowType(), rowType, context.getCluster().getTypeFactory());
        return RelOptUtil.createCastRel(original, castTargetType, true);
    }

    private static RelDataType adaptTimeAttributes(
            RelDataType queryType, RelDataType targetType, RelDataTypeFactory typeFactory) {
        if (queryType instanceof RelRecordType) {
            if (RelOptUtil.areRowTypesEqual(queryType, targetType, true)) {
                return targetType;
            } else if (targetType.getFieldCount() != queryType.getFieldCount()) {
                throw new IllegalArgumentException(
                        "Field counts are not equal: queryType ["
                                + queryType
                                + "]"
                                + " castRowType ["
                                + targetType
                                + "]");
            } else {
                return adaptTimeAttributeInRecord(
                        (RelRecordType) queryType, (RelRecordType) targetType, typeFactory);
            }
        } else {
            return adaptTimeAttributeInSimpleType(queryType, targetType, typeFactory);
        }
    }

    private static RelDataType adaptTimeAttributeInRecord(
            RelRecordType queryType, RelRecordType targetType, RelDataTypeFactory typeFactory) {
        RelDataType structType =
                typeFactory.createStructType(
                        targetType.getStructKind(),
                        new AbstractList<>() {
                            public RelDataType get(int index) {
                                RelDataType targetFieldType =
                                        (targetType.getFieldList().get(index)).getType();
                                RelDataType queryFieldType =
                                        (queryType.getFieldList().get(index)).getType();
                                return adaptTimeAttributes(
                                        queryFieldType, targetFieldType, typeFactory);
                            }

                            public int size() {
                                return targetType.getFieldCount();
                            }
                        },
                        targetType.getFieldNames());
        return typeFactory.createTypeWithNullability(structType, targetType.isNullable());
    }

    private static RelDataType adaptTimeAttributeInSimpleType(
            RelDataType queryType, RelDataType targetType, RelDataTypeFactory typeFactory) {
        if (queryType instanceof TimeIndicatorRelDataType) {
            return typeFactory.createTypeWithNullability(queryType, targetType.isNullable());
        }
        return targetType;
    }
}
