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

package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.sql.parser.ddl.view.SqlAlterViewSetWatermark;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.AlterViewSetWatermarkOperation;

import org.apache.calcite.sql.SqlNode;

import static org.apache.flink.table.planner.operations.converters.SqlNodeConvertUtils.validateAlterView;

/** A converter for {@link SqlAlterViewSetWatermark}. */
public class SqlAlterViewSetWatermarkConverter
        implements SqlNodeConverter<SqlAlterViewSetWatermark> {

    @Override
    public Operation convertSqlNode(SqlAlterViewSetWatermark alterView, ConvertContext context) {
        // Validate the view exists
        validateAlterView(alterView, context);

        // Get the qualified view identifier
        ObjectIdentifier viewIdentifier =
                context.getCatalogManager()
                        .qualifyIdentifier(UnresolvedIdentifier.of(alterView.getFullName()));

        // Get the rowtime column name
        String rowtimeColumn = alterView.getRowtimeColumn().getSimple();

        // Get the watermark expression as string
        // In production, you might want to validate and convert this expression properly
        SqlNode watermarkExprNode = alterView.getWatermarkExpression();
        String watermarkExpression = watermarkExprNode.toString();

        return new AlterViewSetWatermarkOperation(
                viewIdentifier, rowtimeColumn, watermarkExpression);
    }
}
