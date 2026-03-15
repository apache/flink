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

package org.apache.flink.table.operations.ddl;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.WatermarkSpec;
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogView;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.Expression;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Operation to describe an ALTER VIEW ... SET WATERMARK ... statement. */
@Internal
public class AlterViewSetWatermarkOperation extends AlterViewOperation {

    private final String rowtimeColumn;
    private final String watermarkExpression;

    public AlterViewSetWatermarkOperation(
            ObjectIdentifier viewIdentifier,
            String rowtimeColumn,
            String watermarkExpression) {
        super(viewIdentifier);
        this.rowtimeColumn = rowtimeColumn;
        this.watermarkExpression = watermarkExpression;
    }

    public String getRowtimeColumn() {
        return rowtimeColumn;
    }

    public String getWatermarkExpression() {
        return watermarkExpression;
    }

    @Override
    public String asSummaryString() {
        return String.format(
                "ALTER VIEW %s SET WATERMARK FOR %s AS %s",
                viewIdentifier.asSummaryString(), rowtimeColumn, watermarkExpression);
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        // Get the current view from catalog
        ResolvedCatalogView currentView =
                (ResolvedCatalogView)
                        ctx.getCatalogManager()
                                .getTable(getViewIdentifier())
                                .orElseThrow(
                                        () ->
                                                new ValidationException(
                                                        String.format(
                                                                "View %s does not exist",
                                                                getViewIdentifier()
                                                                        .asSummaryString())));

        // Get the current resolved schema
        ResolvedSchema currentSchema = currentView.getResolvedSchema();

        // Verify the rowtime column exists
        if (!currentSchema.getColumn(rowtimeColumn).isPresent()) {
            throw new ValidationException(
                    String.format(
                            "Column '%s' does not exist in view %s",
                            rowtimeColumn, getViewIdentifier().asSummaryString()));
        }

        // Create new schema with watermark
        Schema.Builder schemaBuilder = Schema.newBuilder();

        // Add all columns
        List<Column> columns = currentSchema.getColumns();
        for (Column column : columns) {
            if (column.isPhysical()) {
                schemaBuilder.column(column.getName(), column.getDataType());
            }
        }

        // Add watermark specification
        // Note: This is a simplified implementation. In production, you would need to:
        // 1. Parse and validate the watermark expression
        // 2. Convert it to a proper Expression object
        // 3. Handle existing watermarks properly
        schemaBuilder.watermark(rowtimeColumn, watermarkExpression);

        Schema newSchema = schemaBuilder.build();

        // Create updated view options (preserve existing options)
        Map<String, String> newOptions = new HashMap<>(currentView.getOptions());

        // Create new CatalogView with updated schema
        CatalogView newView =
                CatalogView.of(
                        newSchema,
                        currentView.getComment(),
                        currentView.getOriginalQuery(),
                        currentView.getExpandedQuery(),
                        newOptions);

        // Update the view in catalog
        ctx.getCatalogManager().alterTable(newView, getViewIdentifier(), false);

        return TableResultImpl.TABLE_RESULT_OK;
    }
}
