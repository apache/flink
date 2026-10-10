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

package org.apache.flink.table.expressions.resolver;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.OverWindow;
import org.apache.flink.table.api.OverWindowRange;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.legacy.api.TableSchema;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.SourceQueryOperation;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;
import org.apache.flink.table.utils.FunctionLookupMock;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedCall;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OverWindowExpressionResolverTest {

    @Test
    void resolvesFieldsInExpandedOverExpression() {
        Expression expandedOver =
                unresolvedCall(
                        BuiltInFunctionDefinitions.OVER,
                        Arrays.asList(
                                unresolvedCall(
                                        BuiltInFunctionDefinitions.SUM, unresolvedRef("amount")),
                                unresolvedRef("rowtime"),
                                valueLiteral(2L),
                                valueLiteral(OverWindowRange.CURRENT_ROW),
                                unresolvedRef("tenant")));

        CallExpression resolved =
                (CallExpression)
                        resolver(Collections.emptyList()).resolve(List.of(expandedOver)).get(0);
        List<Expression> children = resolved.getChildren();

        assertThat(children).hasSize(5);
        assertThat(((CallExpression) children.get(0)).getChildren().get(0))
                .isInstanceOf(FieldReferenceExpression.class);
        assertField(children.get(1), "rowtime");
        assertThat(children.get(2).asSummaryString()).isEqualTo("2");
        assertThat(children.get(3).asSummaryString()).contains("CURRENT_ROW");
        assertField(children.get(4), "tenant");
    }

    @Test
    void expandsLegacyNamedOverWindowAlias() {
        OverWindow window =
                Over.partitionBy($("tenant")).orderBy($("rowtime")).preceding(lit(2L)).as("w");
        Expression overAlias =
                unresolvedCall(
                        BuiltInFunctionDefinitions.OVER,
                        unresolvedCall(BuiltInFunctionDefinitions.SUM, unresolvedRef("amount")),
                        unresolvedRef("w"));

        CallExpression resolved =
                (CallExpression) resolver(List.of(window)).resolve(List.of(overAlias)).get(0);
        List<Expression> children = resolved.getChildren();

        assertThat(children).hasSize(5);
        assertThat(((CallExpression) children.get(0)).getChildren().get(0))
                .isInstanceOf(FieldReferenceExpression.class);
        assertField(children.get(1), "rowtime");
        assertThat(children.get(2).asSummaryString()).isEqualTo("2");
        assertThat(children.get(3).asSummaryString()).contains("CURRENT_ROW");
        assertField(children.get(4), "tenant");
    }

    @Test
    void rejectsMalformedOverExpression() {
        Expression malformedOver =
                unresolvedCall(
                        BuiltInFunctionDefinitions.OVER,
                        Arrays.asList(
                                unresolvedCall(
                                        BuiltInFunctionDefinitions.SUM, unresolvedRef("amount")),
                                unresolvedRef("rowtime"),
                                valueLiteral(2L)));

        assertThatThrownBy(() -> resolver(Collections.emptyList()).resolve(List.of(malformedOver)))
                .isInstanceOf(ValidationException.class);
    }

    private static void assertField(Expression expression, String name) {
        assertThat(expression).isInstanceOf(FieldReferenceExpression.class);
        assertThat(((FieldReferenceExpression) expression).getName()).isEqualTo(name);
    }

    private static ExpressionResolver resolver(List<OverWindow> windows) {
        DataType rowtime =
                new AtomicDataType(new LocalZonedTimestampType(false, TimestampKind.ROWTIME, 3));
        TableSchema schema =
                TableSchema.builder()
                        .field("amount", DataTypes.BIGINT())
                        .field("rowtime", rowtime)
                        .field("tenant", DataTypes.STRING())
                        .build();
        QueryOperation input =
                new SourceQueryOperation(
                        ContextResolvedTable.anonymous(
                                new ResolvedCatalogTable(
                                        CatalogTable.newBuilder().schema(schema.toSchema()).build(),
                                        ResolvedSchema.physical(
                                                schema.getFieldNames(),
                                                schema.getFieldDataTypes()))));

        return ExpressionResolver.resolverFor(
                        TableConfig.getDefault(),
                        Thread.currentThread().getContextClassLoader(),
                        name -> Optional.empty(),
                        new FunctionLookupMock(Collections.emptyMap()),
                        new DataTypeFactoryMock(),
                        (sqlExpression, inputRowType, outputType) -> {
                            throw new UnsupportedOperationException();
                        },
                        input)
                .withOverWindows(windows)
                .build();
    }
}
