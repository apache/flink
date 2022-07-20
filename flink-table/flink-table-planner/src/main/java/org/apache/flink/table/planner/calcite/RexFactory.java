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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.LocalReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.resolver.ExpressionResolver;
import org.apache.flink.table.expressions.resolver.ExpressionResolver.ExpressionResolverBuilder;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.expressions.converter.ExpressionConverter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.flink.table.expressions.ApiExpressionUtils.localRef;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapContext;

/** Planner internal factory for parsing/translating to {@link RexNode}. */
@Internal
public class RexFactory {

    private final FlinkTypeFactory typeFactory;
    // we use suppliers to read from TableConfig lazily
    private final Supplier<FlinkPlannerImpl> plannerSupplier;
    private final Supplier<SqlDialect> sqlDialectSupplier;
    private final Function<FlinkPlannerImpl, FlinkRelBuilder> relBuilderSupplier;

    public RexFactory(
            FlinkTypeFactory typeFactory,
            Supplier<FlinkPlannerImpl> plannerSupplier,
            Supplier<SqlDialect> sqlDialectSupplier,
            Function<FlinkPlannerImpl, FlinkRelBuilder> relBuilderSupplier) {
        this.typeFactory = typeFactory;
        this.plannerSupplier = plannerSupplier;
        this.sqlDialectSupplier = sqlDialectSupplier;
        this.relBuilderSupplier = relBuilderSupplier;
    }

    /**
     * Creates a new instance of {@link SqlToRexConverter} to convert SQL expression to {@link
     * RexNode}.
     */
    public SqlToRexConverter createSqlToRexConverter(
            RelDataType inputRowType, @Nullable RelDataType outputType) {
        return new SqlToRexConverter(
                plannerSupplier.get(), sqlDialectSupplier.get(), inputRowType, outputType);
    }

    /**
     * Creates a new instance of {@link SqlToRexConverter} to convert SQL expression to {@link
     * RexNode}.
     */
    public SqlToRexConverter createSqlToRexConverter(
            RowType inputRowType, @Nullable LogicalType outputType) {
        final RelDataType convertedInputRowType = typeFactory.buildRelNodeRowType(inputRowType);

        final RelDataType convertedOutputType;
        if (outputType != null) {
            convertedOutputType = typeFactory.createFieldTypeFromLogicalType(outputType);
        } else {
            convertedOutputType = null;
        }

        return createSqlToRexConverter(convertedInputRowType, convertedOutputType);
    }

    /** Converts {@link Expression} to {@link RexNode}. */
    public RexNode convertExpressionToRex(
            List<RowType.RowField> args, Expression expression, @Nullable LogicalType outputType) {
        final FlinkPlannerImpl planner = plannerSupplier.get();
        final FlinkRelBuilder relBuilder = relBuilderSupplier.apply(planner);
        final FlinkContext context = unwrapContext(relBuilder);
        final Parser parser = createParser(context, planner);

        final RelDataType argRowType = typeFactory.buildRelNodeRowType(new RowType(args));

        final ExpressionResolverBuilder resolverBuilder =
                createExpressionResolverBuilder(context, parser);
        if (outputType != null) {
            resolverBuilder.withOutputDataType(DataTypes.of(outputType));
        } else {
            resolverBuilder.withOutputDataType(null);
        }
        final LocalReferenceExpression[] localRefs =
                args.stream()
                        .map(a -> localRef(a.getName(), DataTypes.of(a.getType())))
                        .toArray(LocalReferenceExpression[]::new);
        final ExpressionResolver resolver = resolverBuilder.withLocalReferences(localRefs).build();

        final ResolvedExpression resolvedExpression =
                resolver.resolve(Collections.singletonList(expression)).get(0);

        relBuilder.values(argRowType);
        return resolvedExpression.accept(new ExpressionConverter(relBuilder));
    }

    private ExpressionResolverBuilder createExpressionResolverBuilder(
            FlinkContext context, Parser parser) {
        return ExpressionResolver.resolverFor(
                context.getTableConfig(),
                context.getClassLoader(),
                name -> Optional.empty(),
                context.getFunctionCatalog().asLookup(parser::parseIdentifier),
                context.getCatalogManager().getDataTypeFactory(),
                parser::parseSqlExpression);
    }

    private Parser createParser(FlinkContext context, FlinkPlannerImpl planner) {
        return new ParserImpl(context.getCatalogManager(), () -> planner, planner::parser, this);
    }
}
