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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.resolver.ExpressionResolver;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.planner.plan.utils.RexNodeToExpressionConverter;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TimeZone;
import java.util.stream.Collectors;

import scala.Option;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A sub-class of {@link SourceAbilitySpec} that can not only serialize/deserialize the filter
 * to/from JSON, but also can push the filter into a {@link SupportsFilterPushDown}.
 */
@JsonTypeName("FilterPushDown")
public final class FilterPushDownSpec extends SourceAbilitySpecBase {
    public static final String FIELD_NAME_PREDICATES = "predicates";

    @JsonProperty(FIELD_NAME_PREDICATES)
    private final List<RexNode> predicates;

    /**
     * A flag which indicates all predicates are retained in the outer Filter operator.
     *
     * <p>This flog is only used for optimization phase, and should not be serialized.
     */
    @JsonIgnore private final boolean allPredicatesRetained;

    public FilterPushDownSpec(List<RexNode> predicates, boolean allPredicatesRetained) {
        this.predicates = new ArrayList<>(checkNotNull(predicates));
        this.allPredicatesRetained = allPredicatesRetained;
    }

    @JsonCreator
    public FilterPushDownSpec(@JsonProperty(FIELD_NAME_PREDICATES) List<RexNode> predicates) {
        this(predicates, true);
    }

    @JsonIgnore
    public boolean isAllPredicatesRetained() {
        return allPredicatesRetained;
    }

    public List<RexNode> getPredicates() {
        return predicates;
    }

    @Override
    public void apply(DynamicTableSource tableSource, SourceAbilityContext context) {
        SupportsFilterPushDown.Result result = apply(predicates, tableSource, context);
        if (result.getAcceptedFilters().size() != predicates.size()) {
            throw new TableException("All predicates should be accepted here.");
        }
    }

    public static SupportsFilterPushDown.Result apply(
            List<RexNode> predicates,
            DynamicTableSource tableSource,
            SourceAbilityContext context) {
        if (tableSource instanceof SupportsFilterPushDown) {
            RexNodeToExpressionConverter converter =
                    new RexNodeToExpressionConverter(
                            new RexBuilder(context.getTypeFactory()),
                            context.getSourceRowType().getFieldNames().toArray(new String[0]),
                            context.getFunctionCatalog(),
                            context.getCatalogManager(),
                            TimeZone.getTimeZone(
                                    TableConfigUtils.getLocalTimeZone(context.getTableConfig())),
                            Option.apply(
                                    context.getTypeFactory()
                                            .buildRelNodeRowType(context.getSourceRowType())));
            List<Expression> filters =
                    predicates.stream()
                            .map(
                                    p -> {
                                        scala.Option<ResolvedExpression> expr = p.accept(converter);
                                        if (expr.isDefined()) {
                                            return expr.get();
                                        } else {
                                            throw new TableException(
                                                    String.format(
                                                            "%s can not be converted to Expression, please make sure %s can accept %s.",
                                                            p.toString(),
                                                            tableSource.getClass().getSimpleName(),
                                                            p.toString()));
                                        }
                                    })
                            .collect(Collectors.toList());
            ExpressionResolver resolver =
                    ExpressionResolver.resolverFor(
                                    context.getTableConfig(),
                                    context.getClassLoader(),
                                    name -> Optional.empty(),
                                    context.getFunctionCatalog()
                                            .asLookup(
                                                    str -> {
                                                        throw new TableException(
                                                                "We should not need to lookup any expressions at this point");
                                                    }),
                                    context.getCatalogManager().getDataTypeFactory(),
                                    (sqlExpression, inputRowType, outputType) -> {
                                        throw new TableException(
                                                "SQL expression parsing is not supported at this location.");
                                    })
                            .build();
            return ((SupportsFilterPushDown) tableSource).applyFilters(resolver.resolve(filters));
        } else {
            throw new TableException(
                    String.format(
                            "%s does not support SupportsFilterPushDown.",
                            tableSource.getClass().getName()));
        }
    }

    @Override
    public boolean needAdjustFieldReferenceAfterProjection() {
        return true;
    }

    @Override
    public String getDigests(SourceAbilityContext context) {
        final List<String> expressionStrs = new ArrayList<>();
        final RowType sourceRowType = context.getSourceRowType();
        for (RexNode rexNode : predicates) {
            expressionStrs.add(
                    FlinkRexUtil.getExpressionString(
                            rexNode,
                            JavaScalaConversionUtil.toScala(sourceRowType.getFieldNames())));
        }

        return String.format(
                "filter=[%s]",
                expressionStrs.stream()
                        .reduce((l, r) -> String.format("and(%s, %s)", l, r))
                        .orElse(""));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        FilterPushDownSpec that = (FilterPushDownSpec) o;
        return Objects.equals(predicates, that.predicates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), predicates);
    }
}
