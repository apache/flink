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
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata.MetadataFilterResult;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava33.com.google.common.collect.Sets;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Serializes metadata filter predicates and replays them during compiled plan restoration.
 *
 * <p>Predicates are stored with a {@code predicateRowType} that already uses metadata key names
 * (not SQL aliases). The alias-to-key translation happens once at optimization time, so no
 * column-to-key mapping needs to be persisted.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName("MetadataFilterPushDown")
public final class MetadataFilterPushDownSpec extends SourceAbilitySpecBase {

    public static final String FIELD_NAME_PREDICATES = "predicates";
    public static final String FIELD_NAME_PREDICATE_ROW_TYPE = "predicateRowType";

    @JsonProperty(FIELD_NAME_PREDICATES)
    private final List<RexNode> predicates;

    /**
     * Row type snapshot using metadata key names. Stored because ProjectPushDownSpec may narrow the
     * context's row type during restore.
     */
    @JsonProperty(FIELD_NAME_PREDICATE_ROW_TYPE)
    private final RowType predicateRowType;

    @JsonCreator
    public MetadataFilterPushDownSpec(
            @JsonProperty(FIELD_NAME_PREDICATES) List<RexNode> predicates,
            @JsonProperty(FIELD_NAME_PREDICATE_ROW_TYPE) RowType predicateRowType) {
        this.predicates = new ArrayList<>(checkNotNull(predicates));
        this.predicateRowType = checkNotNull(predicateRowType);
    }

    public List<RexNode> getPredicates() {
        return predicates;
    }

    @Override
    public void apply(DynamicTableSource tableSource, SourceAbilityContext context) {
        List<ResolvedExpression> resolved =
                resolvedExpressions(predicates, predicateRowType, tableSource, context);
        MetadataFilterResult result = applyMetadataFiltersOnSource(tableSource, resolved);
        // On restore every predicate must round-trip back via instance identity. `remaining`
        // is not validated: the spec only stores already-accepted predicates, so the source
        // should re-accept them all.
        Set<ResolvedExpression> accepted = Sets.newIdentityHashSet();
        accepted.addAll(result.getAcceptedFilters());
        Set<ResolvedExpression> inputs = Sets.newIdentityHashSet();
        inputs.addAll(resolved);
        for (ResolvedExpression r : result.getAcceptedFilters()) {
            if (!inputs.contains(r)) {
                throw new TableException(
                        "Source returned an accepted metadata filter not produced by the "
                                + "planner. Sources must return back the same ResolvedExpression "
                                + "instances they received.");
            }
        }
        for (ResolvedExpression r : resolved) {
            if (!accepted.contains(r)) {
                throw new TableException(
                        "All metadata predicates should be accepted on compiled-plan restore. "
                                + "Source dropped a predicate that was accepted at optimization "
                                + "time.");
            }
        }
    }

    /**
     * Resolves predicates to {@link ResolvedExpression}s; the returned list preserves input order,
     * so callers may correlate against the input list by position.
     */
    public static List<ResolvedExpression> resolvedExpressions(
            List<RexNode> predicates,
            RowType metadataKeyRowType,
            DynamicTableSource tableSource,
            SourceAbilityContext context) {
        ensureMetadataFilterPushDown(tableSource);
        return FilterPushDownSpec.resolvePredicates(
                predicates, metadataKeyRowType, tableSource, context);
    }

    /** Pushes already-resolved expressions to the source. */
    public static MetadataFilterResult applyMetadataFiltersOnSource(
            DynamicTableSource tableSource, List<ResolvedExpression> resolved) {
        SupportsReadingMetadata readingMetadata = ensureMetadataFilterPushDown(tableSource);
        return readingMetadata.applyMetadataFilters(resolved);
    }

    private static SupportsReadingMetadata ensureMetadataFilterPushDown(
            DynamicTableSource tableSource) {
        if (!(tableSource instanceof SupportsReadingMetadata)) {
            throw new TableException(
                    String.format(
                            "%s does not support SupportsReadingMetadata.",
                            tableSource.getClass().getName()));
        }
        SupportsReadingMetadata readingMetadata = (SupportsReadingMetadata) tableSource;
        if (!readingMetadata.supportsMetadataFilterPushDown()) {
            throw new TableException(
                    String.format(
                            "%s no longer supports metadata filter push-down.",
                            tableSource.getClass().getName()));
        }
        return readingMetadata;
    }

    @Override
    public boolean needAdjustFieldReferenceAfterProjection() {
        return false;
    }

    @Override
    public String getDigests(SourceAbilityContext context) {
        final List<String> expressionStrs = new ArrayList<>();
        for (RexNode rexNode : predicates) {
            expressionStrs.add(
                    FlinkRexUtil.getExpressionString(
                            rexNode,
                            JavaScalaConversionUtil.toScala(predicateRowType.getFieldNames())));
        }

        return String.format(
                "metadataFilter=[%s]",
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
        MetadataFilterPushDownSpec that = (MetadataFilterPushDownSpec) o;
        return Objects.equals(predicates, that.predicates)
                && Objects.equals(predicateRowType, that.predicateRowType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), predicates, predicateRowType);
    }
}
