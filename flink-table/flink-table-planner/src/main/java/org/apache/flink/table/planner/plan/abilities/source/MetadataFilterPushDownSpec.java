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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
        // Use stored predicateRowType; context's row type may be narrowed by ProjectPushDownSpec.
        MetadataFilterResult result =
                applyMetadataFilters(predicates, predicateRowType, tableSource, context);
        if (result.getAcceptedFilters().size() != predicates.size()) {
            throw new TableException("All metadata predicates should be accepted here.");
        }
    }

    /**
     * Converts RexNode predicates to ResolvedExpressions using the given row type and calls
     * applyMetadataFilters on the source. The row type must already use metadata key names.
     */
    public static MetadataFilterResult applyMetadataFilters(
            List<RexNode> predicates,
            RowType metadataKeyRowType,
            DynamicTableSource tableSource,
            SourceAbilityContext context) {
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
        List<ResolvedExpression> resolved =
                FilterPushDownSpec.resolvePredicates(
                        predicates, metadataKeyRowType, tableSource, context);
        return readingMetadata.applyMetadataFilters(resolved);
    }

    @Override
    public boolean needAdjustFieldReferenceAfterProjection() {
        return true;
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
