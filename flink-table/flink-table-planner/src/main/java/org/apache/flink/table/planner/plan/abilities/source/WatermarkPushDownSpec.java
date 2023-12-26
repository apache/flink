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

import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.WatermarkGeneratorCodeGenerator;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedWatermarkGenerator;
import org.apache.flink.table.runtime.generated.GeneratedWatermarkGeneratorSupplier;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.watermark.WatermarkEmitStrategy;
import org.apache.flink.table.watermark.WatermarkParams;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Objects;

import scala.Option;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A sub-class of {@link SourceAbilitySpec} that can not only serialize/deserialize the watermark
 * to/from JSON, but also can push the watermark into a {@link SupportsWatermarkPushDown}.
 */
@JsonTypeName("WatermarkPushDown")
public final class WatermarkPushDownSpec extends SourceAbilitySpecBase {
    public static final String FIELD_NAME_WATERMARK_EXPR = "watermarkExpr";
    public static final String FIELD_NAME_GLOBAL_IDLE_TIMEOUT_MILLIS = "idleTimeoutMillis";
    public static final String FIELD_NAME_WATERMARK_PARAMS = "watermarkParams";

    @JsonProperty(FIELD_NAME_WATERMARK_EXPR)
    private final RexNode watermarkExpr;

    @JsonProperty(FIELD_NAME_GLOBAL_IDLE_TIMEOUT_MILLIS)
    private final long globalIdleTimeoutMillis;

    @Nullable
    @JsonProperty(FIELD_NAME_WATERMARK_PARAMS)
    private final WatermarkParams watermarkParams;

    @JsonCreator
    public WatermarkPushDownSpec(
            @JsonProperty(FIELD_NAME_WATERMARK_EXPR) RexNode watermarkExpr,
            @JsonProperty(FIELD_NAME_GLOBAL_IDLE_TIMEOUT_MILLIS) long globalIdleTimeoutMillis,
            @JsonProperty(FIELD_NAME_PRODUCED_TYPE) RowType producedType,
            @JsonProperty(FIELD_NAME_WATERMARK_PARAMS) WatermarkParams watermarkParams) {
        super(producedType);
        this.watermarkExpr = checkNotNull(watermarkExpr);
        this.globalIdleTimeoutMillis = globalIdleTimeoutMillis;
        this.watermarkParams = watermarkParams;
    }

    @Override
    public void apply(DynamicTableSource tableSource, SourceAbilityContext context) {
        if (tableSource instanceof SupportsWatermarkPushDown) {
            GeneratedWatermarkGenerator generatedWatermarkGenerator =
                    WatermarkGeneratorCodeGenerator.generateWatermarkGenerator(
                            context.getTableConfig(),
                            context.getClassLoader(),
                            context.getSourceRowType(),
                            watermarkExpr,
                            Option.apply("context"));

            WatermarkGeneratorSupplier<RowData> supplier =
                    new GeneratedWatermarkGeneratorSupplier(
                            generatedWatermarkGenerator, watermarkParams);

            WatermarkStrategy<RowData> watermarkStrategy = WatermarkStrategy.forGenerator(supplier);
            if (watermarkParams != null && watermarkParams.alignWatermarkEnabled()) {
                watermarkStrategy =
                        watermarkStrategy.withWatermarkAlignment(
                                watermarkParams.getAlignGroupName(),
                                watermarkParams.getAlignMaxDrift(),
                                watermarkParams.getAlignUpdateInterval());
            }
            long actualIdleTimeoutMillis = calculateIdleTimeoutMillis();
            if (actualIdleTimeoutMillis > 0) {
                watermarkStrategy =
                        watermarkStrategy.withIdleness(Duration.ofMillis(actualIdleTimeoutMillis));
            }
            ((SupportsWatermarkPushDown) tableSource).applyWatermark(watermarkStrategy);
        } else {
            throw new TableException(
                    String.format(
                            "%s does not support SupportsWatermarkPushDown.",
                            tableSource.getClass().getName()));
        }
    }

    @Override
    public boolean needAdjustFieldReferenceAfterProjection() {
        return true;
    }

    public WatermarkPushDownSpec copy(RexNode watermarkExpr, RowType producedType) {
        return new WatermarkPushDownSpec(
                watermarkExpr, globalIdleTimeoutMillis, producedType, watermarkParams);
    }

    public RexNode getWatermarkExpr() {
        return watermarkExpr;
    }

    @Override
    public String getDigests(SourceAbilityContext context) {
        final String expressionStr =
                FlinkRexUtil.getExpressionString(
                        watermarkExpr,
                        JavaScalaConversionUtil.toScala(
                                context.getSourceRowType().getFieldNames()));
        StringBuilder sb = new StringBuilder();
        sb.append("watermark=[").append(expressionStr).append("]");
        long actualIdleTimeoutMillis = calculateIdleTimeoutMillis();
        if (actualIdleTimeoutMillis > 0) {
            sb.append(", idletimeout=[").append(actualIdleTimeoutMillis).append("]");
        }
        if (watermarkParams != null) {
            WatermarkEmitStrategy emitStrategy = watermarkParams.getEmitStrategy();
            sb.append(", watermarkEmitStrategy=[").append(emitStrategy).append("]");
            if (watermarkParams.alignWatermarkEnabled()) {
                sb.append(", watermarkAlignment=[")
                        .append(watermarkParams.getAlignGroupName())
                        .append(", ")
                        .append(watermarkParams.getAlignMaxDrift())
                        .append(", ")
                        .append(watermarkParams.getAlignUpdateInterval())
                        .append("]");
            }
        }
        return sb.toString();
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
        WatermarkPushDownSpec that = (WatermarkPushDownSpec) o;
        return globalIdleTimeoutMillis == that.globalIdleTimeoutMillis
                && Objects.equals(watermarkExpr, that.watermarkExpr)
                && Objects.equals(watermarkParams, that.watermarkParams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(), watermarkExpr, globalIdleTimeoutMillis, watermarkParams);
    }

    private long calculateIdleTimeoutMillis() {
        long actualIdleTimeoutMillis = globalIdleTimeoutMillis;
        if (watermarkParams != null && watermarkParams.getSourceIdleTimeout() >= 0) {
            actualIdleTimeoutMillis = watermarkParams.getSourceIdleTimeout();
        }
        return actualIdleTimeoutMillis;
    }
}
