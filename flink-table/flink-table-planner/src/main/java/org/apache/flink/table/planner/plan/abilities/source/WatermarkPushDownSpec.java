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
import org.apache.flink.configuration.Configuration;
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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.calcite.rex.RexNode;

import java.time.Duration;

import scala.Option;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A sub-class of {@link SourceAbilitySpec} that can not only serialize/deserialize the watermark
 * to/from JSON, but also can push the watermark into a {@link SupportsWatermarkPushDown}.
 */
@JsonTypeName("WatermarkPushDown")
public class WatermarkPushDownSpec extends SourceAbilitySpecBase {
    public static final String FIELD_NAME_WATERMARK_EXPR = "watermarkExpr";
    public static final String FIELD_NAME_IDLE_TIMEOUT_MILLIS = "idleTimeoutMillis";

    @JsonProperty(FIELD_NAME_WATERMARK_EXPR)
    private final RexNode watermarkExpr;

    @JsonProperty(FIELD_NAME_IDLE_TIMEOUT_MILLIS)
    private final long idleTimeoutMillis;

    @JsonCreator
    public WatermarkPushDownSpec(
            @JsonProperty(FIELD_NAME_WATERMARK_EXPR) RexNode watermarkExpr,
            @JsonProperty(FIELD_NAME_IDLE_TIMEOUT_MILLIS) long idleTimeoutMillis,
            @JsonProperty(FIELD_NAME_PRODUCED_TYPE) RowType producedType) {
        super(producedType);
        this.watermarkExpr = checkNotNull(watermarkExpr);
        this.idleTimeoutMillis = idleTimeoutMillis;
    }

    @Override
    public void apply(DynamicTableSource tableSource, SourceAbilityContext context) {
        if (tableSource instanceof SupportsWatermarkPushDown) {
            GeneratedWatermarkGenerator generatedWatermarkGenerator =
                    WatermarkGeneratorCodeGenerator.generateWatermarkGenerator(
                            context.getTableConfig(),
                            context.getSourceRowType(),
                            watermarkExpr,
                            Option.apply("context"));
            Configuration configuration = context.getTableConfig().getConfiguration();

            WatermarkGeneratorSupplier<RowData> supplier =
                    new GeneratedWatermarkGeneratorSupplier(
                            configuration, generatedWatermarkGenerator);

            WatermarkStrategy<RowData> watermarkStrategy = WatermarkStrategy.forGenerator(supplier);
            if (idleTimeoutMillis > 0) {
                watermarkStrategy =
                        watermarkStrategy.withIdleness(Duration.ofMillis(idleTimeoutMillis));
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
    public String getDigests(SourceAbilityContext context) {
        final String expressionStr =
                FlinkRexUtil.getExpressionString(
                        watermarkExpr,
                        JavaScalaConversionUtil.toScala(
                                context.getSourceRowType().getFieldNames()));
        if (idleTimeoutMillis > 0) {
            return String.format(
                    "watermark=[%s], idletimeout=[%d]", expressionStr, idleTimeoutMillis);
        }
        return String.format("watermark=[%s]", expressionStr);
    }
}
