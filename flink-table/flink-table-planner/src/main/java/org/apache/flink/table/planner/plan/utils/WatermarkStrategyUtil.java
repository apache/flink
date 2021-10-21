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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.WatermarkGeneratorCodeGenerator;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilityContext;
import org.apache.flink.table.runtime.generated.GeneratedWatermarkGenerator;

import org.apache.calcite.rex.RexNode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import scala.Option;

/** Utilities for generating watermarkStrategy. */
public class WatermarkStrategyUtil {

    /** generate watermarkStrategy using the watermark expression. */
    public static WatermarkStrategy<RowData> generateWatermarkStrategy(
            SourceAbilityContext context, RexNode watermarkExpr, long idleTimeoutMillis) {
        WatermarkStrategy<RowData> watermarkStrategy;
        if (watermarkExpr == null) {
            return WatermarkStrategy.noWatermarks();
        }

        GeneratedWatermarkGenerator generatedWatermarkGenerator =
                WatermarkGeneratorCodeGenerator.generateWatermarkGenerator(
                        context.getTableConfig(),
                        context.getSourceRowType(),
                        watermarkExpr,
                        Option.apply("context"));
        Configuration configuration = context.getTableConfig().getConfiguration();

        WatermarkGeneratorSupplier<RowData> supplier =
                new DefaultWatermarkGeneratorSupplier(configuration, generatedWatermarkGenerator);

        watermarkStrategy = WatermarkStrategy.forGenerator(supplier);
        if (idleTimeoutMillis > 0) {
            watermarkStrategy =
                    watermarkStrategy.withIdleness(Duration.ofMillis(idleTimeoutMillis));
        }

        return watermarkStrategy;
    }

    /**
     * Wrapper of the {@link GeneratedWatermarkGenerator} that is used to create {@link
     * WatermarkGenerator}. The {@link DefaultWatermarkGeneratorSupplier} uses the {@link
     * WatermarkGeneratorSupplier.Context} to init the generated watermark generator.
     */
    public static class DefaultWatermarkGeneratorSupplier
            implements WatermarkGeneratorSupplier<RowData> {
        private static final long serialVersionUID = 1L;

        private final Configuration configuration;
        private final GeneratedWatermarkGenerator generatedWatermarkGenerator;

        public DefaultWatermarkGeneratorSupplier(
                Configuration configuration,
                GeneratedWatermarkGenerator generatedWatermarkGenerator) {
            this.configuration = configuration;
            this.generatedWatermarkGenerator = generatedWatermarkGenerator;
        }

        @Override
        public WatermarkGenerator<RowData> createWatermarkGenerator(Context context) {

            List<Object> references =
                    new ArrayList<>(Arrays.asList(generatedWatermarkGenerator.getReferences()));
            references.add(context);

            org.apache.flink.table.runtime.generated.WatermarkGenerator innerWatermarkGenerator =
                    new GeneratedWatermarkGenerator(
                                    generatedWatermarkGenerator.getClassName(),
                                    generatedWatermarkGenerator.getCode(),
                                    references.toArray(),
                                    configuration)
                            .newInstance(Thread.currentThread().getContextClassLoader());

            try {
                innerWatermarkGenerator.open(configuration);
            } catch (Exception e) {
                throw new RuntimeException("Fail to instantiate generated watermark generator.", e);
            }
            return new DefaultWatermarkGeneratorSupplier.DefaultWatermarkGenerator(
                    innerWatermarkGenerator);
        }

        /**
         * Wrapper of the code-generated {@link
         * org.apache.flink.table.runtime.generated.WatermarkGenerator}.
         */
        public static class DefaultWatermarkGenerator implements WatermarkGenerator<RowData> {
            private static final long serialVersionUID = 1L;

            private final org.apache.flink.table.runtime.generated.WatermarkGenerator
                    innerWatermarkGenerator;
            private Long currentWatermark = Long.MIN_VALUE;

            public DefaultWatermarkGenerator(
                    org.apache.flink.table.runtime.generated.WatermarkGenerator
                            watermarkGenerator) {
                this.innerWatermarkGenerator = watermarkGenerator;
            }

            @Override
            public void onEvent(RowData event, long eventTimestamp, WatermarkOutput output) {
                try {
                    Long watermark = innerWatermarkGenerator.currentWatermark(event);
                    if (watermark != null) {
                        currentWatermark = watermark;
                    }
                } catch (Exception e) {
                    throw new RuntimeException(
                            String.format(
                                    "Generated WatermarkGenerator fails to generate for row: %s.",
                                    event),
                            e);
                }
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput output) {
                output.emitWatermark(new Watermark(currentWatermark));
            }
        }
    }
}
