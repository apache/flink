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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.ValuesCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.runtime.operators.values.ValuesInputFormat;
import org.apache.flink.table.runtime.operators.values.ValuesSource;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexLiteral;

import java.util.Collections;
import java.util.List;

/** Base {@link ExecNode} that read records from given values. */
public abstract class CommonExecValues extends ExecNodeBase<RowData>
        implements SingleTransformationTranslator<RowData> {

    public static final String VALUES_TRANSFORMATION = "values";

    public static final String FIELD_NAME_TUPLES = "tuples";

    private final List<List<RexLiteral>> tuples;

    public CommonExecValues(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            List<List<RexLiteral>> tuples,
            RowType outputType,
            String description) {
        super(id, context, persistedConfig, Collections.emptyList(), outputType, description);
        this.tuples = tuples;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ClassLoader classLoader = planner.getFlinkContext().getClassLoader();
        final RowType outputType = (RowType) getOutputType();
        final String description = getClass().getSimpleName();
        final Transformation<RowData> transformation;
        if (legacySourceEnabled()) {
            final ValuesInputFormat inputFormat =
                    ValuesCodeGenerator.generatorInputFormat(
                            config, classLoader, outputType, tuples, description);
            transformation =
                    planner.getExecEnv()
                            .createInput(inputFormat, inputFormat.getProducedType())
                            .getTransformation();
        } else {
            final ValuesSource source =
                    new ValuesSource(
                            ValuesCodeGenerator.generatorFunction(
                                    config, classLoader, outputType, tuples, description),
                            InternalTypeInfo.of(outputType),
                            tuples.size());
            transformation =
                    planner.getExecEnv()
                            .fromSource(
                                    source, WatermarkStrategy.noWatermarks(), VALUES_TRANSFORMATION)
                            .getTransformation();
        }
        createTransformationMeta(VALUES_TRANSFORMATION, config).fill(transformation);
        transformation.setParallelism(1);
        transformation.setMaxParallelism(1);
        return transformation;
    }

    /**
     * Whether VALUES runs as an {@code InputFormatSourceFunction} instead of a {@link
     * ValuesSource}, which checkpoints the position of the next row to emit.
     */
    protected boolean legacySourceEnabled() {
        return true;
    }

    @JsonProperty(value = FIELD_NAME_TUPLES)
    public List<List<RexLiteral>> getTuples() {
        return tuples;
    }
}
