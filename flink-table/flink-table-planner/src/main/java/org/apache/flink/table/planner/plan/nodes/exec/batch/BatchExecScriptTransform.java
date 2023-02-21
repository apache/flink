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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.runtime.script.ScriptTransformIOInfo;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;

import java.lang.reflect.Constructor;
import java.util.Collections;

/** Batch {@link ExecNode} for ScripTransform. */
public class BatchExecScriptTransform extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    // currently, only Hive dialect supports ScriptTransform,
    // so make the class name of the operator constructed from this ExecNode a static field
    private static final String HIVE_SCRIPT_TRANSFORM_OPERATOR_NAME =
            "org.apache.flink.table.runtime.operators.hive.script.HiveScriptTransformOperator";

    private final int[] inputIndexes;
    private final String script;
    private final ScriptTransformIOInfo scriptTransformIOInfo;
    private final LogicalType inputType;

    public BatchExecScriptTransform(
            ReadableConfig tableConfig,
            InputProperty inputProperty,
            LogicalType inputType,
            LogicalType outputType,
            String description,
            int[] inputIndexes,
            String script,
            ScriptTransformIOInfo scriptTransformIOInfo) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecLimit.class),
                ExecNodeContext.newPersistedConfig(BatchExecLimit.class, tableConfig),
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.inputIndexes = inputIndexes;
        this.script = script;
        this.inputType = inputType;
        this.scriptTransformIOInfo = scriptTransformIOInfo;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        Transformation<RowData> inputTransform =
                (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);
        OneInputStreamOperator<RowData, RowData> scriptOperator = getScriptTransformOperator();
        return new OneInputTransformation<>(
                inputTransform,
                getDescription(),
                SimpleOperatorFactory.of(scriptOperator),
                InternalTypeInfo.of(getOutputType()),
                inputTransform.getParallelism(),
                false);
    }

    @SuppressWarnings("unchecked")
    private OneInputStreamOperator<RowData, RowData> getScriptTransformOperator() {
        try {
            Class<?> cls =
                    Class.forName(
                            HIVE_SCRIPT_TRANSFORM_OPERATOR_NAME,
                            false,
                            Thread.currentThread().getContextClassLoader());
            Constructor<?> ctor =
                    cls.getConstructor(
                            int[].class,
                            String.class,
                            ScriptTransformIOInfo.class,
                            LogicalType.class,
                            LogicalType.class);
            return (OneInputStreamOperator<RowData, RowData>)
                    ctor.newInstance(
                            inputIndexes,
                            script,
                            scriptTransformIOInfo,
                            inputType,
                            getOutputType());
        } catch (Exception e) {
            throw new TableException("HiveScriptTransformOperator constructed failed.", e);
        }
    }
}
