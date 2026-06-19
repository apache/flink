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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.python.PythonFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.CommonPythonUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.AsyncScalarUtil;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Base class for exec Python Async Calc. */
public abstract class CommonExecPythonAsyncCalc extends ExecNodeBase<RowData>
        implements SingleTransformationTranslator<RowData> {

    public static final String PYTHON_ASYNC_CALC_TRANSFORMATION = "python-async-calc";

    public static final String FIELD_NAME_PROJECTION = "projection";

    @JsonProperty(FIELD_NAME_PROJECTION)
    private final List<RexNode> projection;

    public CommonExecPythonAsyncCalc(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            List<RexNode> projection,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.projection = checkNotNull(projection);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final Configuration pythonConfig =
                CommonPythonUtil.extractPythonConfiguration(
                        planner.getTableConfig(), planner.getFlinkContext().getClassLoader());

        OneInputTransformation<RowData, RowData> ret =
                createPythonAsyncOneInputTransformation(
                        inputTransform,
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        pythonConfig);
        if (CommonPythonUtil.isPythonWorkerUsingManagedMemory(
                pythonConfig, planner.getFlinkContext().getClassLoader())) {
            ret.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON);
        }
        return ret;
    }

    /** Creates the transformation for Python async scalar functions. */
    private OneInputTransformation<RowData, RowData> createPythonAsyncOneInputTransformation(
            Transformation<RowData> inputTransform,
            ExecNodeConfig config,
            ClassLoader classLoader,
            Configuration pythonConfig) {

        InternalTypeInfo<RowData> inputTypeInfo =
                (InternalTypeInfo<RowData>) inputTransform.getOutputType();
        InternalTypeInfo<RowData> outputTypeInfo = InternalTypeInfo.of((RowType) getOutputType());

        OneInputStreamOperator<RowData, RowData> pythonAsyncOperator =
                getPythonAsyncOperator(
                        config, classLoader, pythonConfig, inputTypeInfo, outputTypeInfo);

        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(PYTHON_ASYNC_CALC_TRANSFORMATION, config),
                pythonAsyncOperator,
                outputTypeInfo,
                inputTransform.getParallelism(),
                false);
    }

    /** Gets the async operator for executing Python async scalar functions. */
    private OneInputStreamOperator<RowData, RowData> getPythonAsyncOperator(
            ExecNodeConfig config,
            ClassLoader classLoader,
            Configuration pythonConfig,
            InternalTypeInfo<RowData> inputTypeInfo,
            InternalTypeInfo<RowData> outputTypeInfo) {
        boolean isInProcessMode =
                CommonPythonUtil.isPythonWorkerInProcessMode(pythonConfig, classLoader);

        if (!isInProcessMode) {
            throw new UnsupportedOperationException(
                    "Python async scalar function is still not supported for 'thread' mode.");
        }

        // Separate async function calls from forwarded fields
        List<RexCall> asyncRexCalls = new ArrayList<>();
        List<Integer> forwardedFields = new ArrayList<>();

        for (RexNode rexNode : projection) {
            if (rexNode instanceof RexCall) {
                RexCall rexCall = (RexCall) rexNode;
                if (isPythonAsyncCall(rexCall)) {
                    asyncRexCalls.add(rexCall);
                }
            } else if (rexNode instanceof RexInputRef) {
                forwardedFields.add(((RexInputRef) rexNode).getIndex());
            }
        }

        if (asyncRexCalls.isEmpty()) {
            throw new IllegalStateException("No Python async scalar function found in projection");
        }

        // Extract Python function information
        Tuple2<int[], PythonFunctionInfo[]> extractResult =
                extractPythonAsyncScalarFunctionInfos(asyncRexCalls, classLoader);
        int[] udfInputOffsets = extractResult.f0;
        PythonFunctionInfo[] pythonFunctionInfos = extractResult.f1;

        // Calculate types
        final RowType inputType = inputTypeInfo.toRowType();
        final RowType outputType = outputTypeInfo.toRowType();
        final RowType udfInputType = (RowType) Projection.of(udfInputOffsets).project(inputType);
        final int[] forwardedFieldsArray = forwardedFields.stream().mapToInt(x -> x).toArray();
        final RowType forwardedFieldType =
                (RowType) Projection.of(forwardedFieldsArray).project(inputType);
        final RowType udfOutputType =
                (RowType)
                        Projection.range(forwardedFields.size(), outputType.getFieldCount())
                                .project(outputType);

        // Get async options from configuration
        FunctionCallUtil.AsyncOptions asyncOptions = AsyncScalarUtil.getAsyncOptions(config);

        // Get retry options from configuration
        ExecutionConfigOptions.RetryStrategy retryStrategy =
                config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_RETRY_STRATEGY);
        int retryMaxAttempts =
                config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_MAX_ATTEMPTS);
        long retryDelay =
                config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_RETRY_DELAY).toMillis();
        boolean retryEnabled = retryStrategy == ExecutionConfigOptions.RetryStrategy.FIXED_DELAY;

        // Create the Python async scalar function operator
        try {
            Class<?> clazz =
                    CommonPythonUtil.loadClass(
                            "org.apache.flink.table.runtime.operators.python.scalar.async.PythonAsyncScalarFunctionOperator",
                            classLoader);

            Constructor<?> ctor =
                    clazz.getConstructor(
                            Configuration.class,
                            PythonFunctionInfo[].class,
                            RowType.class,
                            RowType.class,
                            RowType.class,
                            GeneratedProjection.class,
                            GeneratedProjection.class,
                            int.class,
                            long.class,
                            boolean.class,
                            int.class,
                            long.class);

            return (OneInputStreamOperator<RowData, RowData>)
                    ctor.newInstance(
                            pythonConfig,
                            pythonFunctionInfos,
                            inputType,
                            udfInputType,
                            udfOutputType,
                            ProjectionCodeGenerator.generateProjection(
                                    new CodeGeneratorContext(config, classLoader),
                                    "UdfInputProjection",
                                    inputType,
                                    udfInputType,
                                    udfInputOffsets),
                            ProjectionCodeGenerator.generateProjection(
                                    new CodeGeneratorContext(config, classLoader),
                                    "ForwardedFieldProjection",
                                    inputType,
                                    forwardedFieldType,
                                    forwardedFieldsArray),
                            asyncOptions.asyncBufferCapacity,
                            asyncOptions.asyncTimeout,
                            retryEnabled,
                            retryMaxAttempts,
                            retryDelay);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Python Async Scalar Function Operator", e);
        }
    }

    private Tuple2<int[], PythonFunctionInfo[]> extractPythonAsyncScalarFunctionInfos(
            List<RexCall> rexCalls, ClassLoader classLoader) {
        LinkedHashMap<RexNode, Integer> inputNodes = new LinkedHashMap<>();
        PythonFunctionInfo[] pythonFunctionInfos =
                rexCalls.stream()
                        .map(
                                x ->
                                        CommonPythonUtil.createPythonFunctionInfo(
                                                x, inputNodes, classLoader))
                        .collect(Collectors.toList())
                        .toArray(new PythonFunctionInfo[rexCalls.size()]);

        int[] udfInputOffsets =
                inputNodes.keySet().stream()
                        .map(
                                x -> {
                                    if (x instanceof RexInputRef) {
                                        return ((RexInputRef) x).getIndex();
                                    } else if (x instanceof RexFieldAccess) {
                                        return ((RexFieldAccess) x).getField().getIndex();
                                    }
                                    return null;
                                })
                        .mapToInt(i -> i)
                        .toArray();
        return Tuple2.of(udfInputOffsets, pythonFunctionInfos);
    }

    private boolean isPythonAsyncCall(RexCall rexCall) {
        if (rexCall.getOperator() instanceof BridgingSqlFunction) {
            BridgingSqlFunction function = (BridgingSqlFunction) rexCall.getOperator();
            FunctionDefinition definition = function.getDefinition();
            if (definition instanceof PythonFunction) {
                return definition.getKind() == FunctionKind.ASYNC_SCALAR;
            }
        }
        return false;
    }
}
