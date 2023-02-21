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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.functions.python.PythonFunctionKind;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.CommonPythonUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.PythonUtil;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Base class for exec Python Calc. */
public abstract class CommonExecPythonCalc extends ExecNodeBase<RowData>
        implements SingleTransformationTranslator<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(CommonExecPythonCalc.class);

    public static final String PYTHON_CALC_TRANSFORMATION = "python-calc";

    public static final String FIELD_NAME_PROJECTION = "projection";

    private static final String PYTHON_SCALAR_FUNCTION_OPERATOR_NAME =
            "org.apache.flink.table.runtime.operators.python.scalar."
                    + "PythonScalarFunctionOperator";

    private static final String EMBEDDED_PYTHON_SCALAR_FUNCTION_OPERATOR_NAME =
            "org.apache.flink.table.runtime.operators.python.scalar."
                    + "EmbeddedPythonScalarFunctionOperator";

    private static final String ARROW_PYTHON_SCALAR_FUNCTION_OPERATOR_NAME =
            "org.apache.flink.table.runtime.operators.python.scalar.arrow."
                    + "ArrowPythonScalarFunctionOperator";

    @JsonProperty(FIELD_NAME_PROJECTION)
    private final List<RexNode> projection;

    public CommonExecPythonCalc(
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
                        planner.getExecEnv(), config, planner.getFlinkContext().getClassLoader());
        OneInputTransformation<RowData, RowData> ret =
                createPythonOneInputTransformation(
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

    private OneInputTransformation<RowData, RowData> createPythonOneInputTransformation(
            Transformation<RowData> inputTransform,
            ExecNodeConfig config,
            ClassLoader classLoader,
            Configuration pythonConfig) {
        List<RexCall> pythonRexCalls =
                projection.stream()
                        .filter(x -> x instanceof RexCall)
                        .map(x -> (RexCall) x)
                        .collect(Collectors.toList());

        List<Integer> forwardedFields =
                projection.stream()
                        .filter(x -> x instanceof RexInputRef)
                        .map(x -> ((RexInputRef) x).getIndex())
                        .collect(Collectors.toList());

        Tuple2<int[], PythonFunctionInfo[]> extractResult =
                extractPythonScalarFunctionInfos(pythonRexCalls, classLoader);
        int[] pythonUdfInputOffsets = extractResult.f0;
        PythonFunctionInfo[] pythonFunctionInfos = extractResult.f1;
        LogicalType[] inputLogicalTypes =
                ((InternalTypeInfo<RowData>) inputTransform.getOutputType()).toRowFieldTypes();
        InternalTypeInfo<RowData> pythonOperatorInputTypeInfo =
                (InternalTypeInfo<RowData>) inputTransform.getOutputType();

        List<LogicalType> forwardedFieldsLogicalTypes =
                forwardedFields.stream()
                        .map(i -> inputLogicalTypes[i])
                        .collect(Collectors.toList());
        List<LogicalType> pythonCallLogicalTypes =
                pythonRexCalls.stream()
                        .map(node -> FlinkTypeFactory.toLogicalType(node.getType()))
                        .collect(Collectors.toList());
        List<LogicalType> fieldsLogicalTypes = new ArrayList<>();
        fieldsLogicalTypes.addAll(forwardedFieldsLogicalTypes);
        fieldsLogicalTypes.addAll(pythonCallLogicalTypes);
        InternalTypeInfo<RowData> pythonOperatorResultTyeInfo =
                InternalTypeInfo.ofFields(fieldsLogicalTypes.toArray(new LogicalType[0]));
        OneInputStreamOperator<RowData, RowData> pythonOperator =
                getPythonScalarFunctionOperator(
                        config,
                        classLoader,
                        pythonConfig,
                        pythonOperatorInputTypeInfo,
                        pythonOperatorResultTyeInfo,
                        pythonUdfInputOffsets,
                        pythonFunctionInfos,
                        forwardedFields.stream().mapToInt(x -> x).toArray(),
                        pythonRexCalls.stream()
                                .anyMatch(
                                        x ->
                                                PythonUtil.containsPythonCall(
                                                        x, PythonFunctionKind.PANDAS)));

        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(PYTHON_CALC_TRANSFORMATION, config),
                pythonOperator,
                pythonOperatorResultTyeInfo,
                inputTransform.getParallelism(),
                false);
    }

    private Tuple2<int[], PythonFunctionInfo[]> extractPythonScalarFunctionInfos(
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

    @SuppressWarnings("unchecked")
    private OneInputStreamOperator<RowData, RowData> getPythonScalarFunctionOperator(
            ExecNodeConfig config,
            ClassLoader classLoader,
            Configuration pythonConfig,
            InternalTypeInfo<RowData> inputRowTypeInfo,
            InternalTypeInfo<RowData> outputRowTypeInfo,
            int[] udfInputOffsets,
            PythonFunctionInfo[] pythonFunctionInfos,
            int[] forwardedFields,
            boolean isArrow) {
        Class<?> clazz;
        boolean isInProcessMode =
                CommonPythonUtil.isPythonWorkerInProcessMode(pythonConfig, classLoader);
        if (isArrow) {
            clazz =
                    CommonPythonUtil.loadClass(
                            ARROW_PYTHON_SCALAR_FUNCTION_OPERATOR_NAME, classLoader);
            if (!isInProcessMode) {
                LOG.warn(
                        "Vectorized Python scalar function only supports process mode, so fallback to process mode.");
                isInProcessMode = true;
            }
        } else {
            if (isInProcessMode) {
                clazz =
                        CommonPythonUtil.loadClass(
                                PYTHON_SCALAR_FUNCTION_OPERATOR_NAME, classLoader);
            } else {
                clazz =
                        CommonPythonUtil.loadClass(
                                EMBEDDED_PYTHON_SCALAR_FUNCTION_OPERATOR_NAME, classLoader);
            }
        }

        final RowType inputType = inputRowTypeInfo.toRowType();
        final RowType outputType = outputRowTypeInfo.toRowType();
        final RowType udfInputType = (RowType) Projection.of(udfInputOffsets).project(inputType);
        final RowType forwardedFieldType =
                (RowType) Projection.of(forwardedFields).project(inputType);
        final RowType udfOutputType =
                (RowType)
                        Projection.range(forwardedFields.length, outputType.getFieldCount())
                                .project(outputType);

        try {
            if (isInProcessMode) {
                Constructor<?> ctor =
                        clazz.getConstructor(
                                Configuration.class,
                                PythonFunctionInfo[].class,
                                RowType.class,
                                RowType.class,
                                RowType.class,
                                GeneratedProjection.class,
                                GeneratedProjection.class);
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
                                        forwardedFields));
            } else {
                GeneratedProjection forwardedFieldGeneratedProjection = null;
                if (forwardedFields.length > 0) {
                    forwardedFieldGeneratedProjection =
                            ProjectionCodeGenerator.generateProjection(
                                    new CodeGeneratorContext(config, classLoader),
                                    "ForwardedFieldProjection",
                                    inputType,
                                    forwardedFieldType,
                                    forwardedFields);
                }
                Constructor<?> ctor =
                        clazz.getConstructor(
                                Configuration.class,
                                PythonFunctionInfo[].class,
                                RowType.class,
                                RowType.class,
                                RowType.class,
                                int[].class,
                                GeneratedProjection.class);
                return (OneInputStreamOperator<RowData, RowData>)
                        ctor.newInstance(
                                pythonConfig,
                                pythonFunctionInfos,
                                inputType,
                                udfInputType,
                                udfOutputType,
                                udfInputOffsets,
                                forwardedFieldGeneratedProjection);
            }
        } catch (Exception e) {
            throw new TableException("Python Scalar Function Operator constructed failed.", e);
        }
    }
}
