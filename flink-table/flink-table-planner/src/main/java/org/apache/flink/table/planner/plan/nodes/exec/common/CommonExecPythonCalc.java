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

import org.apache.flink.annotation.VisibleForTesting;
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
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
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
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

    /**
     * Total number of flattened Python UDF calls across all projection trees (-1 = not yet
     * computed).
     */
    private int cseFlattenedCount = -1;

    /** Number of unique Python UDF calls after deduplication (-1 = not yet computed). */
    private int cseDedupedCount = -1;

    /** CSE annotation for plan description (null = not yet computed). */
    private String cseAnnotation = null;

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
                        planner.getTableConfig(), planner.getFlinkContext().getClassLoader());
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

    // -------------------------------------------------------------------------
    //  Common Sub-expression Elimination for Python UDFs
    // -------------------------------------------------------------------------

    /**
     * Creates a projection operator that expands deduplicated results back to the expected output
     * schema. The Python operator produces [forwarded_fields..., unique_calls...], and this
     * projection maps duplicated call positions back to their shared unique result.
     */
    private OneInputTransformation<RowData, RowData> createRefReuseProjection(
            OneInputTransformation<RowData, RowData> pythonTransformation,
            int[] originalToDedup,
            int forwardedCount,
            InternalTypeInfo<RowData> pythonOperatorResultTypeInfo,
            ExecNodeConfig config,
            ClassLoader classLoader) {
        int[] expansionMapping =
                IntStream.concat(
                                IntStream.range(0, forwardedCount),
                                IntStream.of(originalToDedup).map(idx -> idx + forwardedCount))
                        .toArray();

        RowType pythonOutputRowType = pythonOperatorResultTypeInfo.toRowType();
        RowType finalOutputType = (RowType) getOutputType();

        CodeGenOperatorFactory<RowData> projectionOperator =
                ProjectionCodeGenerator.generateProjectionOperator(
                        new CodeGeneratorContext(config, classLoader),
                        pythonOutputRowType,
                        finalOutputType,
                        expansionMapping,
                        "PythonCalcRefReuseProjection");

        String refReuseDetail = buildRefReuseDetailName(originalToDedup, forwardedCount);

        return ExecNodeUtil.createOneInputTransformation(
                pythonTransformation,
                createTransformationMeta(
                        PYTHON_CALC_TRANSFORMATION + "-ref-reuse",
                        refReuseDetail,
                        "PythonCalcRefReuse",
                        config),
                projectionOperator,
                InternalTypeInfo.of(finalOutputType),
                pythonTransformation.getParallelism(),
                false);
    }

    /**
     * Builds a human-readable detail name describing which Python UDF calls reuse earlier
     * deduplicated results. Uses output column names (e.g. EXPR$1, EXPR$2) to identify calls. For
     * example, {@code PythonCalcRefReuse(EXPR$2=EXPR$1)} means EXPR$2 reuses the result of EXPR$1,
     * since both represent the same deterministic expression.
     */
    @VisibleForTesting
    String buildRefReuseDetailName(int[] originalToDedup, int forwardedCount) {
        List<String> fieldNames = ((RowType) getOutputType()).getFieldNames();
        StringBuilder sb = new StringBuilder("PythonCalcRefReuse");
        List<String> reuseDescs = new ArrayList<>();
        for (int i = 0; i < originalToDedup.length; i++) {
            for (int j = 0; j < i; j++) {
                if (originalToDedup[j] == originalToDedup[i]) {
                    String reusedName = fieldNames.get(forwardedCount + j);
                    String reuserName = fieldNames.get(forwardedCount + i);
                    reuseDescs.add(String.format("%s=%s", reuserName, reusedName));
                    break;
                }
            }
        }
        if (!reuseDescs.isEmpty()) {
            sb.append("(");
            sb.append(String.join(", ", reuseDescs));
            sb.append(")");
        }
        return sb.toString();
    }

    /** Builds the output type info for the Python operator after call deduplication. */
    private InternalTypeInfo<RowData> buildDedupOutputTypeInfo(
            List<Integer> forwardedFields,
            LogicalType[] inputLogicalTypes,
            List<RexCall> uniquePythonRexCalls,
            List<String> outputFieldNames) {
        List<LogicalType> fieldTypes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        for (int idx : forwardedFields) {
            fieldTypes.add(inputLogicalTypes[idx]);
        }
        for (RexCall call : uniquePythonRexCalls) {
            fieldTypes.add(FlinkTypeFactory.toLogicalType(call.getType()));
        }
        // Assign field names: use output field names for forwarded fields first,
        // then for UDF results. When needsExpansionProjection is true, the unique
        // call count may differ from the output field count; in that case the
        // expansion projection handles the mapping, and the intermediate field
        // names are derived from the output field names where possible.
        int totalFields = fieldTypes.size();
        int forwardedCount = forwardedFields.size();
        for (int i = 0; i < totalFields && i < outputFieldNames.size(); i++) {
            fieldNames.add(outputFieldNames.get(i));
        }
        // Fill remaining names (if unique calls count exceeds output names, e.g.
        // flattened nested calls produce more fields than the output schema)
        for (int i = fieldNames.size(); i < totalFields; i++) {
            fieldNames.add("f" + i);
        }
        return InternalTypeInfo.ofFields(
                fieldTypes.toArray(new LogicalType[0]), fieldNames.toArray(new String[0]));
    }

    // -------------------------------------------------------------------------
    //  Core translation logic
    // -------------------------------------------------------------------------

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

        LogicalType[] inputLogicalTypes =
                ((InternalTypeInfo<RowData>) inputTransform.getOutputType()).toRowFieldTypes();
        RowType inputType =
                ((InternalTypeInfo<RowData>) inputTransform.getOutputType()).toRowType();

        // Deduplicate identical deterministic Python function calls (full-tree CSE)
        PythonCallCseResult cseResult = PythonCallDeduplicator.deduplicate(pythonRexCalls);
        List<RexCall> uniquePythonRexCalls = cseResult.getUniqueCalls();
        int[] originalToDedup = cseResult.getOriginalToDedupMapping();
        Map<RexCall, Integer> refMap = cseResult.getRefMap();
        boolean needsExpansionProjection = cseResult.needsExpansionProjection();

        // Compute and log CSE statistics
        cseFlattenedCount = PythonCallDeduplicator.countFlattenedCalls(pythonRexCalls);
        cseDedupedCount = uniquePythonRexCalls.size();
        cseAnnotation = buildCseTopLevelAnnotation();
        if (cseFlattenedCount > cseDedupedCount && LOG.isDebugEnabled()) {
            LOG.debug(
                    "Python CSE in {}: {} UDF calls flattened to {} unique ({} saved, {}% reduction)",
                    getDescription(),
                    cseFlattenedCount,
                    cseDedupedCount,
                    cseFlattenedCount - cseDedupedCount,
                    (cseFlattenedCount - cseDedupedCount) * 100 / cseFlattenedCount);
        }

        Tuple2<int[], PythonFunctionInfo[]> extractResult =
                extractPythonScalarFunctionInfos(uniquePythonRexCalls, classLoader, refMap);
        int[] pythonUdfInputOffsets = extractResult.f0;
        PythonFunctionInfo[] pythonFunctionInfos = extractResult.f1;
        InternalTypeInfo<RowData> pythonOperatorInputTypeInfo =
                (InternalTypeInfo<RowData>) inputTransform.getOutputType();

        // Build output type using deduplicated unique calls
        RowType outputType = (RowType) getOutputType();
        InternalTypeInfo<RowData> pythonOperatorResultTypeInfo =
                buildDedupOutputTypeInfo(
                        forwardedFields,
                        inputLogicalTypes,
                        uniquePythonRexCalls,
                        outputType.getFieldNames());

        OneInputStreamOperator<RowData, RowData> pythonOperator =
                getPythonScalarFunctionOperator(
                        config,
                        classLoader,
                        pythonConfig,
                        pythonOperatorInputTypeInfo,
                        pythonOperatorResultTypeInfo,
                        pythonUdfInputOffsets,
                        pythonFunctionInfos,
                        forwardedFields.stream().mapToInt(x -> x).toArray(),
                        uniquePythonRexCalls.stream()
                                .anyMatch(
                                        x ->
                                                PythonUtil.containsPythonCall(
                                                        x, PythonFunctionKind.PANDAS)));

        OneInputTransformation<RowData, RowData> pythonTransformation =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(PYTHON_CALC_TRANSFORMATION, config),
                        pythonOperator,
                        pythonOperatorResultTypeInfo,
                        inputTransform.getParallelism(),
                        false);

        if (!needsExpansionProjection) {
            return pythonTransformation;
        }

        // Append a ref-reuse projection to map deduplicated results back to expected schema
        return createRefReuseProjection(
                pythonTransformation,
                originalToDedup,
                forwardedFields.size(),
                pythonOperatorResultTypeInfo,
                config,
                classLoader);
    }

    private Tuple2<int[], PythonFunctionInfo[]> extractPythonScalarFunctionInfos(
            List<RexCall> rexCalls, ClassLoader classLoader, Map<RexCall, Integer> refMap) {
        LinkedHashMap<RexNode, Integer> inputNodes = new LinkedHashMap<>();
        PythonFunctionInfo[] pythonFunctionInfos =
                rexCalls.stream()
                        .map(
                                x ->
                                        CommonPythonUtil.createPythonFunctionInfo(
                                                x, inputNodes, classLoader, refMap))
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

    @Override
    public String getDescription() {
        ensureCseStatsComputed();
        if (cseAnnotation != null && !cseAnnotation.isEmpty()) {
            return super.getDescription() + cseAnnotation;
        }
        return super.getDescription();
    }

    /**
     * Lazily computes CSE statistics for plan visualization. Called from {@link #getDescription()}
     * which may be invoked before {@link #translateToPlanInternal}.
     */
    private void ensureCseStatsComputed() {
        if (cseFlattenedCount >= 0) {
            return;
        }

        List<RexCall> pythonCalls =
                projection.stream()
                        .filter(x -> x instanceof RexCall)
                        .map(x -> (RexCall) x)
                        .filter(PythonUtil::isPythonCall)
                        .collect(Collectors.toList());

        if (pythonCalls.isEmpty()) {
            cseFlattenedCount = 0;
            cseDedupedCount = 0;
            cseAnnotation = "";
            return;
        }

        cseFlattenedCount = PythonCallDeduplicator.countFlattenedCalls(pythonCalls);
        cseDedupedCount = PythonCallDeduplicator.deduplicate(pythonCalls).getUniqueCount();
        cseAnnotation = buildCseTopLevelAnnotation();
    }

    /**
     * Builds a CSE annotation showing top-level reuse relationships. Example: {@code (CSE: $2->$1,
     * $3->$2)} indicates $2 reuses $1 as a sub-expression.
     */
    private String buildCseTopLevelAnnotation() {
        List<String> outputFieldNames = ((RowType) getOutputType()).getFieldNames();
        return PythonCallDeduplicator.buildCseTopLevelAnnotation(projection, outputFieldNames);
    }
}
