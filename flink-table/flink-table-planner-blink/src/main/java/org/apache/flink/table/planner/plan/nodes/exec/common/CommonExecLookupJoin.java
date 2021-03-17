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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.LookupJoinCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.schema.LegacyTableSourceTable;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.collector.TableFunctionCollector;
import org.apache.flink.table.runtime.collector.TableFunctionResultFuture;
import org.apache.flink.table.runtime.generated.GeneratedCollector;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.generated.GeneratedResultFuture;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.lookup.AsyncLookupJoinRunner;
import org.apache.flink.table.runtime.operators.join.lookup.AsyncLookupJoinWithCalcRunner;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinRunner;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinWithCalcRunner;
import org.apache.flink.table.runtime.types.PlannerTypeUtils;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Base {@link ExecNode} for temporal table join which shares most methods.
 *
 * <p>For a lookup join query:
 *
 * <pre>
 * SELECT T.id, T.content, D.age
 * FROM T JOIN userTable FOR SYSTEM_TIME AS OF T.proctime AS D
 * ON T.content = concat(D.name, '!') AND D.age = 11 AND T.id = D.id
 * WHERE D.name LIKE 'Jack%'
 * </pre>
 *
 * <p>The LookupJoin physical node encapsulates the following RelNode tree:
 *
 * <pre>
 *      Join (l.name = r.name)
 *    /     \
 * RelNode  Calc (concat(name, "!") as name, name LIKE 'Jack%')
 *           |
 *        DimTable (lookup-keys: age=11, id=l.id)
 *     (age, id, name)
 * </pre>
 *
 * <ul>
 *   <li>lookupKeys: [$0=11, $1=l.id] ($0 and $1 is the indexes of age and id in dim table)
 *   <li>calcOnTemporalTable: calc on temporal table rows before join
 *   <li>joinCondition: join condition on temporal table rows after calc
 * </ul>
 *
 * <p>The workflow of lookup join:
 *
 * <p>1) lookup records dimension table using the lookup-keys <br>
 * 2) project & filter on the lookup-ed records <br>
 * 3) join left input record and lookup-ed records <br>
 * 4) only outputs the rows which match to the condition <br>
 */
public abstract class CommonExecLookupJoin extends ExecNodeBase<RowData>
        implements SingleTransformationTranslator<RowData> {

    private final FlinkJoinType joinType;
    /**
     * lookup keys: the key is index in dim table. the value is source of lookup key either constant
     * or field from right table.
     */
    private final Map<Integer, LookupJoinUtil.LookupKey> lookupKeys;
    /** the reference of temporal table to look up. */
    private final RelOptTable temporalTable;
    /** calc performed on rows of temporal table before join. */
    private final @Nullable RexProgram calcOnTemporalTable;
    /** join condition except equi-conditions extracted as lookup keys. */
    private final @Nullable RexNode joinCondition;

    protected CommonExecLookupJoin(
            FlinkJoinType joinType,
            @Nullable RexNode joinCondition,
            // TODO: refactor this into TableSourceTable, once legacy TableSource is removed
            RelOptTable temporalTable,
            @Nullable RexProgram calcOnTemporalTable,
            Map<Integer, LookupJoinUtil.LookupKey> lookupKeys,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(Collections.singletonList(inputProperty), outputType, description);
        this.joinType = joinType;
        this.joinCondition = joinCondition;
        this.lookupKeys = Collections.unmodifiableMap(lookupKeys);
        this.temporalTable = temporalTable;
        this.calcOnTemporalTable = calcOnTemporalTable;
        // validate whether the node is valid and supported.
        validate();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        RowType inputRowType = (RowType) inputEdge.getOutputType();
        RowType tableSourceRowType = FlinkTypeFactory.toLogicalRowType(temporalTable.getRowType());
        RowType resultRowType = (RowType) getOutputType();
        validateLookupKeyType(lookupKeys, inputRowType, tableSourceRowType);

        boolean isAsyncEnabled = false;
        UserDefinedFunction userDefinedFunction =
                LookupJoinUtil.getLookupFunction(temporalTable, lookupKeys.keySet());
        UserDefinedFunctionHelper.prepareInstance(
                planner.getTableConfig().getConfiguration(), userDefinedFunction);

        if (userDefinedFunction instanceof AsyncTableFunction) {
            isAsyncEnabled = true;
        }

        boolean isLeftOuterJoin = joinType == FlinkJoinType.LEFT;
        StreamOperatorFactory<RowData> operatorFactory;
        if (isAsyncEnabled) {
            operatorFactory =
                    createAsyncLookupJoin(
                            planner.getTableConfig(),
                            lookupKeys,
                            (AsyncTableFunction<Object>) userDefinedFunction,
                            planner.getRelBuilder(),
                            inputRowType,
                            tableSourceRowType,
                            resultRowType,
                            isLeftOuterJoin);
        } else {
            operatorFactory =
                    createSyncLookupJoin(
                            planner.getTableConfig(),
                            lookupKeys,
                            (TableFunction<Object>) userDefinedFunction,
                            planner.getRelBuilder(),
                            inputRowType,
                            tableSourceRowType,
                            resultRowType,
                            isLeftOuterJoin,
                            planner.getExecEnv().getConfig().isObjectReuseEnabled());
        }

        Transformation<RowData> inputTransformation =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        return new OneInputTransformation<>(
                inputTransformation,
                getDescription(),
                operatorFactory,
                InternalTypeInfo.of(resultRowType),
                inputTransformation.getParallelism());
    }

    protected void validateLookupKeyType(
            final Map<Integer, LookupJoinUtil.LookupKey> lookupKeys,
            final RowType inputRowType,
            final RowType tableSourceRowType) {
        final List<String> imCompatibleConditions = new LinkedList<>();
        lookupKeys.entrySet().stream()
                .filter(entry -> entry.getValue() instanceof LookupJoinUtil.FieldRefLookupKey)
                .forEach(
                        entry -> {
                            int rightKey = entry.getKey();
                            int leftKey =
                                    ((LookupJoinUtil.FieldRefLookupKey) entry.getValue()).index;
                            LogicalType leftType = inputRowType.getTypeAt(leftKey);
                            LogicalType rightType = tableSourceRowType.getTypeAt(rightKey);
                            boolean isCompatible =
                                    PlannerTypeUtils.isInteroperable(leftType, rightType);
                            if (!isCompatible) {
                                String leftName = inputRowType.getFieldNames().get(leftKey);
                                String rightName = tableSourceRowType.getFieldNames().get(rightKey);
                                imCompatibleConditions.add(
                                        String.format(
                                                "%s[%s]=%s[%s]",
                                                leftName, leftType, rightName, rightType));
                            }
                        });

        if (!imCompatibleConditions.isEmpty()) {
            throw new TableException(
                    "Temporal table join requires equivalent condition "
                            + "of the same type, but the condition is "
                            + StringUtils.join(imCompatibleConditions, ","));
        }
    }

    @SuppressWarnings("unchecked")
    private StreamOperatorFactory<RowData> createAsyncLookupJoin(
            TableConfig config,
            Map<Integer, LookupJoinUtil.LookupKey> allLookupKeys,
            AsyncTableFunction<Object> asyncLookupFunction,
            RelBuilder relBuilder,
            RowType inputRowType,
            RowType tableSourceRowType,
            RowType resultRowType,
            boolean isLeftOuterJoin) {

        int asyncBufferCapacity =
                config.getConfiguration()
                        .getInteger(ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_BUFFER_CAPACITY);
        long asyncTimeout =
                config.getConfiguration()
                        .get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT)
                        .toMillis();

        DataTypeFactory dataTypeFactory =
                ShortcutUtils.unwrapContext(relBuilder).getCatalogManager().getDataTypeFactory();

        LookupJoinCodeGenerator.GeneratedTableFunctionWithDataType<AsyncFunction<RowData, Object>>
                generatedFuncWithType =
                        LookupJoinCodeGenerator.generateAsyncLookupFunction(
                                config,
                                dataTypeFactory,
                                inputRowType,
                                tableSourceRowType,
                                resultRowType,
                                allLookupKeys,
                                LookupJoinUtil.getOrderedLookupKeys(allLookupKeys.keySet()),
                                asyncLookupFunction,
                                StringUtils.join(temporalTable.getQualifiedName(), "."));

        RowType rightRowType =
                Optional.ofNullable(calcOnTemporalTable)
                        .map(RexProgram::getOutputRowType)
                        .map(FlinkTypeFactory::toLogicalRowType)
                        .orElse(tableSourceRowType);
        // a projection or filter after table source scan
        GeneratedResultFuture<TableFunctionResultFuture<RowData>> generatedResultFuture =
                LookupJoinCodeGenerator.generateTableAsyncCollector(
                        config,
                        "TableFunctionResultFuture",
                        inputRowType,
                        rightRowType,
                        JavaScalaConversionUtil.toScala(Optional.ofNullable(joinCondition)));

        DataStructureConverter<?, ?> fetcherConverter =
                DataStructureConverters.getConverter(generatedFuncWithType.dataType());
        AsyncFunction<RowData, RowData> asyncFunc;
        if (calcOnTemporalTable != null) {
            // a projection or filter after table source scan
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalc =
                    LookupJoinCodeGenerator.generateCalcMapFunction(
                            config,
                            JavaScalaConversionUtil.toScala(Optional.of(calcOnTemporalTable)),
                            tableSourceRowType);
            asyncFunc =
                    new AsyncLookupJoinWithCalcRunner(
                            generatedFuncWithType.tableFunc(),
                            (DataStructureConverter<RowData, Object>) fetcherConverter,
                            generatedCalc,
                            generatedResultFuture,
                            InternalSerializers.create(rightRowType),
                            isLeftOuterJoin,
                            asyncBufferCapacity);
        } else {
            // right type is the same as table source row type, because no calc after temporal table
            asyncFunc =
                    new AsyncLookupJoinRunner(
                            generatedFuncWithType.tableFunc(),
                            (DataStructureConverter<RowData, Object>) fetcherConverter,
                            generatedResultFuture,
                            InternalSerializers.create(rightRowType),
                            isLeftOuterJoin,
                            asyncBufferCapacity);
        }

        // force ORDERED output mode currently, optimize it to UNORDERED
        // when the downstream do not need orderness
        return new AsyncWaitOperatorFactory<>(
                asyncFunc, asyncTimeout, asyncBufferCapacity, AsyncDataStream.OutputMode.ORDERED);
    }

    private StreamOperatorFactory<RowData> createSyncLookupJoin(
            TableConfig config,
            Map<Integer, LookupJoinUtil.LookupKey> allLookupKeys,
            TableFunction<?> syncLookupFunction,
            RelBuilder relBuilder,
            RowType inputRowType,
            RowType tableSourceRowType,
            RowType resultRowType,
            boolean isLeftOuterJoin,
            boolean isObjectReuseEnabled) {

        DataTypeFactory dataTypeFactory =
                ShortcutUtils.unwrapContext(relBuilder).getCatalogManager().getDataTypeFactory();

        int[] orderedLookupKeys = LookupJoinUtil.getOrderedLookupKeys(allLookupKeys.keySet());

        GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedFetcher =
                LookupJoinCodeGenerator.generateSyncLookupFunction(
                        config,
                        dataTypeFactory,
                        inputRowType,
                        tableSourceRowType,
                        resultRowType,
                        allLookupKeys,
                        orderedLookupKeys,
                        syncLookupFunction,
                        StringUtils.join(temporalTable.getQualifiedName(), "."),
                        isObjectReuseEnabled);

        RowType rightRowType =
                Optional.ofNullable(calcOnTemporalTable)
                        .map(RexProgram::getOutputRowType)
                        .map(FlinkTypeFactory::toLogicalRowType)
                        .orElse(tableSourceRowType);
        CodeGeneratorContext ctx = new CodeGeneratorContext(config);
        GeneratedCollector<TableFunctionCollector<RowData>> generatedCollector =
                LookupJoinCodeGenerator.generateCollector(
                        ctx,
                        inputRowType,
                        rightRowType,
                        resultRowType,
                        JavaScalaConversionUtil.toScala(Optional.ofNullable(joinCondition)),
                        JavaScalaConversionUtil.toScala(Optional.empty()),
                        true);
        ProcessFunction<RowData, RowData> processFunc;
        if (calcOnTemporalTable != null) {
            // a projection or filter after table source scan
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalc =
                    LookupJoinCodeGenerator.generateCalcMapFunction(
                            config,
                            JavaScalaConversionUtil.toScala(Optional.of(calcOnTemporalTable)),
                            tableSourceRowType);

            processFunc =
                    new LookupJoinWithCalcRunner(
                            generatedFetcher,
                            generatedCalc,
                            generatedCollector,
                            isLeftOuterJoin,
                            rightRowType.getFieldCount());
        } else {
            // right type is the same as table source row type, because no calc after temporal table
            processFunc =
                    new LookupJoinRunner(
                            generatedFetcher,
                            generatedCollector,
                            isLeftOuterJoin,
                            rightRowType.getFieldCount());
        }
        return SimpleOperatorFactory.of(new ProcessOperator<>(processFunc));
    }

    // ----------------------------------------------------------------------------------------
    //                                       Validation
    // ----------------------------------------------------------------------------------------

    private void validate() {

        // validate table source and function implementation first
        validateTableSource();

        // check join on all fields of PRIMARY KEY or (UNIQUE) INDEX
        if (lookupKeys.isEmpty()) {
            throw new TableException(
                    String.format(
                            "Temporal table join requires an equality condition on fields of %s.",
                            getTableSourceDescription()));
        }

        // check type
        if (joinType != FlinkJoinType.LEFT && joinType != FlinkJoinType.INNER) {
            throw new TableException(
                    String.format(
                            "Temporal table join currently only support INNER JOIN and LEFT JOIN, but was %s JOIN.",
                            joinType.toString()));
        }
        // success
    }

    private String getTableSourceDescription() {
        if (temporalTable instanceof TableSourceTable) {
            return String.format(
                    "table [%s]",
                    ((TableSourceTable) temporalTable).tableIdentifier().asSummaryString());
        } else if (temporalTable instanceof LegacyTableSourceTable) {
            return String.format(
                    "table [%s]",
                    ((LegacyTableSourceTable<?>) temporalTable)
                            .tableIdentifier()
                            .asSummaryString());
        }
        // should never reach here.
        return "";
    }

    private void validateTableSource() {
        if (temporalTable instanceof TableSourceTable) {
            if (!(((TableSourceTable) temporalTable).tableSource() instanceof LookupTableSource)) {
                throw new TableException(
                        String.format(
                                "%s must implement LookupTableSource interface if it is used in temporal table join.",
                                getTableSourceDescription()));
            }

        } else if (temporalTable instanceof LegacyTableSourceTable) {

            TableSource<?> tableSource = ((LegacyTableSourceTable<?>) temporalTable).tableSource();
            if (!(tableSource instanceof LookupableTableSource)) {

                throw new TableException(
                        String.format(
                                "%s must implement LookupableTableSource interface if it is used in temporal table join.",
                                getTableSourceDescription()));
            }
            TypeInformation<?> tableSourceProducedType =
                    TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(
                            tableSource.getProducedDataType());
            if (!(tableSourceProducedType instanceof InternalTypeInfo
                            && tableSourceProducedType
                                    .getTypeClass()
                                    .isAssignableFrom(RowData.class))
                    && !(tableSourceProducedType instanceof RowTypeInfo)) {
                throw new TableException(
                        String.format(
                                "Temporal table join only support Row or RowData type as return type of temporal table. But was %s.",
                                tableSourceProducedType));
            }
        } else {
            throw new TableException(
                    String.format(
                            "table [%s] is neither TableSourceTable not LegacyTableSourceTable.",
                            StringUtils.join(temporalTable.getQualifiedName(), ".")));
        }
    }
}
