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

package org.apache.flink.table.planner.functions.bridging;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.ContextResolvedFunction;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.SpecializedFunction.ExpressionEvaluatorFactory;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexFactory;
import org.apache.flink.table.planner.calcite.RexTableArgCall;
import org.apache.flink.table.planner.functions.inference.OperatorBindingCallContext;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.SystemTypeInference;
import org.apache.flink.table.types.inference.TraitContext;
import org.apache.flink.table.types.inference.TypeInference;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.TableCharacteristic;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createName;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlFunctionCategory;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlIdentifier;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlOperandTypeChecker;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlOperandTypeInference;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlReturnTypeInference;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Bridges {@link FunctionDefinition} to Calcite's representation of a scalar or table function
 * (either a system or user-defined function).
 */
@Internal
public class BridgingSqlFunction extends SqlFunction {

    // it would be great to inject the factories from the outside, but since the code generation
    // stack is too complex, we pass context with every function
    private final DataTypeFactory dataTypeFactory;
    private final FlinkTypeFactory typeFactory;
    private final RexFactory rexFactory;
    private final ContextResolvedFunction resolvedFunction;

    protected final TypeInference typeInference;

    private BridgingSqlFunction(
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory,
            RexFactory rexFactory,
            SqlKind kind,
            ContextResolvedFunction resolvedFunction,
            TypeInference typeInference) {
        super(
                createName(resolvedFunction),
                createSqlIdentifier(resolvedFunction),
                kind,
                createSqlReturnTypeInference(
                        dataTypeFactory, resolvedFunction.getDefinition(), typeInference),
                createSqlOperandTypeInference(
                        dataTypeFactory, resolvedFunction.getDefinition(), typeInference),
                createSqlOperandTypeChecker(
                        dataTypeFactory, resolvedFunction.getDefinition(), typeInference),
                createSqlFunctionCategory(resolvedFunction));

        this.dataTypeFactory = dataTypeFactory;
        this.typeFactory = typeFactory;
        this.rexFactory = rexFactory;
        this.resolvedFunction = resolvedFunction;
        this.typeInference = typeInference;
    }

    /**
     * Creates an instance of a scalar or table function (either a system or user-defined function).
     *
     * @param dataTypeFactory used for creating {@link DataType}
     * @param typeFactory used for bridging to {@link RelDataType}
     * @param rexFactory used for {@link ExpressionEvaluatorFactory}
     * @param kind commonly used SQL standard function; use {@link SqlKind#OTHER_FUNCTION} if this
     *     function cannot be mapped to a common function kind.
     * @param resolvedFunction system or user-defined {@link FunctionDefinition} with context
     * @param typeInference type inference logic
     */
    public static BridgingSqlFunction of(
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory,
            RexFactory rexFactory,
            SqlKind kind,
            ContextResolvedFunction resolvedFunction,
            TypeInference typeInference) {
        final FunctionKind functionKind = resolvedFunction.getDefinition().getKind();
        checkState(
                functionKind == FunctionKind.SCALAR
                        || functionKind == FunctionKind.ASYNC_SCALAR
                        || functionKind == FunctionKind.TABLE
                        || functionKind == FunctionKind.ASYNC_TABLE
                        || functionKind == FunctionKind.PROCESS_TABLE,
                "Scalar or table function kind expected.");

        final TypeInference systemTypeInference =
                SystemTypeInference.of(functionKind, typeInference);

        if (functionKind == FunctionKind.TABLE
                || functionKind == FunctionKind.ASYNC_TABLE
                || functionKind == FunctionKind.PROCESS_TABLE) {
            return new BridgingSqlFunction.WithTableFunction(
                    dataTypeFactory,
                    typeFactory,
                    rexFactory,
                    kind,
                    resolvedFunction,
                    systemTypeInference);
        }
        return new BridgingSqlFunction(
                dataTypeFactory,
                typeFactory,
                rexFactory,
                kind,
                resolvedFunction,
                systemTypeInference);
    }

    /** Creates an instance of a scalar or table function during translation. */
    public static BridgingSqlFunction of(
            FlinkContext context,
            FlinkTypeFactory typeFactory,
            ContextResolvedFunction resolvedFunction) {
        final DataTypeFactory dataTypeFactory = context.getCatalogManager().getDataTypeFactory();
        final TypeInference typeInference =
                resolvedFunction.getDefinition().getTypeInference(dataTypeFactory);
        return of(
                dataTypeFactory,
                typeFactory,
                context.getRexFactory(),
                SqlKind.OTHER_FUNCTION,
                resolvedFunction,
                typeInference);
    }

    /** Creates an instance of a scalar or table function during translation. */
    public static BridgingSqlFunction of(
            RelOptCluster cluster, ContextResolvedFunction resolvedFunction) {
        final FlinkContext context = ShortcutUtils.unwrapContext(cluster);
        final FlinkTypeFactory typeFactory = ShortcutUtils.unwrapTypeFactory(cluster);
        return of(context, typeFactory, resolvedFunction);
    }

    /** Creates an instance of a scalar or table built-in function during translation. */
    public static BridgingSqlFunction of(
            RelOptCluster cluster, BuiltInFunctionDefinition functionDefinition) {
        return of(
                cluster,
                ContextResolvedFunction.permanent(
                        FunctionIdentifier.of(functionDefinition.getName()), functionDefinition));
    }

    public DataTypeFactory getDataTypeFactory() {
        return dataTypeFactory;
    }

    public FlinkTypeFactory getTypeFactory() {
        return typeFactory;
    }

    public RexFactory getRexFactory() {
        return rexFactory;
    }

    public ContextResolvedFunction getResolvedFunction() {
        return resolvedFunction;
    }

    public FunctionDefinition getDefinition() {
        return resolvedFunction.getDefinition();
    }

    public TypeInference getTypeInference() {
        return typeInference;
    }

    @Override
    public boolean isDeterministic() {
        return resolvedFunction.getDefinition().isDeterministic();
    }

    // --------------------------------------------------------------------------------------------
    // Conditional trait resolution
    // --------------------------------------------------------------------------------------------

    /**
     * Rewrites {@code call} so that the operator's {@link StaticArgument}s have any conditional
     * traits (see {@link StaticArgument#withConditionalTrait}) applied against the call site
     * (PARTITION BY, scalar literals). Downstream consumers can then treat the operator's static
     * arguments as the effective signature and use plain {@code arg.is(SET_SEMANTIC_TABLE)} checks.
     *
     * <p>Called from the two places where a planner-level {@link RexCall} for a PTF is first built
     * for downstream consumption: {@code FlinkLogicalTableFunctionScan} converter (fresh planning)
     * and {@code StreamExecProcessTableFunction.@JsonCreator} (compiled-plan restore). A no-op for
     * non-PTF calls and for PTFs that declare no conditional traits.
     */
    public static RexCall resolveCallTraits(RexCall call) {
        if (!(call.getOperator() instanceof BridgingSqlFunction)) {
            return call;
        }
        final BridgingSqlFunction function = (BridgingSqlFunction) call.getOperator();
        final List<StaticArgument> declared =
                function.typeInference.getStaticArguments().orElse(null);
        if (declared == null || declared.stream().noneMatch(StaticArgument::hasConditionalTraits)) {
            return call;
        }
        final List<RexNode> operands = call.getOperands();
        final CallContext callContext = function.toCallContext(call);
        final List<StaticArgument> resolved =
                IntStream.range(0, declared.size())
                        .mapToObj(
                                i ->
                                        resolveArg(
                                                declared.get(i),
                                                declared,
                                                operands,
                                                i,
                                                callContext))
                        .collect(Collectors.toList());
        if (resolved.equals(declared)) {
            return call;
        }
        final BridgingSqlFunction rewritten = function.withStaticArguments(resolved);
        // Use a fresh RexBuilder from the function's own type factory so this can run from a
        // Jackson @JsonCreator that has no planner context.
        return (RexCall)
                new RexBuilder(function.typeFactory).makeCall(call.getType(), rewritten, operands);
    }

    private static StaticArgument resolveArg(
            StaticArgument declaredArg,
            List<StaticArgument> declared,
            List<RexNode> operands,
            int index,
            CallContext callContext) {
        // We only resolve conditional traits for the Table Argument with conditional traits
        if (!declaredArg.hasConditionalTraits()
                || !(operands.get(index) instanceof RexTableArgCall)) {
            return declaredArg;
        }
        return declaredArg.applyConditionalTraits(
                buildTraitContext((RexTableArgCall) operands.get(index), declared, callContext));
    }

    /**
     * Planner-side adapter to {@link TraitContext}. Sourced from a {@link RexCall}: PARTITION BY
     * via the {@link RexTableArgCall} operand, scalar literals via the {@link CallContext} wrapper.
     * Equivalent to {@link TraitContext#of} but takes its inputs from a planner-side call instead
     * of validation-side {@link org.apache.flink.table.functions.TableSemantics}.
     */
    private static TraitContext buildTraitContext(
            RexTableArgCall tableArgCall, List<StaticArgument> declared, CallContext callContext) {
        return new TraitContext() {
            @Override
            public boolean hasPartitionBy() {
                return tableArgCall.getPartitionKeys().length > 0;
            }

            @Override
            public <T> Optional<T> getScalarArgument(String name, Class<T> clazz) {
                for (int i = 0; i < declared.size(); i++) {
                    final StaticArgument arg = declared.get(i);
                    if (arg.is(StaticArgumentTrait.SCALAR) && arg.getName().equals(name)) {
                        return callContext.getArgumentValue(i, clazz);
                    }
                }
                return Optional.empty();
            }
        };
    }

    /**
     * Builds a {@link CallContext} from the given {@link RexCall} for this function. Wraps the call
     * in an {@link OperatorBindingCallContext} so consumers (trait resolution, codegen, etc.) read
     * scalar arguments through the same coercion path as validation.
     */
    public CallContext toCallContext(RexCall call) {
        return toCallContext(call, null, null, null);
    }

    /**
     * Variant of {@link #toCallContext(RexCall)} that additionally exposes the call's input time
     * columns and changelog modes - needed by the streaming codegen path so PTFs can specialize
     * themselves to the exact call.
     */
    public CallContext toCallContext(
            RexCall call,
            @Nullable List<Integer> inputTimeColumns,
            @Nullable List<ChangelogMode> inputChangelogModes,
            @Nullable ChangelogMode outputChangelogMode) {
        return new OperatorBindingCallContext(
                dataTypeFactory,
                getDefinition(),
                RexCallBinding.create(typeFactory, call, Collections.emptyList()),
                call.getType(),
                inputTimeColumns,
                inputChangelogModes,
                outputChangelogMode);
    }

    /**
     * Returns a copy of this function whose {@link TypeInference} reports the given static
     * arguments. The wrapped input/output strategies are reused unchanged - they ran at validation
     * time and aren't invoked again afterwards.
     */
    private BridgingSqlFunction withStaticArguments(List<StaticArgument> staticArguments) {
        final TypeInference rewritten =
                TypeInference.newBuilder()
                        .staticArguments(staticArguments)
                        .inputTypeStrategy(typeInference.getInputTypeStrategy())
                        .stateTypeStrategies(typeInference.getStateTypeStrategies())
                        .outputTypeStrategy(typeInference.getOutputTypeStrategy())
                        .disableSystemArguments(typeInference.disableSystemArguments())
                        .build();
        if (this instanceof WithTableFunction) {
            return new WithTableFunction(
                    dataTypeFactory,
                    typeFactory,
                    rexFactory,
                    getKind(),
                    resolvedFunction,
                    rewritten);
        }
        return new BridgingSqlFunction(
                dataTypeFactory, typeFactory, rexFactory, getKind(), resolvedFunction, rewritten);
    }

    // --------------------------------------------------------------------------------------------
    // Table function extension
    // --------------------------------------------------------------------------------------------

    /** Special flavor of {@link BridgingSqlFunction} to indicate a table function to Calcite. */
    public static class WithTableFunction extends BridgingSqlFunction implements SqlTableFunction {

        private WithTableFunction(
                DataTypeFactory dataTypeFactory,
                FlinkTypeFactory typeFactory,
                RexFactory rexFactory,
                SqlKind kind,
                ContextResolvedFunction resolvedFunction,
                TypeInference systemTypeInference) {
            super(
                    dataTypeFactory,
                    typeFactory,
                    rexFactory,
                    kind,
                    resolvedFunction,
                    systemTypeInference);
        }

        /**
         * The conversion to a row type is handled by the system type inference.
         *
         * @see FlinkRelBuilder#pushFunctionScan(RelBuilder, SqlOperator, int, Iterable, List)
         */
        @Override
        public SqlReturnTypeInference getRowTypeInference() {
            final SqlReturnTypeInference inference = getReturnTypeInference();
            assert inference != null;
            return (opBinding) -> {
                final RelDataType relDataType = inference.inferReturnType(opBinding);
                assert relDataType != null;
                final List<RelDataTypeField> fields = relDataType.getFieldList();
                return opBinding
                        .getTypeFactory()
                        .createStructType(
                                StructKind.FULLY_QUALIFIED,
                                fields.stream()
                                        .map(RelDataTypeField::getType)
                                        .collect(Collectors.toList()),
                                fields.stream()
                                        .map(RelDataTypeField::getName)
                                        .collect(Collectors.toList()));
            };
        }

        @Override
        public @Nullable TableCharacteristic tableCharacteristic(int ordinal) {
            final List<StaticArgument> args = typeInference.getStaticArguments().orElse(null);
            if (args == null || ordinal >= args.size()) {
                return null;
            }
            final StaticArgument arg = args.get(ordinal);
            final TableCharacteristic.Semantics semantics;
            // Report SET semantics if it may apply - which allows the use of PARTITION BY
            if (arg.is(StaticArgumentTrait.SET_SEMANTIC_TABLE)
                    || arg.hasConditionalTrait(StaticArgumentTrait.SET_SEMANTIC_TABLE)) {
                semantics = TableCharacteristic.Semantics.SET;
            } else if (arg.is(StaticArgumentTrait.ROW_SEMANTIC_TABLE)) {
                semantics = TableCharacteristic.Semantics.ROW;
            } else {
                return null;
            }
            return TableCharacteristic.builder(semantics).build();
        }

        @Override
        public boolean argumentMustBeScalar(int ordinal) {
            final List<StaticArgument> args = typeInference.getStaticArguments().orElse(null);
            if (args == null || ordinal >= args.size()) {
                return true;
            }
            final StaticArgument arg = args.get(ordinal);
            return !arg.is(StaticArgumentTrait.TABLE);
        }
    }
}
