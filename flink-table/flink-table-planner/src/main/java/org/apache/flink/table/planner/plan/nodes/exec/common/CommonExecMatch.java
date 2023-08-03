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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.operator.CepOperator;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.Quantifier;
import org.apache.flink.cep.pattern.conditions.BooleanConditions;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGenUtils;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.MatchCodeGenerator;
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.MatchSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.plan.utils.RexDefaultVisitor;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.match.PatternProcessFunctionRunner;
import org.apache.flink.table.runtime.operators.match.RowDataEventComparator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.MathUtils;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Common {@link ExecNode} which matches along with MATCH_RECOGNIZE. */
public abstract class CommonExecMatch extends ExecNodeBase<RowData>
        implements ExecNode<RowData>, MultipleTransformationTranslator<RowData> {

    public static final String MATCH_TRANSFORMATION = "match";

    private final MatchSpec matchSpec;

    public CommonExecMatch(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            MatchSpec matchSpec,
            List<InputProperty> inputProperties,
            LogicalType outputType,
            String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.matchSpec = checkNotNull(matchSpec);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final RowType inputRowType = (RowType) inputEdge.getOutputType();

        checkOrderKeys(inputRowType);
        final EventComparator<RowData> eventComparator =
                createEventComparator(
                        config, planner.getFlinkContext().getClassLoader(), inputRowType);
        final Transformation<RowData> timestampedInputTransform =
                translateOrder(inputTransform, inputRowType, config);

        final Tuple2<Pattern<RowData, RowData>, List<String>> cepPatternAndNames =
                translatePattern(
                        matchSpec,
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        planner.createRelBuilder(),
                        inputRowType);
        final Pattern<RowData, RowData> cepPattern = cepPatternAndNames.f0;

        // TODO remove this once it is supported in CEP library
        if (NFACompiler.canProduceEmptyMatches(cepPattern)) {
            throw new TableException(
                    "Patterns that can produce empty matches are not supported. There must be at least one non-optional state.");
        }

        // TODO remove this once it is supported in CEP library
        if (cepPattern.getQuantifier().hasProperty(Quantifier.QuantifierProperty.GREEDY)) {
            throw new TableException(
                    "Greedy quantifiers are not allowed as the last element of a Pattern yet. "
                            + "Finish your pattern with either a simple variable or reluctant quantifier.");
        }

        if (matchSpec.isAllRows()) {
            throw new TableException("All rows per match mode is not supported yet.");
        }

        final int[] partitionKeys = matchSpec.getPartition().getFieldIndices();
        final InternalTypeInfo<RowData> inputTypeInfo =
                (InternalTypeInfo<RowData>) inputTransform.getOutputType();
        final TypeSerializer<RowData> inputSerializer =
                inputTypeInfo.createSerializer(planner.getExecEnv().getConfig());
        final NFACompiler.NFAFactory<RowData> nfaFactory =
                NFACompiler.compileFactory(cepPattern, false);
        final MatchCodeGenerator generator =
                new MatchCodeGenerator(
                        new CodeGeneratorContext(
                                config, planner.getFlinkContext().getClassLoader()),
                        planner.createRelBuilder(),
                        false, // nullableInput
                        JavaScalaConversionUtil.toScala(cepPatternAndNames.f1),
                        JavaScalaConversionUtil.toScala(Optional.empty()),
                        CodeGenUtils.DEFAULT_COLLECTOR_TERM());
        generator.bindInput(
                inputRowType,
                CodeGenUtils.DEFAULT_INPUT1_TERM(),
                JavaScalaConversionUtil.toScala(Optional.empty()));
        final PatternProcessFunctionRunner patternProcessFunction =
                generator.generateOneRowPerMatchExpression(
                        (RowType) getOutputType(), partitionKeys, matchSpec.getMeasures());
        final CepOperator<RowData, RowData, RowData> operator =
                new CepOperator<>(
                        inputSerializer,
                        isProcTime(inputRowType),
                        nfaFactory,
                        eventComparator,
                        cepPattern.getAfterMatchSkipStrategy(),
                        patternProcessFunction,
                        null);
        final OneInputTransformation<RowData, RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        timestampedInputTransform,
                        createTransformationMeta(MATCH_TRANSFORMATION, config),
                        operator,
                        InternalTypeInfo.of(getOutputType()),
                        timestampedInputTransform.getParallelism());
        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(
                        planner.getFlinkContext().getClassLoader(), partitionKeys, inputTypeInfo);
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());

        if (inputsContainSingleton()) {
            transform.setParallelism(1);
            transform.setMaxParallelism(1);
        }
        return transform;
    }

    protected void checkOrderKeys(RowType inputRowType) {
    }

    private EventComparator<RowData> createEventComparator(
            ExecNodeConfig config, ClassLoader classLoader, RowType inputRowType) {
        SortSpec orderKeys = matchSpec.getOrderKeys();
        if (orderKeys.getFieldIndices().length > 1) {
            GeneratedRecordComparator rowComparator =
                    ComparatorCodeGenerator.gen(
                            config, classLoader, "RowDataComparator", inputRowType, orderKeys);
            return new RowDataEventComparator(rowComparator);
        } else {
            return null;
        }
    }

    protected Transformation<RowData> translateOrder(
            Transformation<RowData> inputTransform, RowType inputRowType, ExecNodeConfig config) {
        return inputTransform;
    }

    @VisibleForTesting
    public static Tuple2<Pattern<RowData, RowData>, List<String>> translatePattern(
            MatchSpec matchSpec,
            ReadableConfig config,
            ClassLoader classLoader,
            RelBuilder relBuilder,
            RowType inputRowType) {
        final PatternVisitor patternVisitor =
                new PatternVisitor(config, classLoader, relBuilder, inputRowType, matchSpec);

        final Pattern<RowData, RowData> cepPattern;
        PatternWithStrategy resultPatternWithStrategy =
                matchSpec.getPattern().accept(patternVisitor);
        if (matchSpec.getInterval().isPresent()) {
            Time interval = translateTimeBound(matchSpec.getInterval().get());
            cepPattern = resultPatternWithStrategy.getPattern().within(interval);
        } else {
            cepPattern = resultPatternWithStrategy.getPattern();
        }
        resultPatternWithStrategy.checkPatternReasonableEnding();
        return new Tuple2<>(cepPattern, new ArrayList<>(patternVisitor.names));
    }

    private static Time translateTimeBound(RexNode interval) {
        if (interval instanceof RexLiteral) {
            final RexLiteral l = (RexLiteral) interval;
            if (l.getTypeName().getFamily() == SqlTypeFamily.INTERVAL_DAY_TIME) {
                return Time.milliseconds(l.getValueAs(Long.class));
            }
        }
        throw new TableException(
                "Only constant intervals with millisecond resolution are supported as time constraints of patterns.");
    }

    public abstract boolean isProcTime(RowType inputRowType);

    /** The visitor to traverse the pattern RexNode. */
    private static class PatternVisitor extends RexDefaultVisitor<PatternWithStrategy> {
        private final ReadableConfig config;
        private final ClassLoader classLoader;
        private final RelBuilder relBuilder;
        private final RowType inputRowType;
        private final MatchSpec matchSpec;
        private final LinkedHashSet<String> names;
        private PatternWithStrategy patternWithStrategy;

        public PatternVisitor(
                ReadableConfig config,
                ClassLoader classLoader,
                RelBuilder relBuilder,
                RowType inputRowType,
                MatchSpec matchSpec) {
            this.config = config;
            this.classLoader = classLoader;
            this.relBuilder = relBuilder;
            this.inputRowType = inputRowType;
            this.matchSpec = matchSpec;
            this.names = new LinkedHashSet<>();
        }

        @Override
        public PatternWithStrategy visitLiteral(RexLiteral literal) {
            String patternName = literal.getValueAs(String.class);
            patternWithStrategy = translateSingleVariable(patternWithStrategy, patternName);

            RexNode patternDefinition = matchSpec.getPatternDefinitions().get(patternName);
            if (patternDefinition != null) {
                MatchCodeGenerator generator =
                        new MatchCodeGenerator(
                                new CodeGeneratorContext(config, classLoader),
                                relBuilder,
                                false, // nullableInput
                                JavaScalaConversionUtil.toScala(new ArrayList<>(names)),
                                JavaScalaConversionUtil.toScala(Optional.of(patternName)),
                                CodeGenUtils.DEFAULT_COLLECTOR_TERM());
                generator.bindInput(
                        inputRowType,
                        CodeGenUtils.DEFAULT_INPUT1_TERM(),
                        JavaScalaConversionUtil.toScala(Optional.empty()));
                IterativeCondition<RowData> condition =
                        generator.generateIterativeCondition(patternDefinition);
                patternWithStrategy.setPattern(patternWithStrategy.getPattern().where(condition));
                return patternWithStrategy;
            } else {
                patternWithStrategy.setPattern(
                        patternWithStrategy.getPattern().where(BooleanConditions.trueFunction()));
                return patternWithStrategy;
            }
        }

        @Override
        public PatternWithStrategy visitCall(RexCall call) {
            SqlOperator operator = call.getOperator();
            if (operator == SqlStdOperatorTable.PATTERN_CONCAT) {
                patternWithStrategy = call.operands.get(0).accept(this);
                patternWithStrategy = call.operands.get(1).accept(this);
                return patternWithStrategy;
            } else if (operator == SqlStdOperatorTable.PATTERN_QUANTIFIER) {
                final RexLiteral name;
                if (call.operands.get(0) instanceof RexLiteral) {
                    name = (RexLiteral) call.operands.get(0);
                } else {
                    throw new TableException(
                            String.format(
                                    "Expression not supported: %s Group patterns are not supported yet.",
                                    call.operands.get(0)));
                }
                int startNum =
                        MathUtils.checkedDownCast(
                                ((RexLiteral) call.operands.get(1)).getValueAs(Long.class));
                int endNum =
                        MathUtils.checkedDownCast(
                                ((RexLiteral) call.operands.get(2)).getValueAs(Long.class));
                boolean isGreedy = !((RexLiteral) call.operands.get(3)).getValueAs(Boolean.class);

                if (patternWithStrategy != null && patternWithStrategy.isExcluded()) {
                    return decideNextConsumingStrategyByExcludePattern(
                            patternWithStrategy, startNum, endNum, isGreedy);
                } else {
                    patternWithStrategy = name.accept(this);
                    return applyQuantifier(patternWithStrategy, startNum, endNum, isGreedy);
                }
            } else if (operator == SqlStdOperatorTable.PATTERN_ALTER) {
                throw new TableException(
                        String.format(
                                "Expression not supported: %s. Currently, CEP doesn't support branching patterns.",
                                call));
            } else if (operator == SqlStdOperatorTable.PATTERN_PERMUTE) {
                throw new TableException(
                        String.format(
                                "Expression not supported: %s. Currently, CEP doesn't support PERMUTE patterns.",
                                call));
            } else if (operator == SqlStdOperatorTable.PATTERN_EXCLUDE) {
                if (patternWithStrategy == null || patternWithStrategy.getPattern() == null) {
                    throw new TableException(
                            String.format(
                                    "Expression not supported: %s. Currently, CEP doesn't support {- -} patterns at the beginning.",
                                    call));
                }
                if (call.operands.get(0) instanceof RexCall) {
                    patternWithStrategy.setNextConsumingStrategy(
                            Quantifier.ConsumingStrategy.SKIP_TILL_NEXT);
                    RexCall name = (RexCall) call.operands.get(0);
                    patternWithStrategy.setExcluded(true);
                    patternWithStrategy = name.accept(this);
                    return patternWithStrategy;
                } else {
                    throw new TableException(
                            String.format(
                                    "Expression not supported: %s. Currently, CEP doesn't support {- -} patterns without quantifier.",
                                    call));
                }
            } else {
                throw new TableException("This should not happen.");
            }
        }

        @Override
        public PatternWithStrategy visitNode(RexNode rexNode) {
            throw new TableException(
                    String.format("Unsupported expression within Pattern: [%s]", rexNode));
        }

        private PatternWithStrategy translateSingleVariable(
                PatternWithStrategy patternWithStrategy, String patternName) {
            Pattern<RowData, RowData> newPattern;
            if (names.contains(patternName)) {
                throw new TableException(
                        "Pattern variables must be unique. That might change in the future.");
            } else {
                names.add(patternName);
            }
            if (patternWithStrategy != null
                    && patternWithStrategy.getPattern() != null
                    && patternWithStrategy.getNextConsumingStrategy() != null) {
                Pattern<RowData, RowData> previousPattern = patternWithStrategy.getPattern();
                switch (patternWithStrategy.getNextConsumingStrategy()) {
                    case STRICT:
                        newPattern = previousPattern.next(patternName);
                        break;
                    case NOT_NEXT:
                        newPattern = previousPattern.notNext(patternName);
                        break;
                    case SKIP_TILL_NEXT:
                        newPattern = previousPattern.followedBy(patternName);
                        break;
                    case SKIP_TILL_ANY:
                        newPattern = previousPattern.followedByAny(patternName);
                        break;
                    case NOT_FOLLOW:
                        newPattern = previousPattern.notFollowedBy(patternName);
                        break;
                    default:
                        throw new TableException(
                                "Pattern ConsumingStrategy must be <STRICT, SKIP_TILL_NEXT, SKIP_TILL_ANY, NOT_FOLLOW, NOT_NEXT>");
                }
            } else {
                newPattern = Pattern.begin(patternName, translateSkipStrategy());
            }
            PatternWithStrategy newPatternWithStrategy = new PatternWithStrategy();
            newPatternWithStrategy.setPattern(newPattern);
            return newPatternWithStrategy;
        }

        private AfterMatchSkipStrategy translateSkipStrategy() {
            switch (matchSpec.getAfter().getKind()) {
                case LITERAL:
                    SqlMatchRecognize.AfterOption afterOption =
                            ((RexLiteral) matchSpec.getAfter())
                                    .getValueAs(SqlMatchRecognize.AfterOption.class);
                    switch (afterOption) {
                        case SKIP_PAST_LAST_ROW:
                            return AfterMatchSkipStrategy.skipPastLastEvent();
                        case SKIP_TO_NEXT_ROW:
                            return AfterMatchSkipStrategy.skipToNext();
                        default:
                            throw new TableException("This should not happen.");
                    }
                case SKIP_TO_FIRST:
                    return AfterMatchSkipStrategy.skipToFirst(getPatternTarget())
                            .throwExceptionOnMiss();
                case SKIP_TO_LAST:
                    return AfterMatchSkipStrategy.skipToLast(getPatternTarget())
                            .throwExceptionOnMiss();
                default:
                    throw new TableException(
                            String.format(
                                    "Corrupted query tree. Unexpected %s for after match strategy.",
                                    matchSpec.getAfter()));
            }
        }

        private String getPatternTarget() {
            return ((RexLiteral) ((RexCall) matchSpec.getAfter()).getOperands().get(0))
                    .getValueAs(String.class);
        }

        private PatternWithStrategy applyQuantifier(
                PatternWithStrategy patternWithStrategy, int startNum, int endNum, boolean greedy) {
            boolean isOptional = startNum == 0 && endNum == 1;

            final Pattern<RowData, RowData> newPattern;
            if (startNum == 0 && endNum == -1) { // zero or more
                newPattern = patternWithStrategy.getPattern().oneOrMore().optional().consecutive();
            } else if (startNum == 1 && endNum == -1) { // one or more
                newPattern = patternWithStrategy.getPattern().oneOrMore().consecutive();
            } else if (isOptional) { // optional
                newPattern = patternWithStrategy.getPattern().optional();
            } else if (endNum != -1) { // times
                newPattern = patternWithStrategy.getPattern().times(startNum, endNum).consecutive();
            } else { // times or more
                newPattern = patternWithStrategy.getPattern().timesOrMore(startNum).consecutive();
            }

            if (greedy && (isOptional || startNum == endNum)) {
                patternWithStrategy.setPattern(newPattern);
                return patternWithStrategy;
            } else if (greedy) {
                patternWithStrategy.setPattern(newPattern.greedy());
                return patternWithStrategy;
            } else if (isOptional) {
                throw new TableException("Reluctant optional variables are not supported yet.");
            } else {
                patternWithStrategy.setPattern(newPattern);
                return patternWithStrategy;
            }
        }

        private PatternWithStrategy decideNextConsumingStrategyByExcludePattern(
                PatternWithStrategy patternWithStrategy, int startNum, int endNum, boolean greedy) {
            if (startNum == 0 && endNum == -1) {
                patternWithStrategy.setNextConsumingStrategy(
                        greedy
                                ? Quantifier.ConsumingStrategy.SKIP_TILL_ANY
                                : Quantifier.ConsumingStrategy.SKIP_TILL_NEXT);
            } else {
                throw new TableException("Pattern excluded quantifier must be <*?, *>.");
            }
            return patternWithStrategy;
        }
    }

    /**
     * This info is used to temporarily store pattern information and next connection information.
     */
    private static class PatternWithStrategy {
        private Pattern<RowData, RowData> pattern;
        private Quantifier.ConsumingStrategy nextConsumingStrategy =
                Quantifier.ConsumingStrategy.STRICT;
        private boolean isExcluded = false;

        public Pattern<RowData, RowData> getPattern() {
            return pattern;
        }

        public void setPattern(Pattern<RowData, RowData> pattern) {
            this.pattern = pattern;
        }

        public Quantifier.ConsumingStrategy getNextConsumingStrategy() {
            return nextConsumingStrategy;
        }

        public void setNextConsumingStrategy(Quantifier.ConsumingStrategy nextConsumingStrategy) {
            this.nextConsumingStrategy = nextConsumingStrategy;
        }

        public boolean isExcluded() {
            return isExcluded;
        }

        public void setExcluded(boolean excluded) {
            isExcluded = excluded;
        }

        private void checkPatternReasonableEnding() {
            Quantifier.ConsumingStrategy lastConsumingStrategy = this.getNextConsumingStrategy();
            if (lastConsumingStrategy.equals(Quantifier.ConsumingStrategy.SKIP_TILL_NEXT)
                    || lastConsumingStrategy.equals(Quantifier.ConsumingStrategy.SKIP_TILL_ANY)) {
                throw new TableException(
                        "Currently, CEP doesn't support {- -} patterns at the ending.");
            }
        }
    }
}
