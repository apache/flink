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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

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
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.TableConfig;
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
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.MatchSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.plan.utils.RexDefaultVisitor;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.match.PatternProcessFunctionRunner;
import org.apache.flink.table.runtime.operators.match.RowDataEventComparator;
import org.apache.flink.table.runtime.operators.sink.StreamRecordTimestampInserter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.MathUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Stream {@link ExecNode} which matches along with MATCH_RECOGNIZE. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamExecMatch extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, MultipleTransformationTranslator<RowData> {

    public static final String FIELD_NAME_MATCH_SPEC = "matchSpec";

    @JsonProperty(FIELD_NAME_MATCH_SPEC)
    private final MatchSpec matchSpec;

    public StreamExecMatch(
            MatchSpec matchSpec,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                matchSpec,
                getNewNodeId(),
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecMatch(
            @JsonProperty(FIELD_NAME_MATCH_SPEC) MatchSpec matchSpec,
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.matchSpec = checkNotNull(matchSpec);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final RowType inputRowType = (RowType) inputEdge.getOutputType();

        checkOrderKeys(inputRowType);
        final TableConfig config = planner.getTableConfig();
        final EventComparator<RowData> eventComparator =
                createEventComparator(config, inputRowType);
        final Transformation<RowData> timestampedInputTransform =
                translateOrder(inputTransform, inputRowType);

        final Tuple2<Pattern<RowData, RowData>, List<String>> cepPatternAndNames =
                translatePattern(matchSpec, config, planner.getRelBuilder(), inputRowType);
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
        final SortSpec.SortFieldSpec timeOrderField = matchSpec.getOrderKeys().getFieldSpec(0);
        final LogicalType timeOrderFieldType =
                inputRowType.getTypeAt(timeOrderField.getFieldIndex());

        final boolean isProctime = TypeCheckUtils.isProcTime(timeOrderFieldType);
        final InternalTypeInfo<RowData> inputTypeInfo =
                (InternalTypeInfo<RowData>) inputTransform.getOutputType();
        final TypeSerializer<RowData> inputSerializer =
                inputTypeInfo.createSerializer(planner.getExecEnv().getConfig());
        final NFACompiler.NFAFactory<RowData> nfaFactory =
                NFACompiler.compileFactory(cepPattern, false);
        final MatchCodeGenerator generator =
                new MatchCodeGenerator(
                        new CodeGeneratorContext(config),
                        planner.getRelBuilder(),
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
                        isProctime,
                        nfaFactory,
                        eventComparator,
                        cepPattern.getAfterMatchSkipStrategy(),
                        patternProcessFunction,
                        null);
        final OneInputTransformation<RowData, RowData> transform =
                new OneInputTransformation<>(
                        timestampedInputTransform,
                        getDescription(),
                        operator,
                        InternalTypeInfo.of(getOutputType()),
                        timestampedInputTransform.getParallelism());
        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(partitionKeys, inputTypeInfo);
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());

        if (inputsContainSingleton()) {
            transform.setParallelism(1);
            transform.setMaxParallelism(1);
        }
        return transform;
    }

    private void checkOrderKeys(RowType inputRowType) {
        SortSpec orderKeys = matchSpec.getOrderKeys();
        if (orderKeys.getFieldSize() == 0) {
            throw new TableException("You must specify either rowtime or proctime for order by.");
        }

        SortSpec.SortFieldSpec timeOrderField = orderKeys.getFieldSpec(0);
        int timeOrderFieldIdx = timeOrderField.getFieldIndex();
        LogicalType timeOrderFieldType = inputRowType.getTypeAt(timeOrderFieldIdx);
        // need to identify time between others order fields. Time needs to be first sort element
        if (!TypeCheckUtils.isRowTime(timeOrderFieldType)
                && !TypeCheckUtils.isProcTime(timeOrderFieldType)) {
            throw new TableException(
                    "You must specify either rowtime or proctime for order by as the first one.");
        }

        // time ordering needs to be ascending
        if (!orderKeys.getAscendingOrders()[0]) {
            throw new TableException(
                    "Primary sort order of a streaming table must be ascending on time.");
        }
    }

    private EventComparator<RowData> createEventComparator(
            TableConfig config, RowType inputRowType) {
        SortSpec orderKeys = matchSpec.getOrderKeys();
        if (orderKeys.getFieldIndices().length > 1) {
            GeneratedRecordComparator rowComparator =
                    ComparatorCodeGenerator.gen(
                            config, "RowDataComparator", inputRowType, orderKeys);
            return new RowDataEventComparator(rowComparator);
        } else {
            return null;
        }
    }

    private Transformation<RowData> translateOrder(
            Transformation<RowData> inputTransform, RowType inputRowType) {
        SortSpec.SortFieldSpec timeOrderField = matchSpec.getOrderKeys().getFieldSpec(0);
        int timeOrderFieldIdx = timeOrderField.getFieldIndex();
        LogicalType timeOrderFieldType = inputRowType.getTypeAt(timeOrderFieldIdx);

        if (TypeCheckUtils.isRowTime(timeOrderFieldType)) {
            // copy the rowtime field into the StreamRecord timestamp field
            int precision = getPrecision(timeOrderFieldType);
            Transformation<RowData> transform =
                    new OneInputTransformation<>(
                            inputTransform,
                            String.format(
                                    "StreamRecordTimestampInserter(rowtime field: %s)",
                                    timeOrderFieldIdx),
                            new StreamRecordTimestampInserter(timeOrderFieldIdx, precision),
                            inputTransform.getOutputType(),
                            inputTransform.getParallelism());
            if (inputsContainSingleton()) {
                transform.setParallelism(1);
                transform.setMaxParallelism(1);
            }
            return transform;
        } else {
            return inputTransform;
        }
    }

    @VisibleForTesting
    public static Tuple2<Pattern<RowData, RowData>, List<String>> translatePattern(
            MatchSpec matchSpec, TableConfig config, RelBuilder relBuilder, RowType inputRowType) {
        final PatternVisitor patternVisitor =
                new PatternVisitor(config, relBuilder, inputRowType, matchSpec);

        final Pattern<RowData, RowData> cepPattern;
        if (matchSpec.getInterval().isPresent()) {
            Time interval = translateTimeBound(matchSpec.getInterval().get());
            cepPattern = matchSpec.getPattern().accept(patternVisitor).within(interval);
        } else {
            cepPattern = matchSpec.getPattern().accept(patternVisitor);
        }
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

    /** The visitor to traverse the pattern RexNode. */
    private static class PatternVisitor extends RexDefaultVisitor<Pattern<RowData, RowData>> {
        private final TableConfig config;
        private final RelBuilder relBuilder;
        private final RowType inputRowType;
        private final MatchSpec matchSpec;
        private final LinkedHashSet<String> names;
        private Pattern<RowData, RowData> pattern;

        public PatternVisitor(
                TableConfig config,
                RelBuilder relBuilder,
                RowType inputRowType,
                MatchSpec matchSpec) {
            this.config = config;
            this.relBuilder = relBuilder;
            this.inputRowType = inputRowType;
            this.matchSpec = matchSpec;
            this.names = new LinkedHashSet<>();
        }

        @Override
        public Pattern<RowData, RowData> visitLiteral(RexLiteral literal) {
            String patternName = literal.getValueAs(String.class);
            pattern = translateSingleVariable(pattern, patternName);

            RexNode patternDefinition = matchSpec.getPatternDefinitions().get(patternName);
            if (patternDefinition != null) {
                MatchCodeGenerator generator =
                        new MatchCodeGenerator(
                                new CodeGeneratorContext(config),
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
                return pattern.where(condition);
            } else {
                return pattern.where(BooleanConditions.trueFunction());
            }
        }

        @Override
        public Pattern<RowData, RowData> visitCall(RexCall call) {
            SqlOperator operator = call.getOperator();
            if (operator == SqlStdOperatorTable.PATTERN_CONCAT) {
                pattern = call.operands.get(0).accept(this);
                pattern = call.operands.get(1).accept(this);
                return pattern;
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

                pattern = name.accept(this);
                int startNum =
                        MathUtils.checkedDownCast(
                                ((RexLiteral) call.operands.get(1)).getValueAs(Long.class));
                int endNum =
                        MathUtils.checkedDownCast(
                                ((RexLiteral) call.operands.get(2)).getValueAs(Long.class));
                boolean isGreedy = !((RexLiteral) call.operands.get(3)).getValueAs(Boolean.class);

                return applyQuantifier(pattern, startNum, endNum, isGreedy);
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
                throw new TableException(
                        String.format(
                                "Expression not supported: %s. Currently, CEP doesn't support '{-' '-}' patterns.",
                                call));
            } else {
                throw new TableException("This should not happen.");
            }
        }

        @Override
        public Pattern<RowData, RowData> visitNode(RexNode rexNode) {
            throw new TableException(
                    String.format("Unsupported expression within Pattern: [%s]", rexNode));
        }

        private Pattern<RowData, RowData> translateSingleVariable(
                Pattern<RowData, RowData> previousPattern, String patternName) {
            if (names.contains(patternName)) {
                throw new TableException(
                        "Pattern variables must be unique. That might change in the future.");
            } else {
                names.add(patternName);
            }

            if (previousPattern != null) {
                return previousPattern.next(patternName);
            } else {
                return Pattern.begin(patternName, translateSkipStrategy());
            }
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

        private Pattern<RowData, RowData> applyQuantifier(
                Pattern<RowData, RowData> pattern, int startNum, int endNum, boolean greedy) {
            boolean isOptional = startNum == 0 && endNum == 1;

            final Pattern<RowData, RowData> newPattern;
            if (startNum == 0 && endNum == -1) { // zero or more
                newPattern = pattern.oneOrMore().optional().consecutive();
            } else if (startNum == 1 && endNum == -1) { // one or more
                newPattern = pattern.oneOrMore().consecutive();
            } else if (isOptional) { // optional
                newPattern = pattern.optional();
            } else if (endNum != -1) { // times
                newPattern = pattern.times(startNum, endNum).consecutive();
            } else { // times or more
                newPattern = pattern.timesOrMore(startNum).consecutive();
            }

            if (greedy && (isOptional || startNum == endNum)) {
                return newPattern;
            } else if (greedy) {
                return newPattern.greedy();
            } else if (isOptional) {
                throw new TableException("Reluctant optional variables are not supported yet.");
            } else {
                return newPattern;
            }
        }
    }
}
