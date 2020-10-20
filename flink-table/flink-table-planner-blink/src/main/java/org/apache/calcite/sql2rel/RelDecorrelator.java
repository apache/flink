/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql2rel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Sets;
import com.google.common.collect.SortedSetMultimap;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSnapshot;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterCorrelateRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.ImmutableBeans;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

import javax.annotation.Nonnull;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Copied to fix CALCITE-4333, should be removed for the next Calcite upgrade.
 *
 * <p>Changes: Line 671 ~ Line 681, Line 430 ~ Line 441.
 */
public class RelDecorrelator implements ReflectiveVisitor {
  //~ Static fields/initializers ---------------------------------------------

  private static final Logger SQL2REL_LOGGER =
          CalciteTrace.getSqlToRelTracer();

  //~ Instance fields --------------------------------------------------------

  private final RelBuilder relBuilder;

  // map built during translation
  protected CorelMap cm;

  private final ReflectUtil.MethodDispatcher<Frame> dispatcher =
          ReflectUtil.createMethodDispatcher(Frame.class, this, "decorrelateRel",
                  RelNode.class);

  // The rel which is being visited
  private RelNode currentRel;

  private final Context context;

  /** Built during decorrelation, of rel to all the newly created correlated
   * variables in its output, and to map old input positions to new input
   * positions. This is from the view point of the parent rel of a new rel. */
  private final Map<RelNode, Frame> map = new HashMap<>();

  private final HashSet<Correlate> generatedCorRels = new HashSet<>();

  //~ Constructors -----------------------------------------------------------

  protected RelDecorrelator(
          CorelMap cm,
          Context context,
          RelBuilder relBuilder) {
    this.cm = cm;
    this.context = context;
    this.relBuilder = relBuilder;
  }

  //~ Methods ----------------------------------------------------------------

  @Deprecated // to be removed before 2.0
  public static RelNode decorrelateQuery(RelNode rootRel) {
    final RelBuilder relBuilder =
            RelFactories.LOGICAL_BUILDER.create(rootRel.getCluster(), null);
    return decorrelateQuery(rootRel, relBuilder);
  }

  /** Decorrelates a query.
   *
   * <p>This is the main entry point to {@code RelDecorrelator}.
   *
   * @param rootRel           Root node of the query
   * @param relBuilder        Builder for relational expressions
   *
   * @return Equivalent query with all
   * {@link org.apache.calcite.rel.core.Correlate} instances removed
   */
  public static RelNode decorrelateQuery(RelNode rootRel,
                                         RelBuilder relBuilder) {
    final CorelMap corelMap = new CorelMapBuilder().build(rootRel);
    if (!corelMap.hasCorrelation()) {
      return rootRel;
    }

    final RelOptCluster cluster = rootRel.getCluster();
    final RelDecorrelator decorrelator =
            new RelDecorrelator(corelMap,
                    cluster.getPlanner().getContext(), relBuilder);

    RelNode newRootRel = decorrelator.removeCorrelationViaRule(rootRel);

    if (SQL2REL_LOGGER.isDebugEnabled()) {
      SQL2REL_LOGGER.debug(
              RelOptUtil.dumpPlan("Plan after removing Correlator", newRootRel,
                      SqlExplainFormat.TEXT, SqlExplainLevel.EXPPLAN_ATTRIBUTES));
    }

    if (!decorrelator.cm.mapCorToCorRel.isEmpty()) {
      newRootRel = decorrelator.decorrelate(newRootRel);
    }

    // Re-propagate the hints.
    newRootRel = RelOptUtil.propagateRelHints(newRootRel, true);

    return newRootRel;
  }

  private void setCurrent(RelNode root, Correlate corRel) {
    currentRel = corRel;
    if (corRel != null) {
      cm = new CorelMapBuilder().build(Util.first(root, corRel));
    }
  }

  protected RelBuilderFactory relBuilderFactory() {
    return RelBuilder.proto(relBuilder);
  }

  protected RelNode decorrelate(RelNode root) {
    // first adjust count() expression if any
    final RelBuilderFactory f = relBuilderFactory();
    HepProgram program = HepProgram.builder()
            .addRuleInstance(
                    AdjustProjectForCountAggregateRule.config(false, this, f).toRule())
            .addRuleInstance(
                    AdjustProjectForCountAggregateRule.config(true, this, f).toRule())
            .addRuleInstance(
                    FilterJoinRule.FilterIntoJoinRule.Config.DEFAULT
                            .withRelBuilderFactory(f)
                            .withOperandSupplier(b0 ->
                                    b0.operand(Filter.class).oneInput(b1 ->
                                            b1.operand(Join.class).anyInputs()))
                            .withDescription("FilterJoinRule:filter")
                            .as(FilterJoinRule.FilterIntoJoinRule.Config.class)
                            .withSmart(true)
                            .withPredicate((join, joinType, exp) -> true)
                            .as(FilterJoinRule.FilterIntoJoinRule.Config.class)
                            .toRule())
            .addRuleInstance(
                    CoreRules.FILTER_PROJECT_TRANSPOSE.config
                            .withRelBuilderFactory(f)
                            .as(FilterProjectTransposeRule.Config.class)
                            .withOperandFor(Filter.class, filter ->
                                            !RexUtil.containsCorrelation(filter.getCondition()),
                                    Project.class, project -> true)
                            .withCopyFilter(true)
                            .withCopyProject(true)
                            .toRule())
            .addRuleInstance(FilterCorrelateRule.Config.DEFAULT
                    .withRelBuilderFactory(f)
                    .toRule())
            .build();

    HepPlanner planner = createPlanner(program);

    planner.setRoot(root);
    root = planner.findBestExp();

    // Perform decorrelation.
    map.clear();

    final Frame frame = getInvoke(root, null);
    if (frame != null) {
      // has been rewritten; apply rules post-decorrelation
      final HepProgram program2 = HepProgram.builder()
              .addRuleInstance(
                      CoreRules.FILTER_INTO_JOIN.config
                              .withRelBuilderFactory(f)
                              .toRule())
              .addRuleInstance(
                      CoreRules.JOIN_CONDITION_PUSH.config
                              .withRelBuilderFactory(f)
                              .toRule())
              .build();

      final HepPlanner planner2 = createPlanner(program2);
      final RelNode newRoot = frame.r;
      planner2.setRoot(newRoot);
      return planner2.findBestExp();
    }

    return root;
  }

  private Function2<RelNode, RelNode, Void> createCopyHook() {
    return (oldNode, newNode) -> {
      if (cm.mapRefRelToCorRef.containsKey(oldNode)) {
        cm.mapRefRelToCorRef.putAll(newNode,
                cm.mapRefRelToCorRef.get(oldNode));
      }
      if (oldNode instanceof Correlate
              && newNode instanceof Correlate) {
        Correlate oldCor = (Correlate) oldNode;
        CorrelationId c = oldCor.getCorrelationId();
        if (cm.mapCorToCorRel.get(c) == oldNode) {
          cm.mapCorToCorRel.put(c, newNode);
        }

        if (generatedCorRels.contains(oldNode)) {
          generatedCorRels.add((Correlate) newNode);
        }
      }
      return null;
    };
  }

  private HepPlanner createPlanner(HepProgram program) {
    // Create a planner with a hook to update the mapping tables when a
    // node is copied when it is registered.
    return new HepPlanner(
            program,
            context,
            true,
            createCopyHook(),
            RelOptCostImpl.FACTORY);
  }

  public RelNode removeCorrelationViaRule(RelNode root) {
    final RelBuilderFactory f = relBuilderFactory();
    HepProgram program = HepProgram.builder()
            .addRuleInstance(RemoveSingleAggregateRule.config(f).toRule())
            .addRuleInstance(
                    RemoveCorrelationForScalarProjectRule.config(this, f).toRule())
            .addRuleInstance(
                    RemoveCorrelationForScalarAggregateRule.config(this, f).toRule())
            .build();

    HepPlanner planner = createPlanner(program);

    planner.setRoot(root);
    return planner.findBestExp();
  }

  protected RexNode decorrelateExpr(RelNode currentRel,
                                    Map<RelNode, Frame> map, CorelMap cm, RexNode exp) {
    DecorrelateRexShuttle shuttle =
            new DecorrelateRexShuttle(currentRel, map, cm);
    return exp.accept(shuttle);
  }

  protected RexNode removeCorrelationExpr(
          RexNode exp,
          boolean projectPulledAboveLeftCorrelator) {
    RemoveCorrelationRexShuttle shuttle =
            new RemoveCorrelationRexShuttle(relBuilder.getRexBuilder(),
                    projectPulledAboveLeftCorrelator, null, ImmutableSet.of());
    return exp.accept(shuttle);
  }

  protected RexNode removeCorrelationExpr(
          RexNode exp,
          boolean projectPulledAboveLeftCorrelator,
          RexInputRef nullIndicator) {
    RemoveCorrelationRexShuttle shuttle =
            new RemoveCorrelationRexShuttle(relBuilder.getRexBuilder(),
                    projectPulledAboveLeftCorrelator, nullIndicator,
                    ImmutableSet.of());
    return exp.accept(shuttle);
  }

  protected RexNode removeCorrelationExpr(
          RexNode exp,
          boolean projectPulledAboveLeftCorrelator,
          Set<Integer> isCount) {
    RemoveCorrelationRexShuttle shuttle =
            new RemoveCorrelationRexShuttle(relBuilder.getRexBuilder(),
                    projectPulledAboveLeftCorrelator, null, isCount);
    return exp.accept(shuttle);
  }

  /** Fallback if none of the other {@code decorrelateRel} methods match. */
  public Frame decorrelateRel(RelNode rel) {
    RelNode newRel = rel.copy(rel.getTraitSet(), rel.getInputs());

    if (rel.getInputs().size() > 0) {
      List<RelNode> oldInputs = rel.getInputs();
      List<RelNode> newInputs = new ArrayList<>();
      for (int i = 0; i < oldInputs.size(); ++i) {
        final Frame frame = getInvoke(oldInputs.get(i), rel);
        if (frame == null || !frame.corDefOutputs.isEmpty()) {
          // if input is not rewritten, or if it produces correlated
          // variables, terminate rewrite
          return null;
        }
        newInputs.add(frame.r);
        newRel.replaceInput(i, frame.r);
      }

      if (!Util.equalShallow(oldInputs, newInputs)) {
        newRel = rel.copy(rel.getTraitSet(), newInputs);
      }
    }

    // the output position should not change since there are no corVars
    // coming from below.
    return register(rel, newRel, identityMap(rel.getRowType().getFieldCount()),
            ImmutableSortedMap.of());
  }

  public Frame decorrelateRel(Sort rel) {
    //
    // Rewrite logic:
    //
    // 1. change the collations field to reference the new input.
    //

    // Sort itself should not reference corVars.
    assert !cm.mapRefRelToCorRef.containsKey(rel);

    // Sort only references field positions in collations field.
    // The collations field in the newRel now need to refer to the
    // new output positions in its input.
    // Its output does not change the input ordering, so there's no
    // need to call propagateExpr.

    final RelNode oldInput = rel.getInput();
    final Frame frame = getInvoke(oldInput, rel);
    if (frame == null) {
      // If input has not been rewritten, do not rewrite this rel.
      return null;
    }

    final RelNode newInput = frame.r;

    Mappings.TargetMapping mapping =
            Mappings.target(frame.oldToNewOutputs,
                    oldInput.getRowType().getFieldCount(),
                    newInput.getRowType().getFieldCount());

    RelCollation oldCollation = rel.getCollation();
    RelCollation newCollation = RexUtil.apply(mapping, oldCollation);

    final int offset = rel.offset == null ? -1 : RexLiteral.intValue(rel.offset);
    final int fetch = rel.fetch == null ? -1 : RexLiteral.intValue(rel.fetch);

    final RelNode newSort = relBuilder
            .push(newInput)
            .sortLimit(offset, fetch, relBuilder.fields(newCollation))
            .build();

    // Sort does not change input ordering
    return register(rel, newSort, frame.oldToNewOutputs, frame.corDefOutputs);
  }

  public Frame decorrelateRel(Values rel) {
    // There are no inputs, so rel does not need to be changed.
    return null;
  }

  public Frame decorrelateRel(LogicalAggregate rel) {
    return decorrelateRel((Aggregate) rel);
  }

  public Frame decorrelateRel(Aggregate rel) {
    //
    // Rewrite logic:
    //
    // 1. Permute the group by keys to the front.
    // 2. If the input of an aggregate produces correlated variables,
    //    add them to the group list.
    // 3. Change aggCalls to reference the new project.
    //

    // Aggregate itself should not reference corVars.
    assert !cm.mapRefRelToCorRef.containsKey(rel);

    final RelNode oldInput = rel.getInput();
    final Frame frame = getInvoke(oldInput, rel);
    if (frame == null) {
      // If input has not been rewritten, do not rewrite this rel.
      return null;
    }
    final RelNode newInput = frame.r;

    // aggregate outputs mapping: group keys and aggregates
    final Map<Integer, Integer> outputMap = new HashMap<>();

    // map from newInput
    final Map<Integer, Integer> mapNewInputToProjOutputs = new HashMap<>();
    final int oldGroupKeyCount = rel.getGroupSet().cardinality();

    // Project projects the original expressions,
    // plus any correlated variables the input wants to pass along.
    final List<Pair<RexNode, String>> projects = new ArrayList<>();

    List<RelDataTypeField> newInputOutput =
            newInput.getRowType().getFieldList();

    int newPos = 0;

    // oldInput has the original group by keys in the front.
    final NavigableMap<Integer, RexLiteral> omittedConstants = new TreeMap<>();
    for (int i = 0; i < oldGroupKeyCount; i++) {
      final RexLiteral constant = projectedLiteral(newInput, i);
      if (constant != null) {
        // Exclude constants. Aggregate({true}) occurs because Aggregate({})
        // would generate 1 row even when applied to an empty table.
        omittedConstants.put(i, constant);
        continue;
      }

      // add mapping of group keys.
      outputMap.put(i, newPos);
      int newInputPos = frame.oldToNewOutputs.get(i);
      projects.add(RexInputRef.of2(newInputPos, newInputOutput));
      mapNewInputToProjOutputs.put(newInputPos, newPos);
      newPos++;
    }

    final SortedMap<CorDef, Integer> corDefOutputs = new TreeMap<>();
    if (!frame.corDefOutputs.isEmpty()) {
      // If input produces correlated variables, move them to the front,
      // right after any existing GROUP BY fields.

      // Now add the corVars from the input, starting from
      // position oldGroupKeyCount.
      for (Map.Entry<CorDef, Integer> entry : frame.corDefOutputs.entrySet()) {
        projects.add(RexInputRef.of2(entry.getValue(), newInputOutput));

        corDefOutputs.put(entry.getKey(), newPos);
        mapNewInputToProjOutputs.put(entry.getValue(), newPos);
        newPos++;
      }
    }

    // add the remaining fields
    final int newGroupKeyCount = newPos;
    for (int i = 0; i < newInputOutput.size(); i++) {
      if (!mapNewInputToProjOutputs.containsKey(i)) {
        projects.add(RexInputRef.of2(i, newInputOutput));
        mapNewInputToProjOutputs.put(i, newPos);
        newPos++;
      }
    }

    assert newPos == newInputOutput.size();

    // This Project will be what the old input maps to,
    // replacing any previous mapping from old input).
    RelNode newProject = relBuilder.push(newInput)
            .projectNamed(Pair.left(projects), Pair.right(projects), true)
            .build();

    newProject = RelOptUtil.copyRelHints(newInput, newProject);

    // update mappings:
    // oldInput ----> newInput
    //
    //                newProject
    //                   |
    // oldInput ----> newInput
    //
    // is transformed to
    //
    // oldInput ----> newProject
    //                   |
    //                newInput
    Map<Integer, Integer> combinedMap = new HashMap<>();

    for (Integer oldInputPos : frame.oldToNewOutputs.keySet()) {
      combinedMap.put(oldInputPos,
              mapNewInputToProjOutputs.get(
                      frame.oldToNewOutputs.get(oldInputPos)));
    }

    register(oldInput, newProject, combinedMap, corDefOutputs);

    // now it's time to rewrite the Aggregate
    final ImmutableBitSet newGroupSet = ImmutableBitSet.range(newGroupKeyCount);
    List<AggregateCall> newAggCalls = new ArrayList<>();
    List<AggregateCall> oldAggCalls = rel.getAggCallList();

    final Iterable<ImmutableBitSet> newGroupSets;
    if (rel.getGroupType() == Aggregate.Group.SIMPLE) {
      newGroupSets = null;
    } else {
      final ImmutableBitSet addedGroupSet =
              ImmutableBitSet.range(oldGroupKeyCount, newGroupKeyCount);
      newGroupSets =
              ImmutableBitSet.ORDERING.immutableSortedCopy(
                      Util.transform(rel.getGroupSets(),
                              bitSet -> bitSet.union(addedGroupSet)));
    }

    int oldInputOutputFieldCount = rel.getGroupSet().cardinality();
    int newInputOutputFieldCount = newGroupSet.cardinality();

    int i = -1;
    for (AggregateCall oldAggCall : oldAggCalls) {
      ++i;
      List<Integer> oldAggArgs = oldAggCall.getArgList();

      List<Integer> aggArgs = new ArrayList<>();

      // Adjust the Aggregate argument positions.
      // Note Aggregate does not change input ordering, so the input
      // output position mapping can be used to derive the new positions
      // for the argument.
      for (int oldPos : oldAggArgs) {
        aggArgs.add(combinedMap.get(oldPos));
      }
      final int filterArg = oldAggCall.filterArg < 0 ? oldAggCall.filterArg
              : combinedMap.get(oldAggCall.filterArg);

      newAggCalls.add(
              oldAggCall.adaptTo(newProject, aggArgs, filterArg,
                      oldGroupKeyCount, newGroupKeyCount));

      // The old to new output position mapping will be the same as that
      // of newProject, plus any aggregates that the oldAgg produces.
      outputMap.put(
              oldInputOutputFieldCount + i,
              newInputOutputFieldCount + i);
    }

    relBuilder.push(newProject)
            .aggregate(newGroupSets == null
                            ? relBuilder.groupKey(newGroupSet)
                            : relBuilder.groupKey(newGroupSet, newGroupSets),
                    newAggCalls);

    if (!omittedConstants.isEmpty()) {
      final List<RexNode> postProjects = new ArrayList<>(relBuilder.fields());
      for (Map.Entry<Integer, RexLiteral> entry
              : omittedConstants.descendingMap().entrySet()) {
        int index = entry.getKey() + frame.corDefOutputs.size();
        postProjects.add(index, entry.getValue());
        // Shift the outputs whose index equals with or bigger than the added index
        // with 1 offset.
        shiftMapping(outputMap, index, 1);
        // Then add the constant key mapping.
        outputMap.put(entry.getKey(), index);
      }
      relBuilder.project(postProjects);
    }

    RelNode newRel = RelOptUtil.copyRelHints(rel, relBuilder.build());

    // Aggregate does not change input ordering so corVars will be
    // located at the same position as the input newProject.
    return register(rel, newRel, outputMap, corDefOutputs);
  }

  /**
   * Shift the mapping to fixed offset from the {@code startIndex}.
   *
   * @param mapping    The original mapping
   * @param startIndex Any output whose index equals with or bigger than the starting index
   *                   would be shift
   * @param offset     Shift offset
   */
  private static void shiftMapping(Map<Integer, Integer> mapping, int startIndex, int offset) {
    for (Map.Entry<Integer, Integer> entry : mapping.entrySet()) {
      if (entry.getValue() >= startIndex) {
        mapping.put(entry.getKey(), entry.getValue() + offset);
      } else {
        mapping.put(entry.getKey(), entry.getValue());
      }
    }
  }

  public Frame getInvoke(RelNode r, RelNode parent) {
    final Frame frame = dispatcher.invoke(r);
    if (frame != null && parent instanceof Correlate && r instanceof Sort) {
      Sort sort = (Sort) r;
      // Can not decorrelate if the sort has per-correlate-key attributes like
      // offset or fetch limit, because these attributes scope would change to
      // global after decorrelation. They should take effect within the scope
      // of the correlation key actually.
      if (sort.offset != null || sort.fetch != null) {
        currentRel = parent;
        return null;
      }
    }
    if (frame != null) {
      map.put(r, frame);
    }
    currentRel = parent;
    return frame;
  }

  /** Returns a literal output field, or null if it is not literal. */
  private static RexLiteral projectedLiteral(RelNode rel, int i) {
    if (rel instanceof Project) {
      final Project project = (Project) rel;
      final RexNode node = project.getProjects().get(i);
      if (node instanceof RexLiteral) {
        return (RexLiteral) node;
      }
    }
    return null;
  }

  public Frame decorrelateRel(LogicalProject rel) {
    return decorrelateRel((Project) rel);
  }

  public Frame decorrelateRel(Project rel) {
    //
    // Rewrite logic:
    //
    // 1. Pass along any correlated variables coming from the input.
    //

    final RelNode oldInput = rel.getInput();
    Frame frame = getInvoke(oldInput, rel);
    if (frame == null) {
      // If input has not been rewritten, do not rewrite this rel.
      return null;
    }
    final List<RexNode> oldProjects = rel.getProjects();
    final List<RelDataTypeField> relOutput = rel.getRowType().getFieldList();

    // Project projects the original expressions,
    // plus any correlated variables the input wants to pass along.
    final List<Pair<RexNode, String>> projects = new ArrayList<>();

    // If this Project has correlated reference, create value generator
    // and produce the correlated variables in the new output.
    if (cm.mapRefRelToCorRef.containsKey(rel)) {
      frame = decorrelateInputWithValueGenerator(rel, frame);
    }

    // Project projects the original expressions
    final Map<Integer, Integer> mapOldToNewOutputs = new HashMap<>();
    int newPos;
    for (newPos = 0; newPos < oldProjects.size(); newPos++) {
      projects.add(
              newPos,
              Pair.of(
                      decorrelateExpr(currentRel, map, cm, oldProjects.get(newPos)),
                      relOutput.get(newPos).getName()));
      mapOldToNewOutputs.put(newPos, newPos);
    }

    // Project any correlated variables the input wants to pass along.
    final SortedMap<CorDef, Integer> corDefOutputs = new TreeMap<>();
    for (Map.Entry<CorDef, Integer> entry : frame.corDefOutputs.entrySet()) {
      projects.add(
              RexInputRef.of2(entry.getValue(),
                      frame.r.getRowType().getFieldList()));
      corDefOutputs.put(entry.getKey(), newPos);
      newPos++;
    }

    RelNode newProject = relBuilder.push(frame.r)
            .projectNamed(Pair.left(projects), Pair.right(projects), true)
            .build();

    newProject = RelOptUtil.copyRelHints(rel, newProject);

    return register(rel, newProject, mapOldToNewOutputs, corDefOutputs);
  }

  /**
   * Create RelNode tree that produces a list of correlated variables.
   *
   * @param correlations         correlated variables to generate
   * @param valueGenFieldOffset  offset in the output that generated columns
   *                             will start
   * @param corDefOutputs        output positions for the correlated variables
   *                             generated
   * @return RelNode the root of the resultant RelNode tree
   */
  private RelNode createValueGenerator(
          Iterable<CorRef> correlations,
          int valueGenFieldOffset,
          SortedMap<CorDef, Integer> corDefOutputs) {
    final Map<RelNode, List<Integer>> mapNewInputToOutputs = new HashMap<>();

    final Map<RelNode, Integer> mapNewInputToNewOffset = new HashMap<>();

    // Input provides the definition of a correlated variable.
    // Add to map all the referenced positions (relative to each input rel).
    for (CorRef corVar : correlations) {
      final int oldCorVarOffset = corVar.field;

      final RelNode oldInput = getCorRel(corVar);
      assert oldInput != null;
      final Frame frame = getFrame(oldInput, true);
      assert frame != null;
      final RelNode newInput = frame.r;

      final List<Integer> newLocalOutputs;
      if (!mapNewInputToOutputs.containsKey(newInput)) {
        newLocalOutputs = new ArrayList<>();
      } else {
        newLocalOutputs = mapNewInputToOutputs.get(newInput);
      }

      final int newCorVarOffset = frame.oldToNewOutputs.get(oldCorVarOffset);

      // Add all unique positions referenced.
      if (!newLocalOutputs.contains(newCorVarOffset)) {
        newLocalOutputs.add(newCorVarOffset);
      }
      mapNewInputToOutputs.put(newInput, newLocalOutputs);
    }

    int offset = 0;

    // Project only the correlated fields out of each input
    // and join the project together.
    // To make sure the plan does not change in terms of join order,
    // join these rels based on their occurrence in corVar list which
    // is sorted.
    final Set<RelNode> joinedInputs = new HashSet<>();

    RelNode r = null;
    for (CorRef corVar : correlations) {
      final RelNode oldInput = getCorRel(corVar);
      assert oldInput != null;
      final RelNode newInput = getFrame(oldInput, true).r;
      assert newInput != null;

      if (!joinedInputs.contains(newInput)) {
        final List<Integer> positions = mapNewInputToOutputs.get(newInput);
        final List<String> fieldNames = newInput.getRowType().getFieldNames();

        RelNode distinct = relBuilder.push(newInput)
                .project(relBuilder.fields(positions))
                .distinct()
                .build();
        RelOptCluster cluster = distinct.getCluster();

        joinedInputs.add(newInput);
        mapNewInputToNewOffset.put(newInput, offset);
        offset += distinct.getRowType().getFieldCount();

        if (r == null) {
          r = distinct;
        } else {
          r = relBuilder.push(r).push(distinct)
                  .join(JoinRelType.INNER, cluster.getRexBuilder().makeLiteral(true)).build();
        }
      }
    }

    // Translate the positions of correlated variables to be relative to
    // the join output, leaving room for valueGenFieldOffset because
    // valueGenerators are joined with the original left input of the rel
    // referencing correlated variables.
    for (CorRef corRef : correlations) {
      // The first input of a Correlate is always the rel defining
      // the correlated variables.
      final RelNode oldInput = getCorRel(corRef);
      assert oldInput != null;
      final Frame frame = getFrame(oldInput, true);
      final RelNode newInput = frame.r;
      assert newInput != null;

      final List<Integer> newLocalOutputs = mapNewInputToOutputs.get(newInput);

      final int newLocalOutput = frame.oldToNewOutputs.get(corRef.field);

      // newOutput is the index of the corVar in the referenced
      // position list plus the offset of referenced position list of
      // each newInput.
      final int newOutput =
              newLocalOutputs.indexOf(newLocalOutput)
                      + mapNewInputToNewOffset.get(newInput)
                      + valueGenFieldOffset;

      corDefOutputs.put(corRef.def(), newOutput);
    }

    return r;
  }

  private Frame getFrame(RelNode r, boolean safe) {
    final Frame frame = map.get(r);
    if (frame == null && safe) {
      return new Frame(r, r, ImmutableSortedMap.of(),
              identityMap(r.getRowType().getFieldCount()));
    }
    return frame;
  }

  private RelNode getCorRel(CorRef corVar) {
    final RelNode r = cm.mapCorToCorRel.get(corVar.corr);
    return r.getInput(0);
  }

  /** Adds a value generator to satisfy the correlating variables used by
   * a relational expression, if those variables are not already provided by
   * its input. */
  private Frame maybeAddValueGenerator(RelNode rel, Frame frame) {
    final CorelMap cm1 = new CorelMapBuilder().build(frame.r, rel);
    if (!cm1.mapRefRelToCorRef.containsKey(rel)) {
      return frame;
    }
    final Collection<CorRef> needs = cm1.mapRefRelToCorRef.get(rel);
    final ImmutableSortedSet<CorDef> haves = frame.corDefOutputs.keySet();
    if (hasAll(needs, haves)) {
      return frame;
    }
    return decorrelateInputWithValueGenerator(rel, frame);
  }

  /** Returns whether all of a collection of {@link CorRef}s are satisfied
   * by at least one of a collection of {@link CorDef}s. */
  private boolean hasAll(Collection<CorRef> corRefs,
                         Collection<CorDef> corDefs) {
    for (CorRef corRef : corRefs) {
      if (!has(corDefs, corRef)) {
        return false;
      }
    }
    return true;
  }

  /** Returns whether a {@link CorrelationId} is satisfied by at least one of a
   * collection of {@link CorDef}s. */
  private boolean has(Collection<CorDef> corDefs, CorRef corr) {
    for (CorDef corDef : corDefs) {
      if (corDef.corr.equals(corr.corr) && corDef.field == corr.field) {
        return true;
      }
    }
    return false;
  }

  private Frame decorrelateInputWithValueGenerator(RelNode rel, Frame frame) {
    // currently only handles one input
    assert rel.getInputs().size() == 1;
    RelNode oldInput = frame.r;

    final SortedMap<CorDef, Integer> corDefOutputs =
            new TreeMap<>(frame.corDefOutputs);

    final Collection<CorRef> corVarList = cm.mapRefRelToCorRef.get(rel);

    // Try to populate correlation variables using local fields.
    // This means that we do not need a value generator.
    if (rel instanceof Filter) {
      SortedMap<CorDef, Integer> map = new TreeMap<>();
      List<RexNode> projects = new ArrayList<>();
      for (CorRef correlation : corVarList) {
        final CorDef def = correlation.def();
        if (corDefOutputs.containsKey(def) || map.containsKey(def)) {
          continue;
        }
        try {
          findCorrelationEquivalent(correlation, ((Filter) rel).getCondition());
        } catch (Util.FoundOne e) {
          if (e.getNode() instanceof RexInputRef) {
            map.put(def, ((RexInputRef) e.getNode()).getIndex());
          } else {
            map.put(def,
                    frame.r.getRowType().getFieldCount() + projects.size());
            projects.add((RexNode) e.getNode());
          }
        }
      }
      // If all correlation variables are now satisfied, skip creating a value
      // generator.
      if (map.size() == corVarList.size()) {
        map.putAll(frame.corDefOutputs);
        final RelNode r;
        if (!projects.isEmpty()) {
          relBuilder.push(oldInput)
                  .project(Iterables.concat(relBuilder.fields(), projects));
          r = relBuilder.build();
        } else {
          r = oldInput;
        }
        return register(rel.getInput(0), r,
                frame.oldToNewOutputs, map);
      }
    }

    int leftInputOutputCount = frame.r.getRowType().getFieldCount();

    // can directly add positions into corDefOutputs since join
    // does not change the output ordering from the inputs.
    RelNode valueGen =
            createValueGenerator(corVarList, leftInputOutputCount, corDefOutputs);

    RelNode join = relBuilder.push(frame.r).push(valueGen)
            .join(JoinRelType.INNER, relBuilder.literal(true),
                    ImmutableSet.of()).build();

    // Join or Filter does not change the old input ordering. All
    // input fields from newLeftInput (i.e. the original input to the old
    // Filter) are in the output and in the same position.
    return register(rel.getInput(0), join, frame.oldToNewOutputs,
            corDefOutputs);
  }

  /** Finds a {@link RexInputRef} that is equivalent to a {@link CorRef},
   * and if found, throws a {@link org.apache.calcite.util.Util.FoundOne}. */
  private void findCorrelationEquivalent(CorRef correlation, RexNode e)
          throws Util.FoundOne {
    switch (e.getKind()) {
    case EQUALS:
      final RexCall call = (RexCall) e;
      final List<RexNode> operands = call.getOperands();
      if (references(operands.get(0), correlation)) {
        throw new Util.FoundOne(operands.get(1));
      }
      if (references(operands.get(1), correlation)) {
        throw new Util.FoundOne(operands.get(0));
      }
      break;
    case AND:
      for (RexNode operand : ((RexCall) e).getOperands()) {
        findCorrelationEquivalent(correlation, operand);
      }
    }
  }

  private boolean references(RexNode e, CorRef correlation) {
    switch (e.getKind()) {
    case CAST:
      final RexNode operand = ((RexCall) e).getOperands().get(0);
      if (isWidening(e.getType(), operand.getType())) {
        return references(operand, correlation);
      }
      return false;
    case FIELD_ACCESS:
      final RexFieldAccess f = (RexFieldAccess) e;
      if (f.getField().getIndex() == correlation.field
              && f.getReferenceExpr() instanceof RexCorrelVariable) {
        if (((RexCorrelVariable) f.getReferenceExpr()).id == correlation.corr) {
          return true;
        }
      }
      // fall through
    default:
      return false;
    }
  }

  /** Returns whether one type is just a widening of another.
   *
   * <p>For example:<ul>
   * <li>{@code VARCHAR(10)} is a widening of {@code VARCHAR(5)}.
   * <li>{@code VARCHAR(10)} is a widening of {@code VARCHAR(10) NOT NULL}.
   * </ul>
   */
  private boolean isWidening(RelDataType type, RelDataType type1) {
    return type.getSqlTypeName() == type1.getSqlTypeName()
            && type.getPrecision() >= type1.getPrecision();
  }

  public Frame decorrelateRel(LogicalSnapshot rel) {
    if (RexUtil.containsCorrelation(rel.getPeriod())) {
      return null;
    }
    return decorrelateRel((RelNode) rel);
  }

  public Frame decorrelateRel(LogicalTableFunctionScan rel) {
    if (RexUtil.containsCorrelation(rel.getCall())) {
      return null;
    }
    return decorrelateRel((RelNode) rel);
  }

  public Frame decorrelateRel(LogicalFilter rel) {
    return decorrelateRel((Filter) rel);
  }

  public Frame decorrelateRel(Filter rel) {
    //
    // Rewrite logic:
    //
    // 1. If a Filter references a correlated field in its filter
    // condition, rewrite the Filter to be
    //   Filter
    //     Join(cross product)
    //       originalFilterInput
    //       ValueGenerator(produces distinct sets of correlated variables)
    // and rewrite the correlated fieldAccess in the filter condition to
    // reference the Join output.
    //
    // 2. If Filter does not reference correlated variables, simply
    // rewrite the filter condition using new input.
    //

    final RelNode oldInput = rel.getInput();
    Frame frame = getInvoke(oldInput, rel);
    if (frame == null) {
      // If input has not been rewritten, do not rewrite this rel.
      return null;
    }

    // If this Filter has correlated reference, create value generator
    // and produce the correlated variables in the new output.
    if (false) {
      if (cm.mapRefRelToCorRef.containsKey(rel)) {
        frame = decorrelateInputWithValueGenerator(rel, frame);
      }
    } else {
      frame = maybeAddValueGenerator(rel, frame);
    }

    final CorelMap cm2 = new CorelMapBuilder().build(rel);

    // Replace the filter expression to reference output of the join
    // Map filter to the new filter over join
    relBuilder.push(frame.r)
            .filter(decorrelateExpr(currentRel, map, cm2, rel.getCondition()));

    // Filter does not change the input ordering.
    // Filter rel does not permute the input.
    // All corVars produced by filter will have the same output positions in the
    // input rel.
    return register(rel, relBuilder.build(), frame.oldToNewOutputs,
            frame.corDefOutputs);
  }

  public Frame decorrelateRel(LogicalCorrelate rel) {
    return decorrelateRel((Correlate) rel);
  }

  public Frame decorrelateRel(Correlate rel) {
    //
    // Rewrite logic:
    //
    // The original left input will be joined with the new right input that
    // has generated correlated variables propagated up. For any generated
    // corVars that are not used in the join key, pass them along to be
    // joined later with the Correlates that produce them.
    //

    // the right input to Correlate should produce correlated variables
    final RelNode oldLeft = rel.getInput(0);
    final RelNode oldRight = rel.getInput(1);

    final Frame leftFrame = getInvoke(oldLeft, rel);
    final Frame rightFrame = getInvoke(oldRight, rel);

    if (leftFrame == null || rightFrame == null) {
      // If any input has not been rewritten, do not rewrite this rel.
      return null;
    }

    if (rightFrame.corDefOutputs.isEmpty()) {
      return null;
    }

    assert rel.getRequiredColumns().cardinality()
            <= rightFrame.corDefOutputs.keySet().size();

    // Change correlator rel into a join.
    // Join all the correlated variables produced by this correlator rel
    // with the values generated and propagated from the right input
    final SortedMap<CorDef, Integer> corDefOutputs =
            new TreeMap<>(rightFrame.corDefOutputs);
    final List<RexNode> conditions = new ArrayList<>();
    final List<RelDataTypeField> newLeftOutput =
            leftFrame.r.getRowType().getFieldList();
    int newLeftFieldCount = newLeftOutput.size();

    final List<RelDataTypeField> newRightOutput =
            rightFrame.r.getRowType().getFieldList();

    for (Map.Entry<CorDef, Integer> rightOutput
            : new ArrayList<>(corDefOutputs.entrySet())) {
      final CorDef corDef = rightOutput.getKey();
      if (!corDef.corr.equals(rel.getCorrelationId())) {
        continue;
      }
      final int newLeftPos = leftFrame.oldToNewOutputs.get(corDef.field);
      final int newRightPos = rightOutput.getValue();
      conditions.add(
              relBuilder.call(SqlStdOperatorTable.EQUALS,
                      RexInputRef.of(newLeftPos, newLeftOutput),
                      new RexInputRef(newLeftFieldCount + newRightPos,
                              newRightOutput.get(newRightPos).getType())));

      // remove this corVar from output position mapping
      corDefOutputs.remove(corDef);
    }

    // Update the output position for the corVars: only pass on the cor
    // vars that are not used in the join key.
    for (CorDef corDef : corDefOutputs.keySet()) {
      int newPos = corDefOutputs.get(corDef) + newLeftFieldCount;
      corDefOutputs.put(corDef, newPos);
    }

    // then add any corVar from the left input. Do not need to change
    // output positions.
    corDefOutputs.putAll(leftFrame.corDefOutputs);

    // Create the mapping between the output of the old correlation rel
    // and the new join rel
    final Map<Integer, Integer> mapOldToNewOutputs = new HashMap<>();

    int oldLeftFieldCount = oldLeft.getRowType().getFieldCount();

    int oldRightFieldCount = oldRight.getRowType().getFieldCount();
    //noinspection AssertWithSideEffects
    assert rel.getRowType().getFieldCount()
            == oldLeftFieldCount + oldRightFieldCount;

    // Left input positions are not changed.
    mapOldToNewOutputs.putAll(leftFrame.oldToNewOutputs);

    // Right input positions are shifted by newLeftFieldCount.
    for (int i = 0; i < oldRightFieldCount; i++) {
      mapOldToNewOutputs.put(i + oldLeftFieldCount,
              rightFrame.oldToNewOutputs.get(i) + newLeftFieldCount);
    }

    final RexNode condition =
            RexUtil.composeConjunction(relBuilder.getRexBuilder(), conditions);
    RelNode newJoin = relBuilder.push(leftFrame.r).push(rightFrame.r)
            .join(rel.getJoinType(), condition).build();

    return register(rel, newJoin, mapOldToNewOutputs, corDefOutputs);
  }

  public Frame decorrelateRel(LogicalJoin rel) {
    return decorrelateRel((Join) rel);
  }

  public Frame decorrelateRel(Join rel) {
    // For SEMI/ANTI join decorrelate it's input directly,
    // because the correlate variables can only be propagated from
    // the left side, which is not supported yet.
    if (!rel.getJoinType().projectsRight()) {
      return decorrelateRel((RelNode) rel);
    }
    //
    // Rewrite logic:
    //
    // 1. rewrite join condition.
    // 2. map output positions and produce corVars if any.
    //

    final RelNode oldLeft = rel.getInput(0);
    final RelNode oldRight = rel.getInput(1);

    final Frame leftFrame = getInvoke(oldLeft, rel);
    final Frame rightFrame = getInvoke(oldRight, rel);

    if (leftFrame == null || rightFrame == null) {
      // If any input has not been rewritten, do not rewrite this rel.
      return null;
    }

    RelNode newJoin = relBuilder
            .push(leftFrame.r)
            .push(rightFrame.r)
            .join(rel.getJoinType(),
                    decorrelateExpr(currentRel, map, cm, rel.getCondition()),
                    ImmutableSet.of())
            .hints(rel.getHints())
            .build();

    // Create the mapping between the output of the old correlation rel
    // and the new join rel
    Map<Integer, Integer> mapOldToNewOutputs = new HashMap<>();

    int oldLeftFieldCount = oldLeft.getRowType().getFieldCount();
    int newLeftFieldCount = leftFrame.r.getRowType().getFieldCount();

    int oldRightFieldCount = oldRight.getRowType().getFieldCount();
    //noinspection AssertWithSideEffects
    assert rel.getRowType().getFieldCount()
            == oldLeftFieldCount + oldRightFieldCount;

    // Left input positions are not changed.
    mapOldToNewOutputs.putAll(leftFrame.oldToNewOutputs);

    // Right input positions are shifted by newLeftFieldCount.
    for (int i = 0; i < oldRightFieldCount; i++) {
      mapOldToNewOutputs.put(i + oldLeftFieldCount,
              rightFrame.oldToNewOutputs.get(i) + newLeftFieldCount);
    }

    final SortedMap<CorDef, Integer> corDefOutputs =
            new TreeMap<>(leftFrame.corDefOutputs);

    // Right input positions are shifted by newLeftFieldCount.
    for (Map.Entry<CorDef, Integer> entry
            : rightFrame.corDefOutputs.entrySet()) {
      corDefOutputs.put(entry.getKey(),
              entry.getValue() + newLeftFieldCount);
    }
    return register(rel, newJoin, mapOldToNewOutputs, corDefOutputs);
  }

  private static RexInputRef getNewForOldInputRef(RelNode currentRel,
                                                  Map<RelNode, Frame> map, RexInputRef oldInputRef) {
    assert currentRel != null;

    int oldOrdinal = oldInputRef.getIndex();
    int newOrdinal = 0;

    // determine which input rel oldOrdinal references, and adjust
    // oldOrdinal to be relative to that input rel
    RelNode oldInput = null;

    for (RelNode oldInput0 : currentRel.getInputs()) {
      RelDataType oldInputType = oldInput0.getRowType();
      int n = oldInputType.getFieldCount();
      if (oldOrdinal < n) {
        oldInput = oldInput0;
        break;
      }
      RelNode newInput = map.get(oldInput0).r;
      newOrdinal += newInput.getRowType().getFieldCount();
      oldOrdinal -= n;
    }

    assert oldInput != null;

    final Frame frame = map.get(oldInput);
    assert frame != null;

    // now oldOrdinal is relative to oldInput
    int oldLocalOrdinal = oldOrdinal;

    // figure out the newLocalOrdinal, relative to the newInput.
    int newLocalOrdinal = oldLocalOrdinal;

    if (!frame.oldToNewOutputs.isEmpty()) {
      newLocalOrdinal = frame.oldToNewOutputs.get(oldLocalOrdinal);
    }

    newOrdinal += newLocalOrdinal;

    return new RexInputRef(newOrdinal,
            frame.r.getRowType().getFieldList().get(newLocalOrdinal).getType());
  }

  /**
   * Pulls project above the join from its RHS input. Enforces nullability
   * for join output.
   *
   * @param join          Join
   * @param project       Original project as the right-hand input of the join
   * @param nullIndicatorPos Position of null indicator
   * @return the subtree with the new Project at the root
   */
  private RelNode projectJoinOutputWithNullability(
          Join join,
          Project project,
          int nullIndicatorPos) {
    final RelDataTypeFactory typeFactory = join.getCluster().getTypeFactory();
    final RelNode left = join.getLeft();
    final JoinRelType joinType = join.getJoinType();

    RexInputRef nullIndicator =
            new RexInputRef(
                    nullIndicatorPos,
                    typeFactory.createTypeWithNullability(
                            join.getRowType().getFieldList().get(nullIndicatorPos)
                                    .getType(),
                            true));

    // now create the new project
    List<Pair<RexNode, String>> newProjExprs = new ArrayList<>();

    // project everything from the LHS and then those from the original
    // projRel
    List<RelDataTypeField> leftInputFields =
            left.getRowType().getFieldList();

    for (int i = 0; i < leftInputFields.size(); i++) {
      newProjExprs.add(RexInputRef.of2(i, leftInputFields));
    }

    // Marked where the projected expr is coming from so that the types will
    // become nullable for the original projections which are now coming out
    // of the nullable side of the OJ.
    boolean projectPulledAboveLeftCorrelator =
            joinType.generatesNullsOnRight();

    for (Pair<RexNode, String> pair : project.getNamedProjects()) {
      RexNode newProjExpr =
              removeCorrelationExpr(
                      pair.left,
                      projectPulledAboveLeftCorrelator,
                      nullIndicator);

      newProjExprs.add(Pair.of(newProjExpr, pair.right));
    }

    return relBuilder.push(join)
            .projectNamed(Pair.left(newProjExprs), Pair.right(newProjExprs), true)
            .build();
  }

  /**
   * Pulls a {@link Project} above a {@link Correlate} from its RHS input.
   * Enforces nullability for join output.
   *
   * @param correlate  Correlate
   * @param project the original project as the RHS input of the join
   * @param isCount Positions which are calls to the <code>COUNT</code>
   *                aggregation function
   * @return the subtree with the new Project at the root
   */
  private RelNode aggregateCorrelatorOutput(
          Correlate correlate,
          Project project,
          Set<Integer> isCount) {
    final RelNode left = correlate.getLeft();
    final JoinRelType joinType = correlate.getJoinType();

    // now create the new project
    final List<Pair<RexNode, String>> newProjects = new ArrayList<>();

    // Project everything from the LHS and then those from the original
    // project
    final List<RelDataTypeField> leftInputFields =
            left.getRowType().getFieldList();

    for (int i = 0; i < leftInputFields.size(); i++) {
      newProjects.add(RexInputRef.of2(i, leftInputFields));
    }

    // Marked where the projected expr is coming from so that the types will
    // become nullable for the original projections which are now coming out
    // of the nullable side of the OJ.
    boolean projectPulledAboveLeftCorrelator =
            joinType.generatesNullsOnRight();

    for (Pair<RexNode, String> pair : project.getNamedProjects()) {
      RexNode newProjExpr =
              removeCorrelationExpr(
                      pair.left,
                      projectPulledAboveLeftCorrelator,
                      isCount);
      newProjects.add(Pair.of(newProjExpr, pair.right));
    }

    return relBuilder.push(correlate)
            .projectNamed(Pair.left(newProjects), Pair.right(newProjects), true)
            .build();
  }

  /**
   * Checks whether the correlations in projRel and filter are related to
   * the correlated variables provided by corRel.
   *
   * @param correlate    Correlate
   * @param project   The original Project as the RHS input of the join
   * @param filter    Filter
   * @param correlatedJoinKeys Correlated join keys
   * @return true if filter and proj only references corVar provided by corRel
   */
  private boolean checkCorVars(
          Correlate correlate,
          Project project,
          Filter filter,
          List<RexFieldAccess> correlatedJoinKeys) {
    if (filter != null) {
      assert correlatedJoinKeys != null;

      // check that all correlated refs in the filter condition are
      // used in the join(as field access).
      Set<CorRef> corVarInFilter =
              Sets.newHashSet(cm.mapRefRelToCorRef.get(filter));

      for (RexFieldAccess correlatedJoinKey : correlatedJoinKeys) {
        corVarInFilter.remove(cm.mapFieldAccessToCorRef.get(correlatedJoinKey));
      }

      if (!corVarInFilter.isEmpty()) {
        return false;
      }

      // Check that the correlated variables referenced in these
      // comparisons do come from the Correlate.
      corVarInFilter.addAll(cm.mapRefRelToCorRef.get(filter));

      for (CorRef corVar : corVarInFilter) {
        if (cm.mapCorToCorRel.get(corVar.corr) != correlate) {
          return false;
        }
      }
    }

    // if project has any correlated reference, make sure they are also
    // provided by the current correlate. They will be projected out of the LHS
    // of the correlate.
    if ((project != null) && cm.mapRefRelToCorRef.containsKey(project)) {
      for (CorRef corVar : cm.mapRefRelToCorRef.get(project)) {
        if (cm.mapCorToCorRel.get(corVar.corr) != correlate) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Removes correlated variables from the tree at root corRel.
   *
   * @param correlate Correlate
   */
  private void removeCorVarFromTree(Correlate correlate) {
    if (cm.mapCorToCorRel.get(correlate.getCorrelationId()) == correlate) {
      cm.mapCorToCorRel.remove(correlate.getCorrelationId());
    }
  }

  /**
   * Projects all {@code input} output fields plus the additional expressions.
   *
   * @param input        Input relational expression
   * @param additionalExprs Additional expressions and names
   * @return the new Project
   */
  private RelNode createProjectWithAdditionalExprs(
          RelNode input,
          List<Pair<RexNode, String>> additionalExprs) {
    final List<RelDataTypeField> fieldList =
            input.getRowType().getFieldList();
    List<Pair<RexNode, String>> projects = new ArrayList<>();
    Ord.forEach(fieldList, (field, i) ->
            projects.add(
                    Pair.of(relBuilder.getRexBuilder().makeInputRef(field.getType(), i),
                            field.getName())));
    projects.addAll(additionalExprs);
    return relBuilder.push(input)
            .projectNamed(Pair.left(projects), Pair.right(projects), true)
            .build();
  }

  /* Returns an immutable map with the identity [0: 0, .., count-1: count-1]. */
  static Map<Integer, Integer> identityMap(int count) {
    ImmutableMap.Builder<Integer, Integer> builder = ImmutableMap.builder();
    for (int i = 0; i < count; i++) {
      builder.put(i, i);
    }
    return builder.build();
  }

  /** Registers a relational expression and the relational expression it became
   * after decorrelation. */
  Frame register(RelNode rel, RelNode newRel,
                 Map<Integer, Integer> oldToNewOutputs,
                 SortedMap<CorDef, Integer> corDefOutputs) {
    final Frame frame = new Frame(rel, newRel, corDefOutputs, oldToNewOutputs);
    map.put(rel, frame);
    return frame;
  }

  static boolean allLessThan(Collection<Integer> integers, int limit,
                             Litmus ret) {
    for (int value : integers) {
      if (value >= limit) {
        return ret.fail("out of range; value: {}, limit: {}", value, limit);
      }
    }
    return ret.succeed();
  }

  private static RelNode stripHep(RelNode rel) {
    if (rel instanceof HepRelVertex) {
      HepRelVertex hepRelVertex = (HepRelVertex) rel;
      rel = hepRelVertex.getCurrentRel();
    }
    return rel;
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Shuttle that decorrelates. */
  private static class DecorrelateRexShuttle extends RexShuttle {
    private final RelNode currentRel;
    private final Map<RelNode, Frame> map;
    private final CorelMap cm;

    private DecorrelateRexShuttle(RelNode currentRel,
                                  Map<RelNode, Frame> map, CorelMap cm) {
      this.currentRel = Objects.requireNonNull(currentRel);
      this.map = Objects.requireNonNull(map);
      this.cm = Objects.requireNonNull(cm);
    }

    @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      int newInputOutputOffset = 0;
      for (RelNode input : currentRel.getInputs()) {
        final Frame frame = map.get(input);

        if (frame != null) {
          // try to find in this input rel the position of corVar
          final CorRef corRef = cm.mapFieldAccessToCorRef.get(fieldAccess);

          if (corRef != null) {
            Integer newInputPos = frame.corDefOutputs.get(corRef.def());
            if (newInputPos != null) {
              // This input does produce the corVar referenced.
              return new RexInputRef(newInputPos + newInputOutputOffset,
                      frame.r.getRowType().getFieldList().get(newInputPos)
                              .getType());
            }
          }

          // this input does not produce the corVar needed
          newInputOutputOffset += frame.r.getRowType().getFieldCount();
        } else {
          // this input is not rewritten
          newInputOutputOffset += input.getRowType().getFieldCount();
        }
      }
      return fieldAccess;
    }

    @Override public RexNode visitInputRef(RexInputRef inputRef) {
      final RexInputRef ref = getNewForOldInputRef(currentRel, map, inputRef);
      if (ref.getIndex() == inputRef.getIndex()
              && ref.getType() == inputRef.getType()) {
        return inputRef; // re-use old object, to prevent needless expr cloning
      }
      return ref;
    }
  }

  /** Shuttle that removes correlations. */
  private class RemoveCorrelationRexShuttle extends RexShuttle {
    final RexBuilder rexBuilder;
    final RelDataTypeFactory typeFactory;
    final boolean projectPulledAboveLeftCorrelator;
    final RexInputRef nullIndicator;
    final ImmutableSet<Integer> isCount;

    RemoveCorrelationRexShuttle(
            RexBuilder rexBuilder,
            boolean projectPulledAboveLeftCorrelator,
            RexInputRef nullIndicator,
            Set<Integer> isCount) {
      this.projectPulledAboveLeftCorrelator =
              projectPulledAboveLeftCorrelator;
      this.nullIndicator = nullIndicator; // may be null
      this.isCount = ImmutableSet.copyOf(isCount);
      this.rexBuilder = rexBuilder;
      this.typeFactory = rexBuilder.getTypeFactory();
    }

    private RexNode createCaseExpression(
            RexInputRef nullInputRef,
            RexLiteral lit,
            RexNode rexNode) {
      RexNode[] caseOperands = new RexNode[3];

      // Construct a CASE expression to handle the null indicator.
      //
      // This also covers the case where a left correlated sub-query
      // projects fields from outer relation. Since LOJ cannot produce
      // nulls on the LHS, the projection now need to make a nullable LHS
      // reference using a nullability indicator. If this this indicator
      // is null, it means the sub-query does not produce any value. As a
      // result, any RHS ref by this sub-query needs to produce null value.

      // WHEN indicator IS NULL
      caseOperands[0] =
              rexBuilder.makeCall(
                      SqlStdOperatorTable.IS_NULL,
                      new RexInputRef(
                              nullInputRef.getIndex(),
                              typeFactory.createTypeWithNullability(
                                      nullInputRef.getType(),
                                      true)));

      // THEN CAST(NULL AS newInputTypeNullable)
      caseOperands[1] =
              lit == null
                      ? rexBuilder.makeNullLiteral(rexNode.getType())
                      : rexBuilder.makeCast(rexNode.getType(), lit);

      // ELSE cast (newInput AS newInputTypeNullable) END
      caseOperands[2] =
              rexBuilder.makeCast(
                      typeFactory.createTypeWithNullability(
                              rexNode.getType(),
                              true),
                      rexNode);

      return rexBuilder.makeCall(
              SqlStdOperatorTable.CASE,
              caseOperands);
    }

    @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      if (cm.mapFieldAccessToCorRef.containsKey(fieldAccess)) {
        // if it is a corVar, change it to be input ref.
        CorRef corVar = cm.mapFieldAccessToCorRef.get(fieldAccess);

        // corVar offset should point to the leftInput of currentRel,
        // which is the Correlate.
        RexNode newRexNode =
                new RexInputRef(corVar.field, fieldAccess.getType());

        if (projectPulledAboveLeftCorrelator
                && (nullIndicator != null)) {
          // need to enforce nullability by applying an additional
          // cast operator over the transformed expression.
          newRexNode =
                  createCaseExpression(nullIndicator, null, newRexNode);
        }
        return newRexNode;
      }
      return fieldAccess;
    }

    @Override public RexNode visitInputRef(RexInputRef inputRef) {
      if (currentRel instanceof Correlate) {
        // if this rel references corVar
        // and now it needs to be rewritten
        // it must have been pulled above the Correlate
        // replace the input ref to account for the LHS of the
        // Correlate
        final int leftInputFieldCount =
                ((Correlate) currentRel).getLeft().getRowType()
                        .getFieldCount();
        RelDataType newType = inputRef.getType();

        if (projectPulledAboveLeftCorrelator) {
          newType =
                  typeFactory.createTypeWithNullability(newType, true);
        }

        int pos = inputRef.getIndex();
        RexInputRef newInputRef =
                new RexInputRef(leftInputFieldCount + pos, newType);

        if ((isCount != null) && isCount.contains(pos)) {
          return createCaseExpression(
                  newInputRef,
                  rexBuilder.makeExactLiteral(BigDecimal.ZERO),
                  newInputRef);
        } else {
          return newInputRef;
        }
      }
      return inputRef;
    }

    @Override public RexNode visitLiteral(RexLiteral literal) {
      // Use nullIndicator to decide whether to project null.
      // Do nothing if the literal is null.
      if (!RexUtil.isNull(literal)
              && projectPulledAboveLeftCorrelator
              && (nullIndicator != null)) {
        return createCaseExpression(nullIndicator, null, literal);
      }
      return literal;
    }

    @Override public RexNode visitCall(final RexCall call) {
      RexNode newCall;

      boolean[] update = {false};
      List<RexNode> clonedOperands = visitList(call.operands, update);
      if (update[0]) {
        SqlOperator operator = call.getOperator();

        boolean isSpecialCast = false;
        if (operator instanceof SqlFunction) {
          SqlFunction function = (SqlFunction) operator;
          if (function.getKind() == SqlKind.CAST) {
            if (call.operands.size() < 2) {
              isSpecialCast = true;
            }
          }
        }

        final RelDataType newType;
        if (!isSpecialCast) {
          // TODO: ideally this only needs to be called if the result
          // type will also change. However, since that requires
          // support from type inference rules to tell whether a rule
          // decides return type based on input types, for now all
          // operators will be recreated with new type if any operand
          // changed, unless the operator has "built-in" type.
          newType = rexBuilder.deriveReturnType(operator, clonedOperands);
        } else {
          // Use the current return type when creating a new call, for
          // operators with return type built into the operator
          // definition, and with no type inference rules, such as
          // cast function with less than 2 operands.

          // TODO: Comments in RexShuttle.visitCall() mention other
          // types in this category. Need to resolve those together
          // and preferably in the base class RexShuttle.
          newType = call.getType();
        }
        newCall =
                rexBuilder.makeCall(
                        newType,
                        operator,
                        clonedOperands);
      } else {
        newCall = call;
      }

      if (projectPulledAboveLeftCorrelator && (nullIndicator != null)) {
        return createCaseExpression(nullIndicator, null, newCall);
      }
      return newCall;
    }
  }

  /**
   * Rule to remove single_value rel. For cases like
   *
   * <blockquote>AggRel single_value proj/filter/agg/ join on unique LHS key
   * AggRel single group</blockquote>
   */
  public static final class RemoveSingleAggregateRule
          extends RelRule<RemoveSingleAggregateRule.Config> {
    static Config config(RelBuilderFactory f) {
      return Config.EMPTY.withRelBuilderFactory(f)
              .withOperandSupplier(b0 ->
                      b0.operand(Aggregate.class).oneInput(b1 ->
                              b1.operand(Project.class).oneInput(b2 ->
                                      b2.operand(Aggregate.class).anyInputs())))
              .as(Config.class);
    }

    /** Creates a RemoveSingleAggregateRule. */
    protected RemoveSingleAggregateRule(Config config) {
      super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      Aggregate singleAggregate = call.rel(0);
      Project project = call.rel(1);
      Aggregate aggregate = call.rel(2);

      // check singleAggRel is single_value agg
      if ((!singleAggregate.getGroupSet().isEmpty())
              || (singleAggregate.getAggCallList().size() != 1)
              || !(singleAggregate.getAggCallList().get(0).getAggregation()
              instanceof SqlSingleValueAggFunction)) {
        return;
      }

      // check projRel only projects one expression
      // check this project only projects one expression, i.e. scalar
      // sub-queries.
      List<RexNode> projExprs = project.getProjects();
      if (projExprs.size() != 1) {
        return;
      }

      // check the input to project is an aggregate on the entire input
      if (!aggregate.getGroupSet().isEmpty()) {
        return;
      }

      // singleAggRel produces a nullable type, so create the new
      // projection that casts proj expr to a nullable type.
      final RelBuilder relBuilder = call.builder();
      final RelDataType type =
              relBuilder.getTypeFactory()
                      .createTypeWithNullability(projExprs.get(0).getType(), true);
      final RexNode cast =
              relBuilder.getRexBuilder().makeCast(type, projExprs.get(0));
      relBuilder.push(aggregate)
              .project(cast);
      call.transformTo(relBuilder.build());
    }

    /** Rule configuration. */
    public interface Config extends RelRule.Config {
      @Override default RemoveSingleAggregateRule toRule() {
        return new RemoveSingleAggregateRule(this);
      }
    }
  }

  /** Planner rule that removes correlations for scalar projects. */
  public static final class RemoveCorrelationForScalarProjectRule
          extends RelRule<RemoveCorrelationForScalarProjectRule.Config> {
    private final RelDecorrelator d;

    static Config config(RelDecorrelator decorrelator,
                         RelBuilderFactory relBuilderFactory) {
      return Config.EMPTY.withRelBuilderFactory(relBuilderFactory)
              .withOperandSupplier(b0 ->
                      b0.operand(Correlate.class).inputs(
                              b1 -> b1.operand(RelNode.class).anyInputs(),
                              b2 -> b2.operand(Aggregate.class).oneInput(b3 ->
                                      b3.operand(Project.class).oneInput(b4 ->
                                              b4.operand(RelNode.class).anyInputs()))))
              .as(Config.class)
              .withDecorrelator(decorrelator)
              .as(Config.class);
    }

    /** Creates a RemoveCorrelationForScalarProjectRule. */
    protected RemoveCorrelationForScalarProjectRule(Config config) {
      super(config);
      this.d = Objects.requireNonNull(config.decorrelator());
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Correlate correlate = call.rel(0);
      final RelNode left = call.rel(1);
      final Aggregate aggregate = call.rel(2);
      final Project project = call.rel(3);
      RelNode right = call.rel(4);
      final RelOptCluster cluster = correlate.getCluster();

      d.setCurrent(call.getPlanner().getRoot(), correlate);

      // Check for this pattern.
      // The pattern matching could be simplified if rules can be applied
      // during decorrelation.
      //
      // Correlate(left correlation, condition = true)
      //   leftInput
      //   Aggregate (groupby (0) single_value())
      //     Project-A (may reference corVar)
      //       rightInput
      final JoinRelType joinType = correlate.getJoinType();

      // corRel.getCondition was here, however Correlate was updated so it
      // never includes a join condition. The code was not modified for brevity.
      RexNode joinCond = d.relBuilder.literal(true);
      if ((joinType != JoinRelType.LEFT)
              || (joinCond != d.relBuilder.literal(true))) {
        return;
      }

      // check that the agg is of the following type:
      // doing a single_value() on the entire input
      if ((!aggregate.getGroupSet().isEmpty())
              || (aggregate.getAggCallList().size() != 1)
              || !(aggregate.getAggCallList().get(0).getAggregation()
              instanceof SqlSingleValueAggFunction)) {
        return;
      }

      // check this project only projects one expression, i.e. scalar
      // sub-queries.
      if (project.getProjects().size() != 1) {
        return;
      }

      int nullIndicatorPos;

      if ((right instanceof Filter)
              && d.cm.mapRefRelToCorRef.containsKey(right)) {
        // rightInput has this shape:
        //
        //       Filter (references corVar)
        //         filterInput

        // If rightInput is a filter and contains correlated
        // reference, make sure the correlated keys in the filter
        // condition forms a unique key of the RHS.

        Filter filter = (Filter) right;
        right = filter.getInput();

        assert right instanceof HepRelVertex;
        right = ((HepRelVertex) right).getCurrentRel();

        // check filter input contains no correlation
        if (RelOptUtil.getVariablesUsed(right).size() > 0) {
          return;
        }

        // extract the correlation out of the filter

        // First breaking up the filter conditions into equality
        // comparisons between rightJoinKeys (from the original
        // filterInput) and correlatedJoinKeys. correlatedJoinKeys
        // can be expressions, while rightJoinKeys need to be input
        // refs. These comparisons are AND'ed together.
        List<RexNode> tmpRightJoinKeys = new ArrayList<>();
        List<RexNode> correlatedJoinKeys = new ArrayList<>();
        RelOptUtil.splitCorrelatedFilterCondition(
                filter,
                tmpRightJoinKeys,
                correlatedJoinKeys,
                false);

        // check that the columns referenced in these comparisons form
        // an unique key of the filterInput
        final List<RexInputRef> rightJoinKeys = new ArrayList<>();
        for (RexNode key : tmpRightJoinKeys) {
          assert key instanceof RexInputRef;
          rightJoinKeys.add((RexInputRef) key);
        }

        // check that the columns referenced in rightJoinKeys form an
        // unique key of the filterInput
        if (rightJoinKeys.isEmpty()) {
          return;
        }

        // The join filters out the nulls.  So, it's ok if there are
        // nulls in the join keys.
        final RelMetadataQuery mq = call.getMetadataQuery();
        if (!RelMdUtil.areColumnsDefinitelyUniqueWhenNullsFiltered(mq, right,
                rightJoinKeys)) {
          SQL2REL_LOGGER.debug("{} are not unique keys for {}",
                  rightJoinKeys, right);
          return;
        }

        RexUtil.FieldAccessFinder visitor =
                new RexUtil.FieldAccessFinder();
        RexUtil.apply(visitor, correlatedJoinKeys, null);
        List<RexFieldAccess> correlatedKeyList =
                visitor.getFieldAccessList();

        if (!d.checkCorVars(correlate, project, filter, correlatedKeyList)) {
          return;
        }

        // Change the plan to this structure.
        // Note that the Aggregate is removed.
        //
        // Project-A' (replace corVar to input ref from the Join)
        //   Join (replace corVar to input ref from leftInput)
        //     leftInput
        //     rightInput (previously filterInput)

        // Change the filter condition into a join condition
        joinCond =
                d.removeCorrelationExpr(filter.getCondition(), false);

        nullIndicatorPos =
                left.getRowType().getFieldCount()
                        + rightJoinKeys.get(0).getIndex();
      } else if (d.cm.mapRefRelToCorRef.containsKey(project)) {
        // check filter input contains no correlation
        if (RelOptUtil.getVariablesUsed(right).size() > 0) {
          return;
        }

        if (!d.checkCorVars(correlate, project, null, null)) {
          return;
        }

        // Change the plan to this structure.
        //
        // Project-A' (replace corVar to input ref from Join)
        //   Join (left, condition = true)
        //     leftInput
        //     Aggregate(groupby(0), single_value(0), s_v(1)....)
        //       Project-B (everything from input plus literal true)
        //         projectInput

        // make the new Project to provide a null indicator
        right =
                d.createProjectWithAdditionalExprs(right,
                        ImmutableList.of(
                                Pair.of(d.relBuilder.literal(true), "nullIndicator")));

        // make the new aggRel
        right =
                RelOptUtil.createSingleValueAggRel(cluster, right);

        // The last field:
        //     single_value(true)
        // is the nullIndicator
        nullIndicatorPos =
                left.getRowType().getFieldCount()
                        + right.getRowType().getFieldCount() - 1;
      } else {
        return;
      }

      // make the new join rel
      final Join join = (Join) d.relBuilder.push(left).push(right)
              .join(joinType, joinCond).build();

      RelNode newProject =
              d.projectJoinOutputWithNullability(join, project, nullIndicatorPos);

      call.transformTo(newProject);

      d.removeCorVarFromTree(correlate);
    }

    /** Rule configuration.
     *
     * <p>Extends {@link RelDecorrelator.Config} because rule needs a
     * decorrelator instance. */
    public interface Config extends RelDecorrelator.Config {
      @Override default RemoveCorrelationForScalarProjectRule toRule() {
        return new RemoveCorrelationForScalarProjectRule(this);
      }
    }
  }

  /** Planner rule that removes correlations for scalar aggregates. */
  public static final class RemoveCorrelationForScalarAggregateRule
          extends RelRule<RemoveCorrelationForScalarAggregateRule.Config> {
    private final RelDecorrelator d;

    static Config config(RelDecorrelator d,
                         RelBuilderFactory relBuilderFactory) {
      return Config.EMPTY
              .withRelBuilderFactory(relBuilderFactory)
              .withOperandSupplier(b0 ->
                      b0.operand(Correlate.class).inputs(
                              b1 -> b1.operand(RelNode.class).anyInputs(),
                              b2 -> b2.operand(Project.class).oneInput(b3 ->
                                      b3.operand(Aggregate.class)
                                              .predicate(Aggregate::isSimple).oneInput(b4 ->
                                              b4.operand(Project.class).oneInput(b5 ->
                                                      b5.operand(RelNode.class).anyInputs())))))
              .as(Config.class)
              .withDecorrelator(d)
              .as(Config.class);
    }

    /** Creates a RemoveCorrelationForScalarAggregateRule. */
    protected RemoveCorrelationForScalarAggregateRule(Config config) {
      super(config);
      d = Objects.requireNonNull(config.decorrelator());
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Correlate correlate = call.rel(0);
      final RelNode left = call.rel(1);
      final Project aggOutputProject = call.rel(2);
      final Aggregate aggregate = call.rel(3);
      final Project aggInputProject = call.rel(4);
      RelNode right = call.rel(5);
      final RelBuilder builder = call.builder();
      final RexBuilder rexBuilder = builder.getRexBuilder();
      final RelOptCluster cluster = correlate.getCluster();

      d.setCurrent(call.getPlanner().getRoot(), correlate);

      // check for this pattern
      // The pattern matching could be simplified if rules can be applied
      // during decorrelation,
      //
      // CorrelateRel(left correlation, condition = true)
      //   leftInput
      //   Project-A (a RexNode)
      //     Aggregate (groupby (0), agg0(), agg1()...)
      //       Project-B (references coVar)
      //         rightInput

      // check aggOutputProject projects only one expression
      final List<RexNode> aggOutputProjects = aggOutputProject.getProjects();
      if (aggOutputProjects.size() != 1) {
        return;
      }

      final JoinRelType joinType = correlate.getJoinType();
      // corRel.getCondition was here, however Correlate was updated so it
      // never includes a join condition. The code was not modified for brevity.
      RexNode joinCond = rexBuilder.makeLiteral(true);
      if ((joinType != JoinRelType.LEFT)
              || (joinCond != rexBuilder.makeLiteral(true))) {
        return;
      }

      // check that the agg is on the entire input
      if (!aggregate.getGroupSet().isEmpty()) {
        return;
      }

      final List<RexNode> aggInputProjects = aggInputProject.getProjects();

      final List<AggregateCall> aggCalls = aggregate.getAggCallList();
      final Set<Integer> isCountStar = new HashSet<>();

      // mark if agg produces count(*) which needs to reference the
      // nullIndicator after the transformation.
      int k = -1;
      for (AggregateCall aggCall : aggCalls) {
        ++k;
        if ((aggCall.getAggregation() instanceof SqlCountAggFunction)
                && (aggCall.getArgList().size() == 0)) {
          isCountStar.add(k);
        }
      }

      if ((right instanceof Filter)
              && d.cm.mapRefRelToCorRef.containsKey(right)) {
        // rightInput has this shape:
        //
        //       Filter (references corVar)
        //         filterInput
        Filter filter = (Filter) right;
        right = filter.getInput();

        assert right instanceof HepRelVertex;
        right = ((HepRelVertex) right).getCurrentRel();

        // check filter input contains no correlation
        if (RelOptUtil.getVariablesUsed(right).size() > 0) {
          return;
        }

        // check filter condition type First extract the correlation out
        // of the filter

        // First breaking up the filter conditions into equality
        // comparisons between rightJoinKeys(from the original
        // filterInput) and correlatedJoinKeys. correlatedJoinKeys
        // can only be RexFieldAccess, while rightJoinKeys can be
        // expressions. These comparisons are AND'ed together.
        List<RexNode> rightJoinKeys = new ArrayList<>();
        List<RexNode> tmpCorrelatedJoinKeys = new ArrayList<>();
        RelOptUtil.splitCorrelatedFilterCondition(
                filter,
                rightJoinKeys,
                tmpCorrelatedJoinKeys,
                true);

        // make sure the correlated reference forms a unique key check
        // that the columns referenced in these comparisons form an
        // unique key of the leftInput
        List<RexFieldAccess> correlatedJoinKeys = new ArrayList<>();
        List<RexInputRef> correlatedInputRefJoinKeys = new ArrayList<>();
        for (RexNode joinKey : tmpCorrelatedJoinKeys) {
          assert joinKey instanceof RexFieldAccess;
          correlatedJoinKeys.add((RexFieldAccess) joinKey);
          RexNode correlatedInputRef =
                  d.removeCorrelationExpr(joinKey, false);
          assert correlatedInputRef instanceof RexInputRef;
          correlatedInputRefJoinKeys.add(
                  (RexInputRef) correlatedInputRef);
        }

        // check that the columns referenced in rightJoinKeys form an
        // unique key of the filterInput
        if (correlatedInputRefJoinKeys.isEmpty()) {
          return;
        }

        // The join filters out the nulls.  So, it's ok if there are
        // nulls in the join keys.
        final RelMetadataQuery mq = call.getMetadataQuery();
        if (!RelMdUtil.areColumnsDefinitelyUniqueWhenNullsFiltered(mq, left,
                correlatedInputRefJoinKeys)) {
          SQL2REL_LOGGER.debug("{} are not unique keys for {}",
                  correlatedJoinKeys, left);
          return;
        }

        // check corVar references are valid
        if (!d.checkCorVars(correlate, aggInputProject, filter,
                correlatedJoinKeys)) {
          return;
        }

        // Rewrite the above plan:
        //
        // Correlate(left correlation, condition = true)
        //   leftInput
        //   Project-A (a RexNode)
        //     Aggregate (groupby(0), agg0(),agg1()...)
        //       Project-B (may reference corVar)
        //         Filter (references corVar)
        //           rightInput (no correlated reference)
        //

        // to this plan:
        //
        // Project-A' (all gby keys + rewritten nullable ProjExpr)
        //   Aggregate (groupby(all left input refs)
        //                 agg0(rewritten expression),
        //                 agg1()...)
        //     Project-B' (rewritten original projected exprs)
        //       Join(replace corVar w/ input ref from leftInput)
        //         leftInput
        //         rightInput
        //

        // In the case where agg is count(*) or count($corVar), it is
        // changed to count(nullIndicator).
        // Note:  any non-nullable field from the RHS can be used as
        // the indicator however a "true" field is added to the
        // projection list from the RHS for simplicity to avoid
        // searching for non-null fields.
        //
        // Project-A' (all gby keys + rewritten nullable ProjExpr)
        //   Aggregate (groupby(all left input refs),
        //                 count(nullIndicator), other aggs...)
        //     Project-B' (all left input refs plus
        //                    the rewritten original projected exprs)
        //       Join(replace corVar to input ref from leftInput)
        //         leftInput
        //         Project (everything from rightInput plus
        //                     the nullIndicator "true")
        //           rightInput
        //

        // first change the filter condition into a join condition
        joinCond = d.removeCorrelationExpr(filter.getCondition(), false);
      } else if (d.cm.mapRefRelToCorRef.containsKey(aggInputProject)) {
        // check rightInput contains no correlation
        if (RelOptUtil.getVariablesUsed(right).size() > 0) {
          return;
        }

        // check corVar references are valid
        if (!d.checkCorVars(correlate, aggInputProject, null, null)) {
          return;
        }

        int nFields = left.getRowType().getFieldCount();
        ImmutableBitSet allCols = ImmutableBitSet.range(nFields);

        // leftInput contains unique keys
        // i.e. each row is distinct and can group by on all the left
        // fields
        final RelMetadataQuery mq = call.getMetadataQuery();
        if (!RelMdUtil.areColumnsDefinitelyUnique(mq, left, allCols)) {
          SQL2REL_LOGGER.debug("There are no unique keys for {}", left);
          return;
        }
        //
        // Rewrite the above plan:
        //
        // CorrelateRel(left correlation, condition = true)
        //   leftInput
        //   Project-A (a RexNode)
        //     Aggregate (groupby(0), agg0(), agg1()...)
        //       Project-B (references coVar)
        //         rightInput (no correlated reference)
        //

        // to this plan:
        //
        // Project-A' (all gby keys + rewritten nullable ProjExpr)
        //   Aggregate (groupby(all left input refs)
        //                 agg0(rewritten expression),
        //                 agg1()...)
        //     Project-B' (rewritten original projected exprs)
        //       Join (LOJ cond = true)
        //         leftInput
        //         rightInput
        //

        // In the case where agg is count($corVar), it is changed to
        // count(nullIndicator).
        // Note:  any non-nullable field from the RHS can be used as
        // the indicator however a "true" field is added to the
        // projection list from the RHS for simplicity to avoid
        // searching for non-null fields.
        //
        // Project-A' (all gby keys + rewritten nullable ProjExpr)
        //   Aggregate (groupby(all left input refs),
        //                 count(nullIndicator), other aggs...)
        //     Project-B' (all left input refs plus
        //                    the rewritten original projected exprs)
        //       Join (replace corVar to input ref from leftInput)
        //         leftInput
        //         Project (everything from rightInput plus
        //                     the nullIndicator "true")
        //           rightInput
      } else {
        return;
      }

      RelDataType leftInputFieldType = left.getRowType();
      int leftInputFieldCount = leftInputFieldType.getFieldCount();
      int joinOutputProjExprCount =
              leftInputFieldCount + aggInputProjects.size() + 1;

      right = d.createProjectWithAdditionalExprs(right,
              ImmutableList.of(
                      Pair.of(rexBuilder.makeLiteral(true), "nullIndicator")));

      Join join = (Join) d.relBuilder.push(left).push(right)
              .join(joinType, joinCond).build();

      // To the consumer of joinOutputProjRel, nullIndicator is located
      // at the end
      int nullIndicatorPos = join.getRowType().getFieldCount() - 1;

      RexInputRef nullIndicator =
              new RexInputRef(
                      nullIndicatorPos,
                      cluster.getTypeFactory().createTypeWithNullability(
                              join.getRowType().getFieldList()
                                      .get(nullIndicatorPos).getType(),
                              true));

      // first project all group-by keys plus the transformed agg input
      List<RexNode> joinOutputProjects = new ArrayList<>();

      // LOJ Join preserves LHS types
      for (int i = 0; i < leftInputFieldCount; i++) {
        joinOutputProjects.add(
                rexBuilder.makeInputRef(
                        leftInputFieldType.getFieldList().get(i).getType(), i));
      }

      for (RexNode aggInputProjExpr : aggInputProjects) {
        joinOutputProjects.add(
                d.removeCorrelationExpr(aggInputProjExpr,
                        joinType.generatesNullsOnRight(),
                        nullIndicator));
      }

      joinOutputProjects.add(
              rexBuilder.makeInputRef(join, nullIndicatorPos));

      final RelNode joinOutputProject = builder.push(join)
              .project(joinOutputProjects)
              .build();

      // nullIndicator is now at a different location in the output of
      // the join
      nullIndicatorPos = joinOutputProjExprCount - 1;

      final int groupCount = leftInputFieldCount;

      List<AggregateCall> newAggCalls = new ArrayList<>();
      k = -1;
      for (AggregateCall aggCall : aggCalls) {
        ++k;
        final List<Integer> argList;

        if (isCountStar.contains(k)) {
          // this is a count(*), transform it to count(nullIndicator)
          // the null indicator is located at the end
          argList = Collections.singletonList(nullIndicatorPos);
        } else {
          argList = new ArrayList<>();

          for (int aggArg : aggCall.getArgList()) {
            argList.add(aggArg + groupCount);
          }
        }

        int filterArg = aggCall.filterArg < 0 ? aggCall.filterArg
                : aggCall.filterArg + groupCount;
        newAggCalls.add(
                aggCall.adaptTo(joinOutputProject, argList, filterArg,
                        aggregate.getGroupCount(), groupCount));
      }

      ImmutableBitSet groupSet =
              ImmutableBitSet.range(groupCount);
      builder.push(joinOutputProject)
              .aggregate(builder.groupKey(groupSet), newAggCalls);
      List<RexNode> newAggOutputProjectList = new ArrayList<>();
      for (int i : groupSet) {
        newAggOutputProjectList.add(
                rexBuilder.makeInputRef(builder.peek(), i));
      }

      RexNode newAggOutputProjects =
              d.removeCorrelationExpr(aggOutputProjects.get(0), false);
      newAggOutputProjectList.add(
              rexBuilder.makeCast(
                      cluster.getTypeFactory().createTypeWithNullability(
                              newAggOutputProjects.getType(),
                              true),
                      newAggOutputProjects));

      builder.project(newAggOutputProjectList);
      call.transformTo(builder.build());

      d.removeCorVarFromTree(correlate);
    }

    /** Rule configuration.
     *
     * <p>Extends {@link RelDecorrelator.Config} because rule needs a
     * decorrelator instance. */
    public interface Config extends RelDecorrelator.Config {
      @Override default RemoveCorrelationForScalarAggregateRule toRule() {
        return new RemoveCorrelationForScalarAggregateRule(this);
      }
    }
  }

  // REVIEW jhyde 29-Oct-2007: This rule is non-static, depends on the state
  // of members in RelDecorrelator, and has side-effects in the decorrelator.
  // This breaks the contract of a planner rule, and the rule will not be
  // reusable in other planners.

  // REVIEW jvs 29-Oct-2007:  Shouldn't it also be incorporating
  // the flavor attribute into the description?

  /** Planner rule that adjusts projects when counts are added. */
  public static final class AdjustProjectForCountAggregateRule
          extends RelRule<AdjustProjectForCountAggregateRule.Config> {
    final RelDecorrelator d;

    static Config config(boolean flavor, RelDecorrelator decorrelator,
                         RelBuilderFactory relBuilderFactory) {
      return Config.EMPTY.withRelBuilderFactory(relBuilderFactory)
              .withOperandSupplier(b0 ->
                      b0.operand(Correlate.class).inputs(
                              b1 -> b1.operand(RelNode.class).anyInputs(),
                              b2 -> flavor
                                      ? b2.operand(Project.class).oneInput(b3 ->
                                      b3.operand(Aggregate.class).anyInputs())
                                      : b2.operand(Aggregate.class).anyInputs()))
              .as(Config.class)
              .withFlavor(flavor)
              .withDecorrelator(decorrelator)
              .as(Config.class);
    }

    /** Creates an AdjustProjectForCountAggregateRule. */
    protected AdjustProjectForCountAggregateRule(Config config) {
      super(config);
      this.d = Objects.requireNonNull(config.decorrelator());
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Correlate correlate = call.rel(0);
      final RelNode left = call.rel(1);
      final Project aggOutputProject;
      final Aggregate aggregate;
      if (config.flavor()) {
        aggOutputProject = call.rel(2);
        aggregate = call.rel(3);
      } else {
        aggregate = call.rel(2);

        // Create identity projection
        final List<Pair<RexNode, String>> projects = new ArrayList<>();
        final List<RelDataTypeField> fields =
                aggregate.getRowType().getFieldList();
        for (int i = 0; i < fields.size(); i++) {
          projects.add(RexInputRef.of2(projects.size(), fields));
        }
        final RelBuilder relBuilder = call.builder();
        relBuilder.push(aggregate)
                .projectNamed(Pair.left(projects), Pair.right(projects), true);
        aggOutputProject = (Project) relBuilder.build();
      }
      onMatch2(call, correlate, left, aggOutputProject, aggregate);
    }

    private void onMatch2(
            RelOptRuleCall call,
            Correlate correlate,
            RelNode leftInput,
            Project aggOutputProject,
            Aggregate aggregate) {
      if (d.generatedCorRels.contains(correlate)) {
        // This Correlate was generated by a previous invocation of
        // this rule. No further work to do.
        return;
      }

      d.setCurrent(call.getPlanner().getRoot(), correlate);

      // check for this pattern
      // The pattern matching could be simplified if rules can be applied
      // during decorrelation,
      //
      // CorrelateRel(left correlation, condition = true)
      //   leftInput
      //   Project-A (a RexNode)
      //     Aggregate (groupby (0), agg0(), agg1()...)

      // check aggOutputProj projects only one expression
      List<RexNode> aggOutputProjExprs = aggOutputProject.getProjects();
      if (aggOutputProjExprs.size() != 1) {
        return;
      }

      JoinRelType joinType = correlate.getJoinType();
      // corRel.getCondition was here, however Correlate was updated so it
      // never includes a join condition. The code was not modified for brevity.
      RexNode joinCond = d.relBuilder.literal(true);
      if ((joinType != JoinRelType.LEFT)
              || (joinCond != d.relBuilder.literal(true))) {
        return;
      }

      // check that the agg is on the entire input
      if (!aggregate.getGroupSet().isEmpty()) {
        return;
      }

      List<AggregateCall> aggCalls = aggregate.getAggCallList();
      Set<Integer> isCount = new HashSet<>();

      // remember the count() positions
      int i = -1;
      for (AggregateCall aggCall : aggCalls) {
        ++i;
        if (aggCall.getAggregation() instanceof SqlCountAggFunction) {
          isCount.add(i);
        }
      }

      // now rewrite the plan to
      //
      // Project-A' (all LHS plus transformed original projections,
      //             replacing references to count() with case statement)
      //   Correlate(left correlation, condition = true)
      //     leftInput
      //     Aggregate(groupby (0), agg0(), agg1()...)
      //
      final RexBuilder rexBuilder = d.relBuilder.getRexBuilder();
      List<RexNode> requiredNodes =
              correlate.getRequiredColumns().asList().stream()
                      .map(ord -> rexBuilder.makeInputRef(correlate, ord))
                      .collect(Collectors.toList());
      Correlate newCorrelate = (Correlate) d.relBuilder.push(leftInput)
              .push(aggregate).correlate(correlate.getJoinType(),
                      correlate.getCorrelationId(),
                      requiredNodes).build();


      // remember this rel so we don't fire rule on it again
      // REVIEW jhyde 29-Oct-2007: rules should not save state; rule
      // should recognize patterns where it does or does not need to do
      // work
      d.generatedCorRels.add(newCorrelate);

      // need to update the mapCorToCorRel Update the output position
      // for the corVars: only pass on the corVars that are not used in
      // the join key.
      if (d.cm.mapCorToCorRel.get(correlate.getCorrelationId()) == correlate) {
        d.cm.mapCorToCorRel.put(correlate.getCorrelationId(), newCorrelate);
      }

      RelNode newOutput =
              d.aggregateCorrelatorOutput(newCorrelate, aggOutputProject, isCount);

      call.transformTo(newOutput);
    }

    /** Rule configuration. */
    public interface Config extends RelDecorrelator.Config {
      @Override default AdjustProjectForCountAggregateRule toRule() {
        return new AdjustProjectForCountAggregateRule(this);
      }

      /** Returns the flavor of the rule (true for 4 operands, false for 3
       * operands). */
      @ImmutableBeans.Property
      boolean flavor();

      /** Sets {@link #flavor}. */
      Config withFlavor(boolean flavor);
    }
  }

  /**
   * A unique reference to a correlation field.
   *
   * <p>For instance, if a RelNode references emp.name multiple times, it would
   * result in multiple {@code CorRef} objects that differ just in
   * {@link CorRef#uniqueKey}.
   */
  static class CorRef implements Comparable<CorRef> {
    public final int uniqueKey;
    public final CorrelationId corr;
    public final int field;

    CorRef(CorrelationId corr, int field, int uniqueKey) {
      this.corr = corr;
      this.field = field;
      this.uniqueKey = uniqueKey;
    }

    @Override public String toString() {
      return corr.getName() + '.' + field;
    }

    @Override public int hashCode() {
      return Objects.hash(uniqueKey, corr, field);
    }

    @Override public boolean equals(Object o) {
      return this == o
              || o instanceof CorRef
              && uniqueKey == ((CorRef) o).uniqueKey
              && corr == ((CorRef) o).corr
              && field == ((CorRef) o).field;
    }

    public int compareTo(@Nonnull CorRef o) {
      int c = corr.compareTo(o.corr);
      if (c != 0) {
        return c;
      }
      c = Integer.compare(field, o.field);
      if (c != 0) {
        return c;
      }
      return Integer.compare(uniqueKey, o.uniqueKey);
    }

    public CorDef def() {
      return new CorDef(corr, field);
    }
  }

  /** A correlation and a field. */
  static class CorDef implements Comparable<CorDef> {
    public final CorrelationId corr;
    public final int field;

    CorDef(CorrelationId corr, int field) {
      this.corr = corr;
      this.field = field;
    }

    @Override public String toString() {
      return corr.getName() + '.' + field;
    }

    @Override public int hashCode() {
      return Objects.hash(corr, field);
    }

    @Override public boolean equals(Object o) {
      return this == o
              || o instanceof CorDef
              && corr == ((CorDef) o).corr
              && field == ((CorDef) o).field;
    }

    public int compareTo(@Nonnull CorDef o) {
      int c = corr.compareTo(o.corr);
      if (c != 0) {
        return c;
      }
      return Integer.compare(field, o.field);
    }
  }

  /** A map of the locations of
   * {@link org.apache.calcite.rel.core.Correlate}
   * in a tree of {@link RelNode}s.
   *
   * <p>It is used to drive the decorrelation process.
   * Treat it as immutable; rebuild if you modify the tree.
   *
   * <p>There are three maps:<ol>
   *
   * <li>{@link #mapRefRelToCorRef} maps a {@link RelNode} to the correlated
   * variables it references;
   *
   * <li>{@link #mapCorToCorRel} maps a correlated variable to the
   * {@link Correlate} providing it;
   *
   * <li>{@link #mapFieldAccessToCorRef} maps a rex field access to
   * the corVar it represents. Because typeFlattener does not clone or
   * modify a correlated field access this map does not need to be
   * updated.
   *
   * </ol> */
  protected static class CorelMap {
    private final Multimap<RelNode, CorRef> mapRefRelToCorRef;
    private final SortedMap<CorrelationId, RelNode> mapCorToCorRel;
    private final Map<RexFieldAccess, CorRef> mapFieldAccessToCorRef;

    // TODO: create immutable copies of all maps
    private CorelMap(Multimap<RelNode, CorRef> mapRefRelToCorRef,
                     SortedMap<CorrelationId, RelNode> mapCorToCorRel,
                     Map<RexFieldAccess, CorRef> mapFieldAccessToCorRef) {
      this.mapRefRelToCorRef = mapRefRelToCorRef;
      this.mapCorToCorRel = mapCorToCorRel;
      this.mapFieldAccessToCorRef = ImmutableMap.copyOf(mapFieldAccessToCorRef);
    }

    @Override public String toString() {
      return "mapRefRelToCorRef=" + mapRefRelToCorRef
              + "\nmapCorToCorRel=" + mapCorToCorRel
              + "\nmapFieldAccessToCorRef=" + mapFieldAccessToCorRef
              + "\n";
    }

    @Override public boolean equals(Object obj) {
      return obj == this
              || obj instanceof CorelMap
              && mapRefRelToCorRef.equals(((CorelMap) obj).mapRefRelToCorRef)
              && mapCorToCorRel.equals(((CorelMap) obj).mapCorToCorRel)
              && mapFieldAccessToCorRef.equals(
              ((CorelMap) obj).mapFieldAccessToCorRef);
    }

    @Override public int hashCode() {
      return Objects.hash(mapRefRelToCorRef, mapCorToCorRel,
              mapFieldAccessToCorRef);
    }

    /** Creates a CorelMap with given contents. */
    public static CorelMap of(
            SortedSetMultimap<RelNode, CorRef> mapRefRelToCorVar,
            SortedMap<CorrelationId, RelNode> mapCorToCorRel,
            Map<RexFieldAccess, CorRef> mapFieldAccessToCorVar) {
      return new CorelMap(mapRefRelToCorVar, mapCorToCorRel,
              mapFieldAccessToCorVar);
    }

    public SortedMap<CorrelationId, RelNode> getMapCorToCorRel() {
      return mapCorToCorRel;
    }

    /**
     * Returns whether there are any correlating variables in this statement.
     *
     * @return whether there are any correlating variables
     */
    public boolean hasCorrelation() {
      return !mapCorToCorRel.isEmpty();
    }
  }

  /** Builds a {@link org.apache.calcite.sql2rel.RelDecorrelator.CorelMap}. */
  public static class CorelMapBuilder extends RelHomogeneousShuttle {
    final SortedMap<CorrelationId, RelNode> mapCorToCorRel =
            new TreeMap<>();

    final SortedSetMultimap<RelNode, CorRef> mapRefRelToCorRef =
            MultimapBuilder.SortedSetMultimapBuilder.hashKeys()
                    .treeSetValues()
                    .build();

    final Map<RexFieldAccess, CorRef> mapFieldAccessToCorVar = new HashMap<>();

    final Holder<Integer> offset = Holder.of(0);
    int corrIdGenerator = 0;

    /** Creates a CorelMap by iterating over a {@link RelNode} tree. */
    public CorelMap build(RelNode... rels) {
      for (RelNode rel : rels) {
        stripHep(rel).accept(this);
      }
      return new CorelMap(mapRefRelToCorRef, mapCorToCorRel,
              mapFieldAccessToCorVar);
    }

    @Override public RelNode visit(RelNode other) {
      if (other instanceof Join) {
        Join join = (Join) other;
        try {
          stack.push(join);
          join.getCondition().accept(rexVisitor(join));
        } finally {
          stack.pop();
        }
        return visitJoin(join);
      } else if (other instanceof Correlate) {
        Correlate correlate = (Correlate) other;
        mapCorToCorRel.put(correlate.getCorrelationId(), correlate);
        return visitJoin(correlate);
      } else if (other instanceof Filter) {
        Filter filter = (Filter) other;
        try {
          stack.push(filter);
          filter.getCondition().accept(rexVisitor(filter));
        } finally {
          stack.pop();
        }
      } else if (other instanceof Project) {
        Project project = (Project) other;
        try {
          stack.push(project);
          for (RexNode node : project.getProjects()) {
            node.accept(rexVisitor(project));
          }
        } finally {
          stack.pop();
        }
      }
      return super.visit(other);
    }

    @Override protected RelNode visitChild(RelNode parent, int i,
                                           RelNode input) {
      return super.visitChild(parent, i, stripHep(input));
    }

    private RelNode visitJoin(BiRel join) {
      final int x = offset.get();
      visitChild(join, 0, join.getLeft());
      offset.set(x + join.getLeft().getRowType().getFieldCount());
      visitChild(join, 1, join.getRight());
      offset.set(x);
      return join;
    }

    private RexVisitorImpl<Void> rexVisitor(final RelNode rel) {
      return new RexVisitorImpl<Void>(true) {
        @Override public Void visitFieldAccess(RexFieldAccess fieldAccess) {
          final RexNode ref = fieldAccess.getReferenceExpr();
          if (ref instanceof RexCorrelVariable) {
            final RexCorrelVariable var = (RexCorrelVariable) ref;
            if (mapFieldAccessToCorVar.containsKey(fieldAccess)) {
              // for cases where different Rel nodes are referring to
              // same correlation var (e.g. in case of NOT IN)
              // avoid generating another correlation var
              // and record the 'rel' is using the same correlation
              mapRefRelToCorRef.put(rel,
                      mapFieldAccessToCorVar.get(fieldAccess));
            } else {
              final CorRef correlation =
                      new CorRef(var.id, fieldAccess.getField().getIndex(),
                              corrIdGenerator++);
              mapFieldAccessToCorVar.put(fieldAccess, correlation);
              mapRefRelToCorRef.put(rel, correlation);
            }
          }
          return super.visitFieldAccess(fieldAccess);
        }

        @Override public Void visitSubQuery(RexSubQuery subQuery) {
          subQuery.rel.accept(CorelMapBuilder.this);
          return super.visitSubQuery(subQuery);
        }
      };
    }
  }

  /** Frame describing the relational expression after decorrelation
   * and where to find the output fields and correlation variables
   * among its output fields. */
  static class Frame {
    final RelNode r;
    final ImmutableSortedMap<CorDef, Integer> corDefOutputs;
    final ImmutableSortedMap<Integer, Integer> oldToNewOutputs;

    Frame(RelNode oldRel, RelNode r, SortedMap<CorDef, Integer> corDefOutputs,
          Map<Integer, Integer> oldToNewOutputs) {
      this.r = Objects.requireNonNull(r);
      this.corDefOutputs = ImmutableSortedMap.copyOf(corDefOutputs);
      this.oldToNewOutputs = ImmutableSortedMap.copyOf(oldToNewOutputs);
      assert allLessThan(this.corDefOutputs.values(),
              r.getRowType().getFieldCount(), Litmus.THROW);
      assert allLessThan(this.oldToNewOutputs.keySet(),
              oldRel.getRowType().getFieldCount(), Litmus.THROW);
      assert allLessThan(this.oldToNewOutputs.values(),
              r.getRowType().getFieldCount(), Litmus.THROW);
    }
  }

  /** Base configuration for rules that are non-static in a RelDecorrelator. */
  public interface Config extends RelRule.Config {
    /** Returns the RelDecorrelator that will be context for the created
     * rule instance. */
    @ImmutableBeans.Property
    RelDecorrelator decorrelator();

    /** Sets {@link #decorrelator}. */
    Config withDecorrelator(RelDecorrelator decorrelator);
  }
}

