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

package org.apache.flink.table.planner.plan.rules.logical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.LoptJoinTree;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.checkerframework.checker.initialization.qual.UnderInitialization;
import org.checkerframework.checker.initialization.qual.UnknownInitialization;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.RequiresNonNull;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Utility class that keeps track of the join factors that
 * make up a {@link MultiJoin}.
 */
public class LoptMultiJoin {
  //~ Instance fields --------------------------------------------------------

  /** The MultiJoin being optimized. */
  MultiJoin multiJoin;

  /**
   * Join filters associated with the MultiJoin, decomposed into a list.
   * Excludes left/right outer join filters.
   */
  private List<RexNode> joinFilters;

  /**
   * All join filters associated with the MultiJoin, decomposed into a
   * list. Includes left/right outer join filters.
   */
  private List<RexNode> allJoinFilters;

  /** Number of factors into the MultiJoin. */
  private final int nJoinFactors;

  /** Total number of fields in the MultiJoin. */
  private int nTotalFields;

  /** Original inputs into the MultiJoin. */
  private final ImmutableList<RelNode> joinFactors;

  /**
   * If a join factor is null-generating in a left or right outer join,
   * joinTypes indicates the join type corresponding to the factor. Otherwise,
   * it is set to INNER.
   */
  private final ImmutableList<JoinRelType> joinTypes;

  /**
   * If a join factor is null-generating in a left or right outer join, the
   * bitmap contains the non-null generating factors that the null-generating
   * factor is dependent upon.
   */
  private final ImmutableBitSet [] outerJoinFactors;

  /**
   * Bitmap corresponding to the fields projected from each join factor, after
   * row scan processing has completed. This excludes fields referenced in
   * join conditions, unless the field appears in the final projection list.
   */
  private List<@Nullable ImmutableBitSet> projFields;

  /**
   * Map containing reference counts of the fields referenced in join
   * conditions for each join factor. If a field is only required for a
   * semijoin, then it is removed from the reference count. (Hence the need
   * for reference counts instead of simply a bitmap.) The map is indexed by
   * the factor number.
   */
  private Map<Integer, int[]> joinFieldRefCountsMap;

  /**
   * For each join filter, associates a bitmap indicating all factors
   * referenced by the filter.
   */
  private final Map<RexNode, ImmutableBitSet> factorsRefByJoinFilter = new HashMap<>();

  /**
   * For each join filter, associates a bitmap indicating all fields
   * referenced by the filter.
   */
  private final Map<RexNode, ImmutableBitSet> fieldsRefByJoinFilter = new HashMap<>();

  /**
   * Starting RexInputRef index corresponding to each join factor.
   */
  int [] joinStart;

  /**
   * Number of fields in each join factor.
   */
  int [] nFieldsInJoinFactor;

  /**
   * Bitmap indicating which factors each factor references in join filters
   * that correspond to comparisons.
   */
  ImmutableBitSet @MonotonicNonNull [] factorsRefByFactor;

  /**
   * Weights of each factor combination.
   */
  int @MonotonicNonNull [][] factorWeights;

  /**
   * Type factory.
   */
  final RelDataTypeFactory factory;

  /**
   * Indicates for each factor whether its join can be removed because it is
   * the dimension table in a semijoin. If it can be, the entry indicates the
   * factor id of the fact table (corresponding to the dimension table) in the
   * semijoin that allows the factor to be removed. If the factor cannot be
   * removed, the entry corresponding to the factor is null.
   */
  @Nullable Integer [] joinRemovalFactors;

  /**
   * The semijoins that allow the join of a dimension table to be removed.
   */
  LogicalJoin[] joinRemovalSemiJoins;

  /**
   * Set of null-generating factors whose corresponding outer join can be
   * removed from the query plan.
   */
  Set<Integer> removableOuterJoinFactors;

  /**
   * Map consisting of all pairs of self-joins where the self-join can be
   * removed because the join between the identical factors is an equality
   * join on the same set of unique keys. The map is keyed by either factor in
   * the self join.
   */
  Map<Integer, RemovableSelfJoin> removableSelfJoinPairs;

  //~ Constructors -----------------------------------------------------------

  public LoptMultiJoin(MultiJoin multiJoin) {
    this.multiJoin = multiJoin;
    joinFactors = ImmutableList.copyOf(multiJoin.getInputs());
    nJoinFactors = joinFactors.size();
    projFields = multiJoin.getProjFields();
    joinFieldRefCountsMap = multiJoin.getCopyJoinFieldRefCountsMap();

    joinFilters =
        Lists.newArrayList(RelOptUtil.conjunctions(multiJoin.getJoinFilter()));

    allJoinFilters = new ArrayList<>(joinFilters);
    List<@Nullable RexNode> outerJoinFilters = multiJoin.getOuterJoinConditions();
    for (int i = 0; i < nJoinFactors; i++) {
      allJoinFilters.addAll(RelOptUtil.conjunctions(outerJoinFilters.get(i)));
    }

    int start = 0;
    nTotalFields = multiJoin.getRowType().getFieldCount();
    joinStart = new int[nJoinFactors];
    nFieldsInJoinFactor = new int[nJoinFactors];
    for (int i = 0; i < nJoinFactors; i++) {
      joinStart[i] = start;
      nFieldsInJoinFactor[i] =
          joinFactors.get(i).getRowType().getFieldCount();
      start += nFieldsInJoinFactor[i];
    }

    // Extract outer join information from the join factors, including the type
    // of outer join and the factors that a null-generating factor is dependent
    // upon.
    joinTypes = ImmutableList.copyOf(multiJoin.getJoinTypes());
    List<@Nullable RexNode> outerJoinConds = this.multiJoin.getOuterJoinConditions();
    outerJoinFactors = new ImmutableBitSet[nJoinFactors];
    for (int i = 0; i < nJoinFactors; i++) {
      RexNode outerJoinCond = outerJoinConds.get(i);
      if (outerJoinCond != null) {
        // set a bitmap containing the factors referenced in the
        // ON condition of the outer join; mask off the factor
        // corresponding to the factor itself
        ImmutableBitSet dependentFactors =
            getJoinFilterFactorBitmap(outerJoinCond, false);
        dependentFactors = dependentFactors.clear(i);
        outerJoinFactors[i] = dependentFactors;
      }
    }

    // determine which join factors each join filter references
    setJoinFilterRefs();

    factory = multiJoin.getCluster().getTypeFactory();

    joinRemovalFactors = new Integer[nJoinFactors];
    joinRemovalSemiJoins = new LogicalJoin[nJoinFactors];

    removableOuterJoinFactors = new HashSet<>();
    removableSelfJoinPairs = new HashMap<>();
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns the MultiJoin corresponding to this multi-join.
   */
  public MultiJoin getMultiJoinRel() {
    return multiJoin;
  }

  /**
   * Returns the number of factors in this multi-join.
   */
  public int getNumJoinFactors() {
    return nJoinFactors;
  }

  /**
   * Returns the factor corresponding to the given factor index.
   *
   * @param factIdx Factor to be returned
   */
  public RelNode getJoinFactor(int factIdx) {
    return joinFactors.get(factIdx);
  }

  /**
   * Returns the total number of fields in the multi-join.
   */
  public int getNumTotalFields() {
    return nTotalFields;
  }

  /**
   * Returns the number of fields in a given factor.
   *
   * @param factIdx Desired factor
   */
  public int getNumFieldsInJoinFactor(int factIdx) {
    return nFieldsInJoinFactor[factIdx];
  }

  /**
   * Returns all non-outer join filters in this multi-join.
   */
  public List<RexNode> getJoinFilters() {
    return joinFilters;
  }

  /**
   * Returns a bitmap corresponding to the factors referenced within
   * the specified join filter.
   *
   * @param joinFilter Filter for which information will be returned
   */
  public ImmutableBitSet getFactorsRefByJoinFilter(RexNode joinFilter) {
    return requireNonNull(
        factorsRefByJoinFilter.get(joinFilter),
        () -> "joinFilter is not found in factorsRefByJoinFilter: " + joinFilter);
  }

  /**
   * Returns an array of fields contained within the multi-join.
   */
  public List<RelDataTypeField> getMultiJoinFields() {
    return multiJoin.getRowType().getFieldList();
  }

  /**
   * Returns a bitmap corresponding to the fields referenced by a join filter.
   *
   * @param joinFilter the filter for which information will be returned
   */
  public ImmutableBitSet getFieldsRefByJoinFilter(RexNode joinFilter) {
    return requireNonNull(
        fieldsRefByJoinFilter.get(joinFilter),
        () -> "joinFilter is not found in fieldsRefByJoinFilter: " + joinFilter);
  }

  /**
   * Returns weights of the different factors relative to one another.
   */
  public int @Nullable [][] getFactorWeights() {
    return factorWeights;
  }

  /**
   * Returns a bitmap corresponding to the factors referenced by the specified
   * factor in the various join filters that correspond to comparisons.
   *
   * @param factIdx Factor for which information will be returned
   */
  public ImmutableBitSet getFactorsRefByFactor(int factIdx) {
    return requireNonNull(factorsRefByFactor, "factorsRefByFactor")[factIdx];
  }

  /**
   * Returns the starting offset within the multi-join for the specified factor.
   *
   * @param factIdx Factor for which information will be returned
   */
  public int getJoinStart(int factIdx) {
    return joinStart[factIdx];
  }

  /**
   * Returns whether the factor corresponds to a null-generating factor
   * in a left or right outer join.
   *
   * @param factIdx Factor for which information will be returned
   */
  public boolean isNullGenerating(int factIdx) {
    return joinTypes.get(factIdx).isOuterJoin();
  }

  /**
   * Returns a bitmap containing the factors that a null-generating factor is
   * dependent upon, if the factor is null-generating in a left or right outer
   * join; otherwise null is returned.
   *
   * @param factIdx Factor for which information will be returned
   */
  public ImmutableBitSet getOuterJoinFactors(int factIdx) {
    return outerJoinFactors[factIdx];
  }

  /**
   * Returns outer join conditions associated with the specified null-generating
   * factor.
   *
   * @param factIdx Factor for which information will be returned
   */
  public @Nullable RexNode getOuterJoinCond(int factIdx) {
    return multiJoin.getOuterJoinConditions().get(factIdx);
  }

  /**
   * Returns a bitmap containing the fields that are projected from a factor.
   *
   * @param factIdx Factor for which information will be returned
   */
  public @Nullable ImmutableBitSet getProjFields(int factIdx) {
    return projFields.get(factIdx);
  }

  /**
   * Returns the join field reference counts for a factor.
   *
   * @param factIdx Factor for which information will be returned
   */
  public int [] getJoinFieldRefCounts(int factIdx) {
    return requireNonNull(
        joinFieldRefCountsMap.get(factIdx),
        () -> "no entry in joinFieldRefCountsMap found for " + factIdx);
  }

  /**
   * Returns the factor id of the fact table corresponding to a dimension
   * table in a semi-join, in the case where the join with the dimension table
   * can be removed.
   *
   * @param dimIdx Dimension factor for which information will be returned
   */
  public @Nullable Integer getJoinRemovalFactor(int dimIdx) {
    return joinRemovalFactors[dimIdx];
  }

  /**
   * Returns the semi-join that allows the join of a dimension table to be
   * removed.
   *
   * @param dimIdx Dimension factor for which information will be returned
   */
  public LogicalJoin getJoinRemovalSemiJoin(int dimIdx) {
    return joinRemovalSemiJoins[dimIdx];
  }

  /**
   * Indicates that a dimension factor's join can be removed because of a
   * semijoin with a fact table.
   *
   * @param dimIdx Dimension factor
   * @param factIdx Fact factor
   */
  public void setJoinRemovalFactor(int dimIdx, int factIdx) {
    joinRemovalFactors[dimIdx] = factIdx;
  }

  /**
   * Indicates the semi-join that allows the join of a dimension table to be
   * removed.
   *
   * @param dimIdx Dimension factor
   * @param semiJoin the semijoin
   */
  public void setJoinRemovalSemiJoin(int dimIdx, LogicalJoin semiJoin) {
    joinRemovalSemiJoins[dimIdx] = semiJoin;
  }

  /**
   * Returns a bitmap representing the factors referenced in a join filter.
   *
   * @param joinFilter the join filter
   * @param setFields if true, add the fields referenced by the join filter
   * into a map
   *
   * @return the bitmap containing the factor references
   */
  @RequiresNonNull({"joinStart", "nFieldsInJoinFactor"})
  ImmutableBitSet getJoinFilterFactorBitmap(
      @UnderInitialization LoptMultiJoin this,
      RexNode joinFilter,
      boolean setFields) {
    ImmutableBitSet fieldRefBitmap = fieldBitmap(joinFilter);
    if (setFields) {
      fieldsRefByJoinFilter.put(joinFilter, fieldRefBitmap);
    }

    return factorBitmap(fieldRefBitmap);
  }

  private static ImmutableBitSet fieldBitmap(RexNode joinFilter) {
    final RelOptUtil.InputFinder inputFinder = new RelOptUtil.InputFinder();
    joinFilter.accept(inputFinder);
    return inputFinder.build();
  }

  /**
   * Sets bitmaps indicating which factors and fields each join filter
   * references.
   */
  @RequiresNonNull({"allJoinFilters", "joinStart", "nFieldsInJoinFactor"})
  private void setJoinFilterRefs(
      @UnderInitialization LoptMultiJoin this) {
    ListIterator<RexNode> filterIter = allJoinFilters.listIterator();
    while (filterIter.hasNext()) {
      RexNode joinFilter = filterIter.next();

      // ignore the literal filter; if necessary, we'll add it back
      // later
      if (joinFilter.isAlwaysTrue()) {
        filterIter.remove();
      }
      ImmutableBitSet factorRefBitmap =
          getJoinFilterFactorBitmap(joinFilter, true);
      factorsRefByJoinFilter.put(joinFilter, factorRefBitmap);
    }
  }

  /**
   * Sets the bitmap indicating which factors a filter references based on
   * which fields it references.
   *
   * @param fieldRefBitmap bitmap representing fields referenced
   * @return bitmap representing factors referenced that will
   * be set by this method
   */
  @RequiresNonNull({"joinStart", "nFieldsInJoinFactor"})
  private ImmutableBitSet factorBitmap(
      @UnknownInitialization LoptMultiJoin this,
      ImmutableBitSet fieldRefBitmap) {
    ImmutableBitSet.Builder factorRefBitmap = ImmutableBitSet.builder();
    for (int field : fieldRefBitmap) {
      int factor = findRef(field);
      factorRefBitmap.set(factor);
    }
    return factorRefBitmap.build();
  }

  /**
   * Determines the join factor corresponding to a RexInputRef.
   *
   * @param rexInputRef rexInputRef index
   *
   * @return index corresponding to join factor
   */
  @RequiresNonNull({"joinStart", "nFieldsInJoinFactor"})
  public int findRef(
      @UnknownInitialization LoptMultiJoin this,
      int rexInputRef) {
    for (int i = 0; i < nJoinFactors; i++) {
      if ((rexInputRef >= joinStart[i])
          && (rexInputRef < (joinStart[i] + nFieldsInJoinFactor[i]))) {
        return i;
      }
    }
    throw new AssertionError();
  }

  /**
   * Sets weighting for each combination of factors, depending on which join
   * filters reference which factors. Greater weight is given to equality
   * conditions. Also, sets bitmaps indicating which factors are referenced by
   * each factor within join filters that are comparisons.
   */
  public void setFactorWeights() {
    factorWeights = new int[nJoinFactors][nJoinFactors];
    factorsRefByFactor = new ImmutableBitSet[nJoinFactors];
    for (int i = 0; i < nJoinFactors; i++) {
      factorsRefByFactor[i] = ImmutableBitSet.of();
    }

    for (RexNode joinFilter : allJoinFilters) {
      ImmutableBitSet factorRefs = factorsRefByJoinFilter.get(joinFilter);

      // don't give weights to non-comparison expressions
      if (!(joinFilter instanceof RexCall)) {
        continue;
      }
      if (!joinFilter.isA(SqlKind.COMPARISON)) {
        continue;
      }

      // OR the factors referenced in this join filter into the
      // bitmaps corresponding to each of the factors; however,
      // exclude the bit corresponding to the factor itself
      for (int factor : requireNonNull(factorRefs, "factorRefs")) {
        factorsRefByFactor[factor] =
            factorsRefByFactor[factor]
                .rebuild()
                .addAll(factorRefs)
                .clear(factor)
                .build();
      }

      if (factorRefs.cardinality() == 2) {
        int leftFactor = factorRefs.nextSetBit(0);
        int rightFactor = factorRefs.nextSetBit(leftFactor + 1);

        final RexCall call = (RexCall) joinFilter;
        ImmutableBitSet leftFields = fieldBitmap(call.getOperands().get(0));
        ImmutableBitSet leftBitmap = factorBitmap(leftFields);

        // filter contains only two factor references, one on each
        // side of the operator
        int weight;
        if (leftBitmap.cardinality() == 1) {
          // give higher weight to equi-joins
          switch (joinFilter.getKind()) {
          case EQUALS:
            weight = 3;
            break;
          default:
            weight = 2;
          }
        } else {
          // cross product of two tables
          weight = 1;
        }
        setFactorWeight(weight, leftFactor, rightFactor);
      } else {
        // multiple factor references -- set a weight for each
        // combination of factors referenced within the filter
        final List<Integer> list  = ImmutableIntList.copyOf(factorRefs);
        for (int outer : list) {
          for (int inner : list) {
            if (outer != inner) {
              setFactorWeight(1, outer, inner);
            }
          }
        }
      }
    }
  }

  /**
   * Sets an individual weight if the new weight is better than the current
   * one.
   *
   * @param weight weight to be set
   * @param leftFactor index of left factor
   * @param rightFactor index of right factor
   */
  @RequiresNonNull("factorWeights")
  private void setFactorWeight(int weight, int leftFactor, int rightFactor) {
    if (factorWeights[leftFactor][rightFactor] < weight) {
      factorWeights[leftFactor][rightFactor] = weight;
      factorWeights[rightFactor][leftFactor] = weight;
    }
  }

  /**
   * Returns whether if a join tree contains all factors required.
   *
   * @param joinTree Join tree to be examined
   * @param factorsNeeded Bitmap of factors required
   *
   * @return true if join tree contains all required factors
   */
  public boolean hasAllFactors(
      LoptJoinTree joinTree,
      BitSet factorsNeeded) {
    return BitSets.contains(BitSets.of(joinTree.getTreeOrder()), factorsNeeded);
  }

  /**
   * Sets a bitmap indicating all child RelNodes in a join tree.
   *
   * @param joinTree join tree to be examined
   * @param childFactors bitmap to be set
   */
  @Deprecated // to be removed before 2.0
  public void getChildFactors(LoptJoinTree joinTree,
      ImmutableBitSet.Builder childFactors) {
    for (int child : joinTree.getTreeOrder()) {
      childFactors.set(child);
    }
  }

  /**
   * Retrieves the fields corresponding to a join between a left and right
   * tree.
   *
   * @param left left hand side of the join
   * @param right right hand side of the join
   *
   * @return fields of the join
   */
  public List<RelDataTypeField> getJoinFields(
      LoptJoinTree left,
      LoptJoinTree right) {
    RelDataType rowType =
        factory.createJoinType(
            left.getJoinTree().getRowType(),
            right.getJoinTree().getRowType());
    return rowType.getFieldList();
  }

  /**
   * Adds a join factor to the set of factors that can be removed because the
   * factor is the null-generating factor in an outer join, its join keys are
   * unique, and the factor is not projected in the query.
   *
   * @param factIdx Join factor
   */
  public void addRemovableOuterJoinFactor(int factIdx) {
    removableOuterJoinFactors.add(factIdx);
  }

  /**
   * Return whether the factor corresponds to the null-generating factor in
   * an outer join that can be removed.
   *
   * @param factIdx Factor in question
   */
  public boolean isRemovableOuterJoinFactor(int factIdx) {
    return removableOuterJoinFactors.contains(factIdx);
  }

  /**
   * Adds to a map that keeps track of removable self-join pairs.
   *
   * @param factor1 one of the factors in the self-join
   * @param factor2 the second factor in the self-join
   */
  public void addRemovableSelfJoinPair(int factor1, int factor2) {
    int leftFactor;
    int rightFactor;

    // Put the factor with more fields on the left so it will be
    // preserved after the self-join is removed.
    if (getNumFieldsInJoinFactor(factor1)
        > getNumFieldsInJoinFactor(factor2)) {
      leftFactor = factor1;
      rightFactor = factor2;
    } else {
      leftFactor = factor2;
      rightFactor = factor1;
    }

    // Compute a column mapping such that if a column from the right
    // factor is also referenced in the left factor, we will map the
    // right reference to the left to avoid redundant references.
    final Map<Integer, Integer> columnMapping = new HashMap<>();

    // First, locate the originating column for all simple column
    // references in the left factor.
    final RelNode left = getJoinFactor(leftFactor);
    final RelMetadataQuery mq = left.getCluster().getMetadataQuery();
    final Map<Integer, Integer> leftFactorColMapping = new HashMap<>();
    for (int i = 0; i < left.getRowType().getFieldCount(); i++) {
      final RelColumnOrigin colOrigin = mq.getColumnOrigin(left, i);
      if (colOrigin != null && colOrigin.isDerived()) {
        leftFactorColMapping.put(
            colOrigin.getOriginColumnOrdinal(),
            i);
      }
    }

    // Then, see if the right factor references any of the same columns
    // by locating their originating columns.  If there are matches,
    // then we want to store the corresponding offset into the left
    // factor.
    RelNode right = getJoinFactor(rightFactor);
    for (int i = 0; i < right.getRowType().getFieldCount(); i++) {
      final RelColumnOrigin colOrigin = mq.getColumnOrigin(right, i);
      if (colOrigin == null || !colOrigin.isDerived()) {
        continue;
      }
      Integer leftOffset =
          leftFactorColMapping.get(colOrigin.getOriginColumnOrdinal());
      if (leftOffset == null) {
        continue;
      }
      columnMapping.put(i, leftOffset);
    }

    RemovableSelfJoin selfJoin =
        new RemovableSelfJoin(leftFactor, rightFactor, columnMapping);

    removableSelfJoinPairs.put(leftFactor, selfJoin);
    removableSelfJoinPairs.put(rightFactor, selfJoin);
  }

  /**
   * Returns the other factor in a self-join pair if the factor passed in is
   * a factor in a removable self-join; otherwise, returns null.
   *
   * @param factIdx one of the factors in a self-join pair
   */
  public @Nullable Integer getOtherSelfJoinFactor(int factIdx) {
    RemovableSelfJoin selfJoin = removableSelfJoinPairs.get(factIdx);
    if (selfJoin == null) {
      return null;
    } else if (selfJoin.rightFactor == factIdx) {
      return selfJoin.leftFactor;
    } else {
      return selfJoin.rightFactor;
    }
  }

  /**
   * Returns whether the factor is the left factor in a self-join.
   *
   * @param factIdx Factor in a self-join
   */
  public boolean isLeftFactorInRemovableSelfJoin(int factIdx) {
    RemovableSelfJoin selfJoin = removableSelfJoinPairs.get(factIdx);
    if (selfJoin == null) {
      return false;
    }
    return selfJoin.leftFactor == factIdx;
  }

  /**
   * Returns whether the factor is the right factor in a self-join.
   *
   * @param factIdx Factor in a self-join
   */
  public boolean isRightFactorInRemovableSelfJoin(int factIdx) {
    RemovableSelfJoin selfJoin = removableSelfJoinPairs.get(factIdx);
    if (selfJoin == null) {
      return false;
    }
    return selfJoin.rightFactor == factIdx;
  }

  /**
   * Determines whether there is a mapping from a column in the right factor
   * of a self-join to a column from the left factor. Assumes that the right
   * factor is a part of a self-join.
   *
   * @param rightFactor the index of the right factor
   * @param rightOffset the column offset of the right factor
   *
   * @return the offset of the corresponding column in the left factor, if
   * such a column mapping exists; otherwise, null is returned
   */
  public @Nullable Integer getRightColumnMapping(int rightFactor, int rightOffset) {
    RemovableSelfJoin selfJoin = requireNonNull(removableSelfJoinPairs.get(rightFactor),
        () -> "removableSelfJoinPairs.get(rightFactor) is null for " + rightFactor
            + ", map=" + removableSelfJoinPairs);
    assert selfJoin.rightFactor == rightFactor;
    return selfJoin.columnMapping.get(rightOffset);
  }

  public Edge createEdge(RexNode condition) {
    ImmutableBitSet fieldRefBitmap = fieldBitmap(condition);
    ImmutableBitSet factorRefBitmap = factorBitmap(fieldRefBitmap);
    return new Edge(condition, factorRefBitmap, fieldRefBitmap);
  }

  /** Information about a join-condition. */
  static class Edge {
    final ImmutableBitSet factors;
    final ImmutableBitSet columns;
    final RexNode condition;

    Edge(RexNode condition, ImmutableBitSet factors, ImmutableBitSet columns) {
      this.condition = condition;
      this.factors = factors;
      this.columns = columns;
    }

    @Override public String toString() {
      return "Edge(condition: " + condition
          + ", factors: " + factors
          + ", columns: " + columns + ")";
    }
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Utility class used to keep track of the factors in a removable self-join.
   * The right factor in the self-join is the one that will be removed.
   */
  private static class RemovableSelfJoin {
    /** The left factor in a removable self-join. */
    private final int leftFactor;

    /** The right factor in a removable self-join, namely the factor that will
     * be removed. */
    private final int rightFactor;

    /** A mapping that maps references to columns from the right factor to
     * columns in the left factor, if the column is referenced in both
     * factors. */
    private final Map<Integer, Integer> columnMapping;

    RemovableSelfJoin(
        int leftFactor,
        int rightFactor,
        Map<Integer, Integer> columnMapping) {
      this.leftFactor = leftFactor;
      this.rightFactor = rightFactor;
      this.columnMapping = columnMapping;
    }
  }
}
