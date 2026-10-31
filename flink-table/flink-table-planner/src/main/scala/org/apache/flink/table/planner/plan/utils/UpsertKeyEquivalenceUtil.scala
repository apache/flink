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
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.planner.plan.metadata.{EquiJoinColumnPair, FlinkRelMetadataQuery}
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMultiJoin

import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelDistribution, RelNode}
import org.apache.calcite.rel.core.{Calc, Exchange, Join, JoinRelType, Project}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.{ImmutableBitSet, Util}

import java.util
import java.util.{List => JList, Map => JMap, Set => JSet}

import scala.collection.JavaConverters._

/**
 * Finds the upsert keys of a subtree that fall within a target column set, treating the columns of
 * [[FlinkRelMetadataQuery#getEquiJoinColumnPairs]] as interchangeable where the substitution is
 * null-safe (see `directionalSubstitutions`), also through chains of such pairs. The target is
 * fixed, so this stays bounded -- it never materializes equivalent key variants the way eager key
 * enrichment would. For a Calc that forwards deletes by key, [[upsertKeyColumns]] gives the columns
 * such keys can use, as one column set, so it does not enumerate keys either.
 */
object UpsertKeyEquivalenceUtil {

  /**
   * Upsert keys of `rel` (in its output columns) that are a subset of `target`, both those reported
   * by [[FlinkRelMetadataQuery#getUpsertKeys]] and those recovered by null-safe equi-join
   * substitution. Null if the upsert keys are undetermined.
   */
  def upsertKeysWithin(
      rel: RelNode,
      target: ImmutableBitSet,
      mq: FlinkRelMetadataQuery): JSet[ImmutableBitSet] = {
    val upsertKeys = mq.getUpsertKeys(rel)
    if (upsertKeys == null) {
      return null
    }
    val result = new util.LinkedHashSet[ImmutableBitSet]()
    upsertKeys.asScala.foreach(key => if (target.contains(key)) result.add(key))
    result.addAll(recoverWithin(rel, target, mq))
    result
  }

  /**
   * Every output column of `rel` that can be part of one of its upsert keys, reported or recovered
   * by equi-join substitution, for any target: a superset of the columns of every key
   * [[upsertKeysWithin]] can return. Recovered columns are pass-through join columns, which every
   * row a join emits carries: a join accepts deletes by key only from an input whose unique key
   * contains its join key, and a row an all-inner multi-join emits matched every join condition.
   */
  def upsertKeyColumns(rel: RelNode, mq: FlinkRelMetadataQuery): ImmutableBitSet = {
    val upsertKeys = mq.getUpsertKeys(rel)
    if (upsertKeys == null) {
      return ImmutableBitSet.of()
    }
    keyColumns(upsertKeys).union(recoverableColumns(rel, mq))
  }

  /** The columns [[recoverWithin]] can use, over all targets. */
  private def recoverableColumns(rel: RelNode, mq: FlinkRelMetadataQuery): ImmutableBitSet =
    rel match {
      case subset: RelSubset =>
        recoverableColumns(Util.first(subset.getBest, subset.getOriginal), mq)
      case vertex: HepRelVertex =>
        recoverableColumns(vertex.getCurrentRel, mq)
      case calc: Calc =>
        projectColumns(upsertKeyColumns(calc.getInput, mq), calcProjects(calc))
      case project: Project =>
        projectColumns(upsertKeyColumns(project.getInput, mq), project.getProjects.asScala)
      case exchange: Exchange =>
        upsertKeyColumns(exchange.getInput, mq)
      case join @ (_: Join | _: StreamPhysicalMultiJoin) =>
        recoverableColumnsAtJoin(join, mq)
      case _ =>
        ImmutableBitSet.of()
    }

  /** [[recoverAtJoin]] over all targets. */
  private def recoverableColumnsAtJoin(
      join: RelNode,
      mq: FlinkRelMetadataQuery): ImmutableBitSet = {
    val columns = keyPreservingInputs(join, mq).foldLeft(keyColumns(mq.getUpsertKeys(join))) {
      case (acc, (input, offset)) => acc.union(upsertKeyColumns(input, mq).shift(offset))
    }
    withSubstitutes(columns, substitutionsOf(join, mq))
  }

  /**
   * Keys that fit `target` only after an equi-join substitution. [[recoverableColumns]] walks the
   * same nodes, so a node handled here must be handled there too.
   */
  private def recoverWithin(
      rel: RelNode,
      target: ImmutableBitSet,
      mq: FlinkRelMetadataQuery): JSet[ImmutableBitSet] = rel match {
    case subset: RelSubset =>
      recoverWithin(Util.first(subset.getBest, subset.getOriginal), target, mq)
    case vertex: HepRelVertex =>
      recoverWithin(vertex.getCurrentRel, target, mq)
    case calc: Calc =>
      recoverThroughProjection(calcProjects(calc), calc.getInput, target, mq)
    case project: Project =>
      recoverThroughProjection(project.getProjects.asScala, project.getInput, target, mq)
    case exchange: Exchange =>
      keysKeptByExchange(exchange, recoverWithin(exchange.getInput, target, mq), mq)
    case join @ (_: Join | _: StreamPhysicalMultiJoin) =>
      recoverAtJoin(join, target, mq)
    case _ =>
      util.Collections.emptySet()
  }

  /**
   * The keys that stay upsert keys after `exchange`. A hash shuffle keeps only the keys that
   * contain its hash keys (modulo immutable columns): any other key may change its hash key in an
   * update, which then reaches two tasks in no particular order. This is the rule
   * [[FlinkRelMetadataQuery#getUpsertKeys]] applies to an exchange.
   */
  private def keysKeptByExchange(
      exchange: Exchange,
      keys: JSet[ImmutableBitSet],
      mq: FlinkRelMetadataQuery): JSet[ImmutableBitSet] = {
    if (exchange.getDistribution.getType != RelDistribution.Type.HASH_DISTRIBUTED) {
      return keys
    }
    val hashKeys = ImmutableBitSet.of(exchange.getDistribution.getKeys)
    val immutableColumns =
      Option(mq.getImmutableColumns(exchange.getInput)).getOrElse(ImmutableBitSet.of())
    val kept = new util.LinkedHashSet[ImmutableBitSet]()
    keys.asScala.foreach(key => if (key.union(immutableColumns).contains(hashKeys)) kept.add(key))
    kept
  }

  /** Maps the target down to the projection input, recovers keys there, and maps them back up. */
  private def recoverThroughProjection(
      projects: Seq[RexNode],
      input: RelNode,
      target: ImmutableBitSet,
      mq: FlinkRelMetadataQuery): JSet[ImmutableBitSet] = {
    val inputToOutput = FlinkRelMdUtil.inputToOutputPositions(projects.asJava)
    val inputTarget = ImmutableBitSet.builder()
    inputToOutput.asScala.foreach {
      case (in, outs) => if (outs.asScala.exists(target.get(_))) inputTarget.set(in)
    }

    val result = new util.LinkedHashSet[ImmutableBitSet]()
    recoverWithin(input, inputTarget.build(), mq).asScala.foreach {
      inputKey =>
        val outputKey = ImmutableBitSet.builder()
        var mappable = true
        for (inIdx <- inputKey.toArray if mappable) {
          inputToOutput.get(inIdx).asScala.find(target.get(_)) match {
            case Some(out) => outputKey.set(out)
            case None => mappable = false // not kept within the target -> cannot be expressed
          }
        }
        if (mappable) {
          result.add(outputKey.build())
        }
    }
    result
  }

  /**
   * Repairs the join's own keys, then recurses into the inputs whose keys the join preserves --
   * which reaches a deeper join when an intermediate projection dropped the equated column.
   */
  private def recoverAtJoin(
      join: RelNode,
      target: ImmutableBitSet,
      mq: FlinkRelMetadataQuery): JSet[ImmutableBitSet] = {
    val result = new util.LinkedHashSet[ImmutableBitSet]()
    val substitutes = substitutionsOf(join, mq)
    addRepaired(mq.getUpsertKeys(join), 0, target, substitutes, result)

    val expanded = expandToward(target, substitutes)
    keyPreservingInputs(join, mq).foreach {
      case (input, offset) =>
        val inputTarget = childTarget(expanded, offset, input.getRowType.getFieldCount)
        addRepaired(upsertKeysWithin(input, inputTarget, mq), offset, target, substitutes, result)
    }
    result
  }

  /**
   * The inputs of a join or multi-join whose upsert keys stay unique in its output, each with the
   * position of its first column there. Shared by [[recoverAtJoin]] and
   * [[recoverableColumnsAtJoin]] so that [[upsertKeyColumns]] covers every key [[upsertKeysWithin]]
   * can return.
   */
  private def keyPreservingInputs(rel: RelNode, mq: FlinkRelMetadataQuery): Seq[(RelNode, Int)] =
    rel match {
      case multiJoin: StreamPhysicalMultiJoin =>
        // only all-inner (no padding); see isKeyPreservingMultiJoinInput
        if (multiJoin.getJoinTypes.asScala.forall(_ == JoinRelType.INNER)) {
          val inputs = multiJoin.getInputs.asScala
          val offsets = inputs.scanLeft(0)(_ + _.getRowType.getFieldCount)
          inputs.indices
            .filter(i => isKeyPreservingMultiJoinInput(multiJoin, i, offsets, mq))
            .map(i => (inputs(i), offsets(i)))
        } else {
          Seq.empty
        }
      case join: Join if !join.getJoinType.projectsRight =>
        // SEMI/ANTI output the left side only and never duplicate a left row, so left keys survive
        Seq((join.getLeft, 0))
      case join: Join =>
        // a side's keys survive iff the other side is unique on the join key and this side isn't
        // padded
        val joinType = join.getJoinType
        val joinInfo = join.analyzeCondition()
        val left =
          if (
            !joinType.generatesNullsOnLeft &&
            isSideUnique(mq.getUpsertKeys(join.getRight), ImmutableBitSet.of(joinInfo.rightKeys))
          ) Seq((join.getLeft, 0))
          else Seq.empty
        val right =
          if (
            !joinType.generatesNullsOnRight &&
            isSideUnique(mq.getUpsertKeys(join.getLeft), ImmutableBitSet.of(joinInfo.leftKeys))
          ) Seq((join.getRight, join.getLeft.getRowType.getFieldCount))
          else Seq.empty
        left ++ right
      case _ =>
        Seq.empty
    }

  /**
   * Whether a row of input `start` of an all-inner multi-join matches at most one row of every
   * other input, so that the keys of `start` stay unique in the output. Starting from `start`, an
   * input is reached once it is unique on its columns equated to inputs already reached: those
   * columns are then fixed by the rows already matched. Columns equated only to inputs not reached
   * yet don't count, as such an input may still match several rows.
   */
  private def isKeyPreservingMultiJoinInput(
      multiJoin: StreamPhysicalMultiJoin,
      start: Int,
      offsets: Seq[Int],
      mq: FlinkRelMetadataQuery): Boolean = {
    val inputs = multiJoin.getInputs.asScala
    def inputOf(column: Int): Int = offsets.lastIndexWhere(_ <= column)
    // the multi-join's own equalities, between columns of two different inputs
    val crossPairs = mq
      .getEquiJoinColumnPairs(multiJoin)
      .asScala
      .map(pair => (pair.getSource, pair.getTarget))
      .filter { case (a, b) => inputOf(a) != inputOf(b) }
    val reached = new util.BitSet()
    reached.set(start)
    var grown = true
    while (grown) {
      grown = false
      for (j <- inputs.indices if !reached.get(j)) {
        val columns = ImmutableBitSet.builder()
        crossPairs.foreach {
          case (a, b) =>
            if (inputOf(a) == j && reached.get(inputOf(b))) columns.set(a - offsets(j))
            if (inputOf(b) == j && reached.get(inputOf(a))) columns.set(b - offsets(j))
        }
        if (isSideUnique(mq.getUpsertKeys(inputs(j)), columns.build())) {
          reached.set(j)
          grown = true
        }
      }
    }
    reached.cardinality == inputs.size
  }

  /**
   * Repairs each key toward `target` (shifting up by `offset` first) and adds the ones that fit.
   */
  private def addRepaired(
      keys: JSet[ImmutableBitSet],
      offset: Int,
      target: ImmutableBitSet,
      substitutes: JMap[Integer, JList[Integer]],
      result: JSet[ImmutableBitSet]): Unit = {
    if (keys == null) {
      return
    }
    keys.asScala.foreach(
      key => repairWithin(key.shift(offset), target, substitutes).foreach(result.add))
  }

  /** The columns a child must carry (in its own space) to land within `expanded`. */
  private def childTarget(
      expanded: ImmutableBitSet,
      offset: Int,
      fieldCount: Int): ImmutableBitSet =
    expanded.intersect(ImmutableBitSet.range(offset, offset + fieldCount)).shift(-offset)

  private def isSideUnique(
      sideKeys: JSet[ImmutableBitSet],
      sideJoinKeys: ImmutableBitSet): Boolean =
    sideKeys != null && sideKeys.asScala.exists(key => sideJoinKeys.contains(key))

  private def substitutionsOf(
      rel: RelNode,
      mq: FlinkRelMetadataQuery): JMap[Integer, JList[Integer]] =
    directionalSubstitutions(mq.getEquiJoinColumnPairs(rel), rel.getRowType)

  private def calcProjects(calc: Calc): Seq[RexNode] =
    calc.getProgram.getProjectList.asScala.map(calc.getProgram.expandLocalRef)

  /** The output positions where a projection passes any of `inputColumns` through as-is. */
  private def projectColumns(
      inputColumns: ImmutableBitSet,
      projects: Seq[RexNode]): ImmutableBitSet = {
    val inputToOutput = FlinkRelMdUtil.inputToOutputPositions(projects.asJava)
    val builder = ImmutableBitSet.builder()
    inputColumns.toArray.foreach {
      in => Option(inputToOutput.get(in)).foreach(_.asScala.foreach(out => builder.set(out)))
    }
    builder.build()
  }

  private def keyColumns(keys: JSet[ImmutableBitSet]): ImmutableBitSet =
    if (keys == null) ImmutableBitSet.of() else ImmutableBitSet.union(keys)

  /** `columns` plus every column that one or more substitutions can turn one of them into. */
  private def withSubstitutes(
      columns: ImmutableBitSet,
      substitutes: JMap[Integer, JList[Integer]]): ImmutableBitSet =
    ImmutableBitSet.of(reachable(columns.toArray, substitutes).asJava)

  /**
   * The columns each column may be replaced by. The two columns of a pair are equal or both null in
   * every row, so they may replace each other, unless the pair comes from an outer join's own
   * condition: then a column may only be replaced by a partner that is non-nullable in `rowType`,
   * which is never the column on the null-generating side.
   */
  private def directionalSubstitutions(
      pairs: JList[EquiJoinColumnPair],
      rowType: RelDataType): JMap[Integer, JList[Integer]] = {
    val fields = rowType.getFieldList
    def mayReplace(pair: EquiJoinColumnPair, replacement: Int): Boolean =
      !pair.isFromOuterJoin || !fields.get(replacement).getType.isNullable
    val substitutes = new util.HashMap[Integer, JList[Integer]]()
    def allow(from: Int, to: Int): Unit =
      substitutes.computeIfAbsent(from, (_: Integer) => new util.ArrayList[Integer]()).add(to)
    pairs.asScala.foreach {
      pair =>
        if (mayReplace(pair, pair.getTarget)) allow(pair.getSource, pair.getTarget)
        if (mayReplace(pair, pair.getSource)) allow(pair.getTarget, pair.getSource)
    }
    substitutes
  }

  /** `target` plus every column that one or more substitutions can turn into a `target` column. */
  private def expandToward(
      target: ImmutableBitSet,
      substitutes: JMap[Integer, JList[Integer]]): ImmutableBitSet = {
    val replaces = new util.HashMap[Integer, JList[Integer]]()
    substitutes.asScala.foreach {
      case (column, replacements) =>
        replacements.asScala.foreach(
          replacement =>
            replaces
              .computeIfAbsent(replacement, (_: Integer) => new util.ArrayList[Integer]())
              .add(column))
    }
    ImmutableBitSet.of(reachable(target.toArray, replaces).asJava)
  }

  /** The columns reachable from `start` along `edges`, breadth first, starting with `start`. */
  private def reachable(start: Seq[Int], edges: JMap[Integer, JList[Integer]]): Seq[Integer] = {
    val visited = new util.LinkedHashSet[Integer]()
    val pending = new util.ArrayDeque[Integer]()
    start.foreach(column => if (visited.add(column)) pending.add(column))
    while (!pending.isEmpty) {
      Option(edges.get(pending.poll()))
        .foreach(_.asScala.foreach(next => if (visited.add(next)) pending.add(next)))
    }
    visited.asScala.toSeq
  }

  /**
   * Rewrites `key` so every column lies in `target`, swapping an out-of-target column for an
   * in-target substitute. None if some column has neither itself nor a substitute there.
   */
  private def repairWithin(
      key: ImmutableBitSet,
      target: ImmutableBitSet,
      substitutes: JMap[Integer, JList[Integer]]): Option[ImmutableBitSet] = {
    val builder = ImmutableBitSet.builder()
    val columns = key.toArray
    var i = 0
    var repaired = true
    while (i < columns.length && repaired) {
      val column = columns(i)
      if (target.get(column)) {
        builder.set(column)
      } else {
        // the nearest column within the target that substitutions can turn this column into
        reachable(Seq(column), substitutes).find(c => target.get(c)) match {
          case Some(c) => builder.set(c)
          case None => repaired = false
        }
      }
      i += 1
    }
    if (repaired) Some(builder.build()) else None
  }
}
