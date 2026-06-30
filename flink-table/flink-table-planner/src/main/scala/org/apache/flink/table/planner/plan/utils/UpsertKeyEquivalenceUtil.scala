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

import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMultiJoin

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Calc, Exchange, Join, JoinRelType, Project}
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.util.{ImmutableBitSet, Util}

import java.util
import java.util.{List => JList, Map => JMap, Set => JSet}

import scala.collection.JavaConverters._

/**
 * Finds the upsert keys of a subtree that fall within a target column set, treating equi-join
 * equivalents as interchangeable where the substitution is null-safe (only toward a non-nullable
 * column). The target is fixed, so this stays bounded -- it never materializes equivalent key
 * variants the way eager key enrichment would.
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

  /** Keys that fit `target` only after an equi-join substitution. */
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
      recoverWithin(exchange.getInput, target, mq)
    case multiJoin: StreamPhysicalMultiJoin =>
      recoverAtMultiJoin(multiJoin, target, mq)
    case join: Join =>
      recoverAtJoin(join, target, mq)
    case _ =>
      util.Collections.emptySet()
  }

  /** Maps the target down to the projection input, recovers keys there, and maps them back up. */
  private def recoverThroughProjection(
      projects: Seq[RexNode],
      input: RelNode,
      target: ImmutableBitSet,
      mq: FlinkRelMetadataQuery): JSet[ImmutableBitSet] = {
    val inputToOutput = inputToOutputMap(projects)
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
   * Repairs the join's own keys, then recurses into the side whose keys the join preserves -- which
   * reaches a deeper join when an intermediate projection dropped the equated column.
   */
  private def recoverAtJoin(
      join: Join,
      target: ImmutableBitSet,
      mq: FlinkRelMetadataQuery): JSet[ImmutableBitSet] = {
    val result = new util.LinkedHashSet[ImmutableBitSet]()
    val substitutes = substitutionsOf(join)
    val joinType = join.getJoinType
    val leftFieldCount = join.getLeft.getRowType.getFieldCount
    val rightFieldCount = join.getRight.getRowType.getFieldCount
    val expanded = expandToward(target, substitutes)

    addRepaired(mq.getUpsertKeys(join), 0, target, substitutes, result)

    // SEMI/ANTI output the left side only and never duplicate a left row, so left keys survive.
    if (!joinType.projectsRight) {
      val leftKeys = upsertKeysWithin(join.getLeft, childTarget(expanded, 0, leftFieldCount), mq)
      addRepaired(leftKeys, 0, target, substitutes, result)
      return result
    }

    // a side's keys survive iff the other side is unique on the join key and this side isn't padded
    val joinInfo = join.analyzeCondition()
    if (
      !joinType.generatesNullsOnLeft &&
      isSideUnique(mq.getUpsertKeys(join.getRight), ImmutableBitSet.of(joinInfo.rightKeys))
    ) {
      val leftKeys = upsertKeysWithin(join.getLeft, childTarget(expanded, 0, leftFieldCount), mq)
      addRepaired(leftKeys, 0, target, substitutes, result)
    }
    if (
      !joinType.generatesNullsOnRight &&
      isSideUnique(mq.getUpsertKeys(join.getLeft), ImmutableBitSet.of(joinInfo.leftKeys))
    ) {
      val target0 = childTarget(expanded, leftFieldCount, rightFieldCount)
      addRepaired(
        upsertKeysWithin(join.getRight, target0, mq),
        leftFieldCount,
        target,
        substitutes,
        result)
    }
    result
  }

  /** [[recoverAtJoin]] for the fused multi-join; recursion is limited to all-inner (no padding). */
  private def recoverAtMultiJoin(
      multiJoin: StreamPhysicalMultiJoin,
      target: ImmutableBitSet,
      mq: FlinkRelMetadataQuery): JSet[ImmutableBitSet] = {
    val result = new util.LinkedHashSet[ImmutableBitSet]()
    val substitutes = substitutionsOf(multiJoin)
    addRepaired(mq.getUpsertKeys(multiJoin), 0, target, substitutes, result)

    val inputs = multiJoin.getInputs.asScala
    if (multiJoin.getJoinTypes.asScala.forall(_ == JoinRelType.INNER)) {
      val expanded = expandToward(target, substitutes)
      var offset = 0
      inputs.indices.foreach {
        i =>
          val off = offset
          val fieldCount = inputs(i).getRowType.getFieldCount
          offset += fieldCount
          // input i is preserved when every other input is unique on its join key
          val othersUnique = inputs.indices.filter(_ != i).forall {
            j =>
              isSideUnique(
                mq.getUpsertKeys(inputs(j)),
                ImmutableBitSet.of(multiJoin.getJoinKeyIndices(j): _*))
          }
          if (othersUnique) {
            val inputKeys = upsertKeysWithin(inputs(i), childTarget(expanded, off, fieldCount), mq)
            addRepaired(inputKeys, off, target, substitutes, result)
          }
      }
    }
    result
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

  private def substitutionsOf(rel: RelNode): JMap[Integer, JList[Integer]] =
    directionalSubstitutions(equiPairsOf(rel), rel.getRowType)

  /** Equi-join column pairs of the whole subtree, undirected, in `rel`'s output columns. */
  private def equiPairsOf(rel: RelNode): Seq[(Int, Int)] = rel match {
    case subset: RelSubset =>
      equiPairsOf(Util.first(subset.getBest, subset.getOriginal))
    case vertex: HepRelVertex =>
      equiPairsOf(vertex.getCurrentRel)
    case calc: Calc =>
      remapPairs(equiPairsOf(calc.getInput), calcProjects(calc))
    case project: Project =>
      remapPairs(equiPairsOf(project.getInput), project.getProjects.asScala)
    case exchange: Exchange =>
      equiPairsOf(exchange.getInput)
    case multiJoin: StreamPhysicalMultiJoin =>
      val pairs = equalsRefPairs(multiJoin.getMultiJoinCondition).toBuffer
      var offset = 0
      multiJoin.getInputs.asScala.foreach {
        input =>
          val shiftBy = offset
          pairs ++= equiPairsOf(input).map { case (a, b) => (a + shiftBy, b + shiftBy) }
          offset += input.getRowType.getFieldCount
      }
      pairs.toSeq
    case join: Join if !join.getJoinType.projectsRight =>
      // SEMI/ANTI output only the left side, so the cross-side pairs are not in the output.
      equiPairsOf(join.getLeft)
    case join: Join =>
      val leftFieldCount = join.getLeft.getRowType.getFieldCount
      val ownPairs =
        join.analyzeCondition().pairs().asScala.map(p => (p.source, p.target + leftFieldCount))
      ownPairs.toSeq ++ equiPairsOf(join.getLeft) ++
        equiPairsOf(join.getRight).map { case (a, b) => (a + leftFieldCount, b + leftFieldCount) }
    case _ =>
      Seq.empty
  }

  private def calcProjects(calc: Calc): Seq[RexNode] =
    calc.getProgram.getProjectList.asScala.map(calc.getProgram.expandLocalRef)

  /** {{{col = col}}} pairs from a (possibly conjunctive) condition. */
  private def equalsRefPairs(condition: RexNode): Seq[(Int, Int)] = {
    if (condition == null) {
      return Seq.empty
    }
    RelOptUtil
      .conjunctions(condition)
      .asScala
      .flatMap {
        case call: RexCall if call.getKind == SqlKind.EQUALS =>
          (call.getOperands.get(0), call.getOperands.get(1)) match {
            case (l: RexInputRef, r: RexInputRef) => Some((l.getIndex, r.getIndex))
            case _ => None
          }
        case _ => None
      }
      .toSeq
  }

  private def remapPairs(pairs: Seq[(Int, Int)], projects: Seq[RexNode]): Seq[(Int, Int)] = {
    val inputToOutput = inputToOutputMap(projects)
    pairs.flatMap {
      case (i, j) =>
        val outsI = inputToOutput.getOrDefault(i, util.Collections.emptyList[Integer]()).asScala
        val outsJ = inputToOutput.getOrDefault(j, util.Collections.emptyList[Integer]()).asScala
        for (oi <- outsI; oj <- outsJ) yield (oi.intValue(), oj.intValue())
    }
  }

  /** Each input column to the output positions where a projection passes it through as-is. */
  private def inputToOutputMap(projects: Seq[RexNode]): JMap[Integer, JList[Integer]] = {
    val map = new util.HashMap[Integer, JList[Integer]]()
    projects.zipWithIndex.foreach {
      case (ref: RexInputRef, outIdx) =>
        map.computeIfAbsent(ref.getIndex, (_: Integer) => new util.ArrayList[Integer]()).add(outIdx)
      case _ =>
    }
    map
  }

  /** A column may be replaced by a partner only if that partner is non-nullable in `rowType`. */
  private def directionalSubstitutions(
      pairs: Seq[(Int, Int)],
      rowType: RelDataType): JMap[Integer, JList[Integer]] = {
    val fields = rowType.getFieldList
    def nonNullable(idx: Int): Boolean = !fields.get(idx).getType.isNullable
    val substitutes = new util.HashMap[Integer, JList[Integer]]()
    def allow(from: Int, to: Int): Unit =
      substitutes.computeIfAbsent(from, (_: Integer) => new util.ArrayList[Integer]()).add(to)
    pairs.foreach {
      case (i, j) =>
        if (nonNullable(j)) allow(i, j)
        if (nonNullable(i)) allow(j, i)
    }
    substitutes
  }

  /** `target` plus every column that can be substituted into a `target` column. */
  private def expandToward(
      target: ImmutableBitSet,
      substitutes: JMap[Integer, JList[Integer]]): ImmutableBitSet = {
    val builder = ImmutableBitSet.builder()
    target.toArray.foreach(builder.set)
    substitutes.asScala.foreach {
      case (column, replacements) =>
        if (replacements.asScala.exists(r => target.get(r))) {
          builder.set(column)
        }
    }
    builder.build()
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
        val candidates = substitutes.get(column)
        val replacement =
          if (candidates == null) None else candidates.asScala.find(c => target.get(c))
        replacement match {
          case Some(c) => builder.set(c)
          case None => repaired = false
        }
      }
      i += 1
    }
    if (repaired) Some(builder.build()) else None
  }
}
