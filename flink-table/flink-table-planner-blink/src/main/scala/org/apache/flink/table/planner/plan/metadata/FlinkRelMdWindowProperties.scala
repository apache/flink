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

package org.apache.flink.table.planner.plan.metadata

import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.util.{ImmutableBitSet, Util}
import org.apache.flink.table.planner.expressions.{PlannerProctimeAttribute, PlannerRowtimeAttribute, PlannerWindowEnd, PlannerWindowStart}
import org.apache.flink.table.planner.plan.`trait`.RelWindowProperties
import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy
import org.apache.flink.table.planner.plan.nodes.calcite.{Expand, WatermarkAssigner}
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalAggregate, FlinkLogicalCorrelate}
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalLookupJoin
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalCorrelateBase, StreamPhysicalMiniBatchAssigner, StreamPhysicalTemporalJoin, StreamPhysicalWindowAggregate, StreamPhysicalWindowTableFunction}
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase
import org.apache.flink.table.planner.plan.utils.WindowUtil
import org.apache.flink.table.planner.plan.utils.WindowUtil.{convertToWindowingStrategy, isWindowTableFunctionCall}
import org.apache.flink.table.planner.{JArrayList, JHashMap, JList}

import java.util.Collections

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * FlinkRelMdWindowProperties supplies a default implementation of
 * [[FlinkRelMetadataQuery#getRelWindowProperties]] for logical algebra.
 */
class FlinkRelMdWindowProperties private extends MetadataHandler[FlinkMetadata.WindowProperties] {

  override def getDef: MetadataDef[FlinkMetadata.WindowProperties] = {
    FlinkMetadata.WindowProperties.DEF
  }

  def getWindowProperties(rel: TableScan, mq: RelMetadataQuery): RelWindowProperties = {
    rel.getTable match {
      case table: FlinkPreparingTableBase => table.getStatistic.getRelWindowProperties
      case _ => null
    }
  }

  def getWindowProperties(rel: Project, mq: RelMetadataQuery): RelWindowProperties = {
    getProjectWindowProperties(rel.getProjects, rel.getInput, mq)
  }

  def getWindowProperties(rel: Filter, mq: RelMetadataQuery): RelWindowProperties = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getRelWindowProperties(rel.getInput)
  }

  def getWindowProperties(calc: Calc, mq: RelMetadataQuery): RelWindowProperties = {
    val input = calc.getInput
    val projects = calc.getProgram.getProjectList.map(calc.getProgram.expandLocalRef)
    getProjectWindowProperties(projects, input, mq)
  }

  private def getProjectWindowProperties(
      projects: JList[RexNode],
      input: RelNode,
      mq: RelMetadataQuery): RelWindowProperties = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val mapInToOutPos = buildProjectionMap(projects)
    if (mapInToOutPos.isEmpty) {
      // if there's no RexInputRef in the projected expressions, return no window properties
      return null
    }

    val childProps = fmq.getRelWindowProperties(input)
    if (childProps == null) {
      return null
    }

    childProps.copy(
      transformColumnIndex(childProps.getWindowStartColumns, mapInToOutPos),
      transformColumnIndex(childProps.getWindowEndColumns, mapInToOutPos),
      transformColumnIndex(childProps.getWindowTimeColumns, mapInToOutPos))
  }

  private def transformColumnIndex(
      columns: ImmutableBitSet,
      mapping: JHashMap[Int, JList[Int]]): ImmutableBitSet = {
    val newColumns = columns.flatMap {
      col => mapping.getOrDefault(col, Collections.emptyList[Int]())
    }
    ImmutableBitSet.of(newColumns.toArray: _*)
  }

  private def buildProjectionMap(projects: JList[RexNode]): JHashMap[Int, JList[Int]] = {
    val mapInToOutPos = new JHashMap[Int, JList[Int]]()

    def appendMapInToOutPos(inIndex: Int, outIndex: Int): Unit = {
      if (mapInToOutPos.contains(inIndex)) {
        mapInToOutPos(inIndex).add(outIndex)
      } else {
        val arrayBuffer = new JArrayList[Int]()
        arrayBuffer.add(outIndex)
        mapInToOutPos.put(inIndex, arrayBuffer)
      }
    }

    // Build an input to output position map.
    projects.zipWithIndex.foreach {
      case (projExpr, i) =>
        projExpr match {
          case ref: RexInputRef => appendMapInToOutPos(ref.getIndex, i)
          // rename
          case a: RexCall if a.getKind.equals(SqlKind.AS) &&
            a.getOperands.get(0).isInstanceOf[RexInputRef] =>
            appendMapInToOutPos(a.getOperands.get(0).asInstanceOf[RexInputRef].getIndex, i)
          // any operation on the window properties should lose the window attribute,
          // even it's a simple cast
          case _ => // ignore
        }
    }

    mapInToOutPos
  }

  def getWindowProperties(rel: Expand, mq: RelMetadataQuery): RelWindowProperties = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getRelWindowProperties(rel.getInput)
  }

  def getWindowProperties(rel: Exchange, mq: RelMetadataQuery): RelWindowProperties = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getRelWindowProperties(rel.getInput)
  }

  def getWindowProperties(rel: Union, mq: RelMetadataQuery): RelWindowProperties = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val properties = rel.getInputs.map(fmq.getRelWindowProperties)
    if (properties.contains(null)) {
      return null
    }
    val window = properties.head.getWindowSpec
    val isRowtime = properties.head.isRowtime
    if (properties.forall(p => window.equals(p.getWindowSpec) && isRowtime == p.isRowtime)) {
      val starts = properties.map(_.getWindowStartColumns).reduce((l, r) => l.intersect(r))
      val ends = properties.map(_.getWindowEndColumns).reduce((l, r) => l.intersect(r))
      val times = properties.map(_.getWindowTimeColumns).reduce((l, r) => l.intersect(r))
      properties.head.copy(starts, ends, times)
    } else {
      // window properties is lost if windows are not equal
      null
    }
  }

  def getWindowProperties(rel: TableFunctionScan, mq: RelMetadataQuery): RelWindowProperties = {
    if (isWindowTableFunctionCall(rel.getCall)) {
      val fieldCount = rel.getRowType.getFieldCount
      val windowingStrategy = convertToWindowingStrategy(
        rel.getCall.asInstanceOf[RexCall],
        rel.getInput(0).getRowType)

      RelWindowProperties.create(
        ImmutableBitSet.of(fieldCount - 3),
        ImmutableBitSet.of(fieldCount - 2),
        ImmutableBitSet.of(fieldCount - 1),
        windowingStrategy.window,
        windowingStrategy.timeAttributeType)
    } else {
      null
    }
  }

  def getWindowProperties(
      agg: FlinkLogicalAggregate,
      mq: RelMetadataQuery): RelWindowProperties = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val windowProperties = fmq.getRelWindowProperties(agg.getInput)
    val grouping = agg.getGroupSet
    if (!WindowUtil.groupingContainsWindowStartEnd(grouping, windowProperties)) {
      return null
    }

    val startColumns = windowProperties.getWindowStartColumns.intersect(grouping)
    val endColumns = windowProperties.getWindowEndColumns.intersect(grouping)
    val timeColumns = windowProperties.getWindowTimeColumns.intersect(grouping)

    RelWindowProperties.create(
      startColumns,
      endColumns,
      timeColumns,
      windowProperties.getWindowSpec,
      windowProperties.getTimeAttributeType
    )
  }

  def getWindowProperties(
      rel: StreamPhysicalWindowTableFunction,
      mq: RelMetadataQuery): RelWindowProperties = {
    val fieldCount = rel.getRowType.getFieldCount
    RelWindowProperties.create(
      ImmutableBitSet.of(fieldCount - 3),
      ImmutableBitSet.of(fieldCount - 2),
      ImmutableBitSet.of(fieldCount - 1),
      rel.windowing.window,
      rel.windowing.timeAttributeType
    )
  }

  def getWindowProperties(
      rel: StreamPhysicalWindowAggregate,
      mq: RelMetadataQuery): RelWindowProperties = {
    val starts = ArrayBuffer[Int]()
    val ends = ArrayBuffer[Int]()
    val times = ArrayBuffer[Int]()
    val propertyOffset = rel.grouping.length + rel.aggCalls.size()
    rel.namedWindowProperties.map(_.property).zipWithIndex.foreach { case (p, index) =>
      p match {
        case PlannerWindowStart(_) =>
          starts += propertyOffset + index

        case PlannerWindowEnd(_) =>
          ends += propertyOffset + index

        case PlannerRowtimeAttribute(_) | PlannerProctimeAttribute(_) =>
          times += propertyOffset + index
      }
    }
    RelWindowProperties.create(
      ImmutableBitSet.of(starts :_*),
      ImmutableBitSet.of(ends :_*),
      ImmutableBitSet.of(times :_*),
      rel.windowing.window,
      rel.windowing.timeAttributeType
    )
  }

  def getWindowProperties(
      rel: FlinkLogicalCorrelate,
      mq: RelMetadataQuery): RelWindowProperties = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getRelWindowProperties(rel.getInput(0))
  }

  def getWindowProperties(
      rel: StreamPhysicalCorrelateBase,
      mq: RelMetadataQuery): RelWindowProperties = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getRelWindowProperties(rel.getInput)
  }

  def getWindowProperties(
      rel: WatermarkAssigner,
      mq: RelMetadataQuery): RelWindowProperties = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getRelWindowProperties(rel.getInput)
  }

  def getWindowProperties(
      rel: StreamPhysicalMiniBatchAssigner,
      mq: RelMetadataQuery): RelWindowProperties = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getRelWindowProperties(rel.getInput)
  }

  def getWindowProperties(
    rel: CommonPhysicalLookupJoin,
    mq: RelMetadataQuery): RelWindowProperties = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getRelWindowProperties(rel.getInput)
  }

  def getWindowProperties(
      rel: StreamPhysicalTemporalJoin,
      mq: RelMetadataQuery): RelWindowProperties = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getRelWindowProperties(rel.getLeft)
  }

  def getWindowProperties(
      hepRelVertex: HepRelVertex,
      mq: RelMetadataQuery): RelWindowProperties = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getRelWindowProperties(hepRelVertex.getCurrentRel)
  }

  def getWindowProperties(
      subset: RelSubset,
      mq: RelMetadataQuery): RelWindowProperties = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val rel = Util.first(subset.getBest, subset.getOriginal)
    fmq.getRelWindowProperties(rel)
  }

  // catch-all rule when non of others apply.
  def getWindowProperties(rel: RelNode, mq: RelMetadataQuery): RelWindowProperties = null

}

object FlinkRelMdWindowProperties {

  private val INSTANCE = new FlinkRelMdWindowProperties

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    FlinkMetadata.WindowProperties.METHOD, INSTANCE)
}
