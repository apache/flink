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

package org.apache.flink.table.plan.subplan

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.table.api.{StreamTableEnvironment, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.expressions._
import org.apache.flink.table.plan.`trait`.{AccMode, AccModeTraitDef, UpdateAsRetractionTraitDef}
import org.apache.flink.table.plan.logical.LogicalNode
import org.apache.flink.table.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.calcite.Sink
import org.apache.flink.table.plan.nodes.physical.stream.{StreamExecDataStreamScan, StreamExecIntermediateTableScan, StreamPhysicalRel}
import org.apache.flink.table.plan.schema.IntermediateRelNodeTable
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.sinks.BaseRetractStreamTableSink
import org.apache.flink.util.Preconditions

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan

import java.util

import scala.collection.JavaConversions._

/**
  * DAG optimizer for Stream.
  */
object StreamDAGOptimizer extends AbstractDAGOptimizer[StreamTableEnvironment] {

  override protected def doOptimize(
      sinks: Seq[LogicalNode],
      tEnv: StreamTableEnvironment): Seq[RelNodeBlock] = {
    // build RelNodeBlock plan
    val relNodeBlocks = RelNodeBlockPlanBuilder.buildRelNodeBlockPlan(sinks, tEnv)
    // infer updateAsRetraction property for each block
    relNodeBlocks.foreach {
      sinkBlock =>
        val retractionFromSink = sinkBlock.outputNode match {
          case n: Sink => n.sink.isInstanceOf[BaseRetractStreamTableSink[_]]
          case _ => false
        }
        sinkBlock.setUpdateAsRetraction(retractionFromSink)
        inferUpdateAsRetraction(tEnv, sinkBlock, retractionFromSink)
    }

    // propagate updateAsRetraction property to all input blocks
    relNodeBlocks.foreach(propagateUpdateAsRetraction)
    // clear the intermediate result
    relNodeBlocks.foreach(resetIntermediateResult)
    // optimize recursively RelNodeBlock
    relNodeBlocks.foreach(block => optimizeBlock(block, tEnv))
    relNodeBlocks
  }

  private def optimizeBlock(block: RelNodeBlock, tEnv: StreamTableEnvironment): Unit = {
    block.children.foreach {
      child =>
        if (child.getNewOutputNode.isEmpty) {
          optimizeBlock(child, tEnv)
        }
    }

    val blockLogicalPlan = block.getPlan
    blockLogicalPlan match {
      case n: Sink =>
        val optimizedTree = tEnv.optimize(n)
        block.setOptimizedPlan(optimizedTree)

      case o =>
        val optimizedPlan = tEnv.optimize(
          o,
          updatesAsRetraction = block.isUpdateAsRetraction,
          isSinkBlock = false)
        val isAccRetract = optimizedPlan.getTraitSet
          .getTrait(AccModeTraitDef.INSTANCE).getAccMode == AccMode.AccRetract
        val rowType = optimizedPlan.getRowType
        val fieldExpressions = getExprsWithTimeAttribute(o.getRowType, rowType)
        val name = tEnv.createUniqueTableName()
        registerIntermediateTable(tEnv, name, optimizedPlan, isAccRetract, fieldExpressions)
        val newTable = tEnv.scan(name)
        block.setNewOutputNode(newTable.getRelNode)
        block.setOutputTableName(name)
        block.setOptimizedPlan(optimizedPlan)
    }
  }

  /**
    * Infer UpdateAsRetraction property for each block.
    *
    * @param block              The [[RelNodeBlock]] instance.
    * @param retractionFromSink Whether the sink need update as retraction messages.
    */
  private def inferUpdateAsRetraction(
      tEnv: StreamTableEnvironment,
      block: RelNodeBlock,
      retractionFromSink: Boolean): Unit = {

    block.children.foreach {
      child =>
        if (child.getNewOutputNode.isEmpty) {
          inferUpdateAsRetraction(tEnv, child, retractionFromSink = false)
        }
    }

    block.getPlan match {
      case n: Sink =>
        val optimizedPlan = tEnv.optimize(n, retractionFromSink)
        block.setOptimizedPlan(optimizedPlan)

      case o =>
        val optimizedPlan = tEnv.optimize(o, retractionFromSink)
        val rowType = optimizedPlan.getRowType
        val fieldExpressions = getExprsWithTimeAttribute(o.getRowType, rowType)
        val name = tEnv.createUniqueTableName()
        registerIntermediateTable(tEnv, name, optimizedPlan, isAccRetract = false, fieldExpressions)
        val newTable = tEnv.scan(name)
        block.setNewOutputNode(newTable.getRelNode)
        block.setOutputTableName(name)
        block.setOptimizedPlan(optimizedPlan)
    }
  }

  /**
    * Propagate updateAsRetraction property to all input blocks
    *
    * @param block The [[RelNodeBlock]] instance.
    */
  private def propagateUpdateAsRetraction(block: RelNodeBlock): Unit = {

    // process current block
    def shipUpdateAsRetraction(rel: RelNode, updateAsRetraction: Boolean): Unit = {
      rel match {
        case _: StreamExecDataStreamScan | _: StreamExecIntermediateTableScan =>
          val scan = rel.asInstanceOf[TableScan]
          val retractionTrait = scan.getTraitSet.getTrait(UpdateAsRetractionTraitDef.INSTANCE)
          if (retractionTrait.sendsUpdatesAsRetractions || updateAsRetraction) {
            val tableName = scan.getTable.getQualifiedName.last
            val retractionBlocks = block.children.filter(_.getOutputTableName eq tableName)
            Preconditions.checkArgument(retractionBlocks.size <= 1)
            if (retractionBlocks.size == 1) {
              retractionBlocks.head.setUpdateAsRetraction(true)
            }
          }
        case ser: StreamPhysicalRel => ser.getInputs.foreach(e => {
          if (ser.needsUpdatesAsRetraction(e) || (updateAsRetraction && !ser.consumesRetractions)) {
            shipUpdateAsRetraction(e, updateAsRetraction = true)
          } else {
            shipUpdateAsRetraction(e, updateAsRetraction = false)
          }
        })
      }
    }

    shipUpdateAsRetraction(block.getOptimizedPlan, block.isUpdateAsRetraction)
    block.children.foreach(propagateUpdateAsRetraction)
  }


  /**
    * Reset the intermediate result including newOutputNode and outputTableName
    *
    * @param block the [[RelNodeBlock]] instance.
    */
  private def resetIntermediateResult(block: RelNodeBlock): Unit = {
    block.setNewOutputNode(null)
    block.setOutputTableName(null)

    block.children.foreach {
      child =>
        if (child.getNewOutputNode.nonEmpty) {
          resetIntermediateResult(child)
        }
    }
  }

  private def registerIntermediateTable(
      tEnv: StreamTableEnvironment,
      name: String,
      relNode: RelNode,
      isAccRetract: Boolean,
      fields: Array[Expression]): Unit = {
    val rowType = relNode.getRowType
    val streamType = FlinkTypeFactory.toInternalRowType(rowType)

    // validate and extract time attributes
    val (rowtime, _) = tEnv.validateAndExtractTimeAttributes(streamType, fields)

    // check if event-time is enabled
    if (rowtime.isDefined &&
      tEnv.execEnv.getStreamTimeCharacteristic != TimeCharacteristic.EventTime) {
      throw new TableException(
        s"A rowtime attribute requires an EventTime time characteristic in stream environment. " +
          s"But is: ${tEnv.execEnv.getStreamTimeCharacteristic}")
    }

    val uniqueKeys = getUniqueKeys(tEnv, relNode)
    val monotonicity = FlinkRelMetadataQuery
      .reuseOrCreate(tEnv.getRelBuilder.getCluster.getMetadataQuery)
      .getRelModifiedMonotonicity(relNode)
    val statistic = FlinkStatistic.builder.uniqueKeys(uniqueKeys).monotonicity(monotonicity).build()

    val table = new IntermediateRelNodeTable(
      relNode,
      isAccRetract,
      statistic)
    tEnv.registerTableInternal(name, table)
  }

  /**
    * Mark Expression to RowtimeAttribute or ProctimeAttribute for time indicators
    */
  private def getExprsWithTimeAttribute(
      preRowType: RelDataType,
      postRowType: RelDataType): Array[Expression] = {

    preRowType.getFieldNames.zipWithIndex.map {
      case (name, index) =>
        val field = postRowType.getFieldList.get(index)
        val relType = field.getValue
        val relName = field.getName
        val expression = UnresolvedFieldReference(relName)

        relType match {
          case _ if FlinkTypeFactory.isProctimeIndicatorType(relType) =>
            ProctimeAttribute(expression)
          case _ if FlinkTypeFactory.isRowtimeIndicatorType(relType) => RowtimeAttribute(expression)
          case _ if !relName.equals(name) => Alias(expression, name)
          case _ => expression
        }
    }.toArray[Expression]
  }

  private def getUniqueKeys(
      tEnv: StreamTableEnvironment,
      relNode: RelNode): util.Set[_ <: util.Set[String]] = {
    val rowType = relNode.getRowType
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(tEnv.getRelBuilder.getCluster.getMetadataQuery)
    val uniqueKeys = fmq.getUniqueKeys(relNode)
    if (uniqueKeys != null) {
      uniqueKeys.map { uniqueKey =>
        val keys = new util.HashSet[String]()
        uniqueKey.asList().foreach { idx =>
          keys.add(rowType.getFieldNames.get(idx))
        }
        keys
      }
    } else {
      null
    }
  }

}
