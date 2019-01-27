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

package org.apache.flink.table.plan.metadata

import org.apache.flink.table.plan.metadata.FlinkMetadata.ColumnOriginNullCount
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalSnapshot
import org.apache.flink.table.plan.schema.FlinkRelOptTable
import org.apache.flink.util.Preconditions

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rex.{RexInputRef, RexLiteral, RexNode}

import java.lang.{Double => JDouble}
import java.util

import scala.collection.JavaConversions._

/**
  * FlinkRelMdColumnOriginNullCount supplies a default implementation of
  * [[FlinkRelMetadataQuery.getColumnOriginNullCount]] for the standard logical algebra.
  * If there is null, then return the original stats. If there is no null, then return 0.
  * If don't know, then return null.
  */
class FlinkRelMdColumnOriginNullCount private extends MetadataHandler[ColumnOriginNullCount] {

  override def getDef: MetadataDef[ColumnOriginNullCount] = FlinkMetadata.ColumnOriginNullCount.DEF

  def getColumnOriginNullCount(ts: TableScan, mq: RelMetadataQuery, index: Int): JDouble = {
    Preconditions.checkArgument(mq.isInstanceOf[FlinkRelMetadataQuery])
    val relOptTable = ts.getTable.asInstanceOf[FlinkRelOptTable]
    val fieldNames = relOptTable.getRowType.getFieldNames
    Preconditions.checkArgument(index >= 0 && index < fieldNames.size())
    val fieldName = fieldNames.get(index)
    val statistic = relOptTable.getFlinkStatistic
    val colStats = statistic.getColumnStats(fieldName)
    if (colStats != null && colStats.nullCount != null) colStats.nullCount.toDouble else null
  }

  def getColumnOriginNullCount(project: Project, mq: RelMetadataQuery, index: Int): JDouble = {
    getColumnOriginNullAfterProjects(project.getInput, project.getProjects, mq, index)
  }

  def getColumnOriginNullCount(calc: Calc, mq: RelMetadataQuery, index: Int): JDouble = {
    val program = calc.getProgram
    if (program.getCondition == null) {
      getColumnOriginNullAfterProjects(
        calc.getInput, program.getProjectList.map(program.expandLocalRef), mq, index)
    } else {
      null
    }
  }

  def getColumnOriginNullCount(
      snapshot: FlinkLogicalSnapshot,
      mq: RelMetadataQuery,
      index: Int): JDouble = null

  private def getColumnOriginNullAfterProjects(
      input: RelNode,
      projects: util.List[RexNode],
      mq: RelMetadataQuery,
      index: Int): JDouble = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    projects.get(index) match {
      case inputRef: RexInputRef => fmq.getColumnNullCount(input, inputRef.getIndex)
      case literal: RexLiteral => if (literal.isNull) 1D else 0D
      case _ => null
    }
  }

  def getColumnOriginNullCount(rel: Join, mq: RelMetadataQuery, index: Int): JDouble = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    if (rel.getJoinType == JoinRelType.INNER) {
      val leftFieldCnt = rel.getLeft.getRowType.getFieldCount
      val filterNulls = new util.ArrayList[java.lang.Boolean]
      val cond = JoinInfo.of(rel.getLeft, rel.getRight, rel.getCondition, filterNulls)
      val keys = cond.leftKeys ++ cond.rightKeys.map(_ + leftFieldCnt)

      def filterNull(): Boolean = {
        var i = keys.indexOf(index)
        if (i >= cond.leftKeys.length) {
          i = i - cond.leftKeys.length
        }
        filterNulls(i)
      }

      if (keys.contains(index) && filterNull()) {
        0D
      } else {
        // As same with its children, there may be better ways to estimate it.
        // With JoinNullFilterPushdownRule, we can generate more NotNullFilters.
        if (index < leftFieldCnt) {
          fmq.getColumnOriginNullCount(rel.getLeft, index)
        } else {
          fmq.getColumnOriginNullCount(rel.getRight, index - leftFieldCnt)
        }
      }
    } else {
      null
    }
  }

  def getColumnOriginNullCount(rel: RelNode, mq: RelMetadataQuery, index: Int): JDouble = null
}

object FlinkRelMdColumnOriginNullCount {

  private val INSTANCE = new FlinkRelMdColumnOriginNullCount

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    FlinkMetadata.ColumnOriginNullCount.METHOD, INSTANCE)

}
