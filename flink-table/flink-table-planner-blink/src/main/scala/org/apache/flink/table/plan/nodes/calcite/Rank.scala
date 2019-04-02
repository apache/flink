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

package org.apache.flink.table.plan.nodes.calcite

import org.apache.flink.table.api.TableException
import org.apache.flink.table.plan.util._

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelCollation, RelNode, RelWriter, SingleRel}
import org.apache.calcite.sql.SqlRankFunction
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.util.{ImmutableBitSet, NumberUtil}

import java.util

import scala.collection.JavaConversions._

/**
  * Relational expression that returns the rows in which the rank function value of each row
  * is in the given range.
  *
  * <p>NOTES: Different from [[org.apache.calcite.sql.fun.SqlStdOperatorTable.RANK]],
  * [[Rank]] is a Relational expression， not a window function.
  *
  * <p>[[Rank]] will output rank function value as its last column.
  *
  * <p>This RelNode only handles single rank function, is an optimization for some cases. e.g.
  * <ol>
  * <li>
  *   single rank function (on `OVER`) with filter in a SQL query statement
  * </li>
  * <li>
  *   `ORDER BY` with `LIMIT` in a SQL query statement
  *   (equivalent to `ROW_NUMBER` with filter and project)
  * </li>
  * </ol>
  *
  * @param cluster        cluster that this relational expression belongs to
  * @param traitSet       the traits of this rel
  * @param input          input relational expression
  * @param rankFunction   rank function, including: CUME_DIST, DENSE_RANK, PERCENT_RANK, RANK,
  *                       ROW_NUMBER
  * @param partitionKey   partition keys (may be empty)
  * @param sortCollation  order keys for rank function
  * @param rankRange      the expected range of rank function value
  */
abstract class Rank(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    val rankFunction: SqlRankFunction,
    val partitionKey: ImmutableBitSet,
    val sortCollation: RelCollation,
    val rankRange: RankRange)
  extends SingleRel(cluster, traitSet, input) {

  rankRange match {
    case r: ConstantRankRange =>
      if (r.rankEnd <= 0) {
        throw new TableException(s"Rank end can't smaller than zero. The rank end is ${r.rankEnd}")
      }
      if (r.rankStart > r.rankEnd) {
        throw new TableException(
          s"Rank start '${r.rankStart}' can't greater than rank end '${r.rankEnd}'.")
      }
    case v: VariableRankRange =>
      if (v.rankEndIndex < 0) {
        throw new TableException(s"Rank end index can't smaller than zero.")
      }
      if (v.rankEndIndex >= input.getRowType.getFieldCount) {
        throw new TableException(s"Rank end index can't greater than input field count.")
      }
  }

  override def deriveRowType(): RelDataType = {
    val typeFactory = cluster.getRexBuilder.getTypeFactory
    val typeBuilder = typeFactory.builder()
    input.getRowType.getFieldList.foreach(typeBuilder.add)
    // rank function column is always the last column, and its type is BIGINT NOT NULL
    val allFieldNames = new util.HashSet[String]()
    allFieldNames.addAll(input.getRowType.getFieldNames)
    val rankFieldName = FlinkRelOptUtil.buildUniqueFieldName(allFieldNames, "rk")
    val bigIntType = typeFactory.createSqlType(SqlTypeName.BIGINT)
    typeBuilder.add(rankFieldName, typeFactory.createTypeWithNullability(bigIntType, false))
    typeBuilder.build()
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val select = getRowType.getFieldNames.zipWithIndex.map {
      case (name, idx) => s"$name=$$$idx"
    }.mkString(", ")
    super.explainTerms(pw)
      .item("rankFunction", rankFunction)
      .item("rankRange", rankRange.toString())
      .item("partitionBy", partitionKey.map(i => s"$$$i").mkString(","))
      .item("orderBy", RelExplainUtil.collationToString(sortCollation))
      .item("select", select)
  }

  override def estimateRowCount(mq: RelMetadataQuery): Double = {
    val countPerGroup = FlinkRelMdUtil.getRankRangeNdv(rankRange)
    if (partitionKey.isEmpty) {
      // only one group
      countPerGroup
    } else {
      val inputRowCount = mq.getRowCount(input)
      val numOfGroup = mq.getDistinctRowCount(input, partitionKey, null)
      if (numOfGroup != null) {
        NumberUtil.min(numOfGroup * countPerGroup, inputRowCount)
      } else {
        NumberUtil.min(mq.getRowCount(input) * 0.1, inputRowCount)
      }
    }
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCount = mq.getRowCount(input)
    val cpuCost = rowCount
    planner.getCostFactory.makeCost(rowCount, cpuCost, 0)
  }

}

sealed trait RankRange extends Serializable {
  def toString(inputFieldNames: Seq[String]): String
}

/** [[ConstantRankRangeWithoutEnd]] is a RankRange which not specify RankEnd. */
case class ConstantRankRangeWithoutEnd(rankStart: Long) extends RankRange {
  override def toString(inputFieldNames: Seq[String]): String = this.toString

  override def toString: String = s"rankStart=$rankStart"
}

/** rankStart and rankEnd are inclusive, rankStart always start from one. */
case class ConstantRankRange(rankStart: Long, rankEnd: Long) extends RankRange {

  override def toString(inputFieldNames: Seq[String]): String = this.toString

  override def toString: String = s"rankStart=$rankStart, rankEnd=$rankEnd"
}

/** changing rank limit depends on input */
case class VariableRankRange(rankEndIndex: Int) extends RankRange {
  override def toString(inputFieldNames: Seq[String]): String = {
    s"rankEnd=${inputFieldNames(rankEndIndex)}"
  }

  override def toString: String = {
    s"rankEnd=$$$rankEndIndex"
  }
}
