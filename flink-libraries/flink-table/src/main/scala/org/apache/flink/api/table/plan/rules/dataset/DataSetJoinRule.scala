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

package org.apache.flink.api.table.plan.rules.dataset

import org.apache.calcite.plan.{RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelDataTypeField, RelDataType}
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rex.{RexCall, RexInputRef}
import org.apache.calcite.sql.SqlKind
import org.apache.flink.api.table.plan.PlanGenException
import org.apache.flink.api.table.plan.nodes.dataset.{DataSetConvention, DataSetJoin}
import org.apache.flink.api.table.plan.nodes.logical.{FlinkConvention, FlinkJoin}
import org.apache.flink.api.table.plan.TypeConverter._

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class DataSetJoinRule
  extends ConverterRule(
    classOf[FlinkJoin],
    FlinkConvention.INSTANCE,
    DataSetConvention.INSTANCE,
    "DataSetJoinRule")
{

  def convert(rel: RelNode): RelNode = {
    val join: FlinkJoin = rel.asInstanceOf[FlinkJoin]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(DataSetConvention.INSTANCE)
    val convLeft: RelNode = RelOptRule.convert(join.getInput(0), DataSetConvention.INSTANCE)
    val convRight: RelNode = RelOptRule.convert(join.getInput(1), DataSetConvention.INSTANCE)

    val joinKeys = getJoinKeys(join)

    // There would be a FlinkProject after FlinkJoin to handle the output fields afterward join,
    // so we do not need JoinFunction here by now.
    new DataSetJoin(
      rel.getCluster,
      traitSet,
      convLeft,
      convRight,
      rel.getRowType,
      join.toString,
      joinKeys._1,
      joinKeys._2,
      sqlJoinTypeToFlinkJoinType(join.getJoinType),
      null,
      null)
  }

  private def getJoinKeys(join: FlinkJoin): (Array[Int], Array[Int]) = {
    val joinKeys = ArrayBuffer.empty[(Int, Int)]
    parseJoinRexNode(join.getCondition.asInstanceOf[RexCall], joinKeys)

    val joinedRowType= join.getRowType
    val leftRowType = join.getLeft.getRowType
    val rightRowType = join.getRight.getRowType

    // The fetched join key index from Calcite is based on joined row type, we need
    // the join key index based on left/right input row type.
    val joinKeyPairs: ArrayBuffer[(Int, Int)] = joinKeys.map {
      case (first, second) =>
        var leftIndex = findIndexInSingleInput(first, joinedRowType, leftRowType)
        if (leftIndex == -1) {
          leftIndex = findIndexInSingleInput(second, joinedRowType, leftRowType)
          if (leftIndex == -1) {
            throw new PlanGenException("Invalid join condition, could not find " +
              joinedRowType.getFieldNames.get(first) + " and " +
              joinedRowType.getFieldNames.get(second) + " in left table")
          }
          val rightIndex = findIndexInSingleInput(first, joinedRowType, rightRowType)
          if (rightIndex == -1) {
            throw new PlanGenException("Invalid join condition could not find " +
              joinedRowType.getFieldNames.get(first) + " in right table")
          }
          (leftIndex, rightIndex)
        } else {
          val rightIndex = findIndexInSingleInput(second, joinedRowType, rightRowType)
          if (rightIndex == -1) {
            throw new PlanGenException("Invalid join condition could not find " +
              joinedRowType.getFieldNames.get(second) + " in right table")
          }
          (leftIndex, rightIndex)
        }
    }

    val joinKeysPair = joinKeyPairs.unzip

    (joinKeysPair._1.toArray, joinKeysPair._2.toArray)
  }

  // Parse the join condition recursively, find all the join keys' index.
  private def parseJoinRexNode(condition: RexCall, joinKeys: ArrayBuffer[(Int, Int)]): Unit = {
    condition.getOperator.getKind match {
      case SqlKind.AND =>
        condition.getOperands.foreach {
          operand => parseJoinRexNode(operand.asInstanceOf[RexCall], joinKeys)
        }
      case SqlKind.EQUALS =>
        val operands = condition.getOperands
        val leftIndex = operands(0).asInstanceOf[RexInputRef].getIndex
        val rightIndex = operands(1).asInstanceOf[RexInputRef].getIndex
        joinKeys += (leftIndex -> rightIndex)
      case _ =>
        // Do not support operands like OR in join condition due to the limitation
        // of current Flink JoinOperator implementation.
        throw new PlanGenException("Do not support operands other than " +
          "AND and Equals in join condition now.")
    }
  }

  // Find the field index of input row type.
  private def findIndexInSingleInput(
      globalIndex: Int,
      joinedRowType: RelDataType,
      inputRowType: RelDataType): Int = {

    val fieldType: RelDataTypeField = joinedRowType.getFieldList.get(globalIndex)
    inputRowType.getFieldList.zipWithIndex.foreach {
      case (inputFieldType, index) =>
        if (inputFieldType.getName.equals(fieldType.getName)
          && inputFieldType.getType.equals(fieldType.getType)) {

          return index
        }
    }

    // return -1 if match none field of input row type.
    -1
  }
}

object DataSetJoinRule {
  val INSTANCE: RelOptRule = new DataSetJoinRule
}
