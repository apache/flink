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

package org.apache.flink.table.plan.util

import org.apache.flink.streaming.api.bundle.CountCoBundleTrigger
import org.apache.flink.table.api.{TableConfig, TableConfigOptions, TableException}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.errorcode.TableErrors
import org.apache.flink.table.plan.FlinkJoinRelType

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.mapping.IntPair

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object JoinUtil {

  private[flink] def joinSelectionToString(inputType: RelDataType): String = {
    inputType.getFieldNames.asScala.toList.mkString(", ")
  }

  private[flink] def joinConditionToString(
      inputType: RelDataType,
      joinCondition: RexNode,
      expression: (RexNode, List[String], Option[List[RexNode]]) => String): String = {
    val inFields = inputType.getFieldNames.asScala.toList
    expression(joinCondition, inFields, None)
  }

  private[flink] def joinTypeToString(joinType: FlinkJoinRelType) = {
    joinType match {
      case FlinkJoinRelType.INNER => "InnerJoin"
      case FlinkJoinRelType.LEFT=> "LeftOuterJoin"
      case FlinkJoinRelType.RIGHT => "RightOuterJoin"
      case FlinkJoinRelType.FULL => "FullOuterJoin"
      case FlinkJoinRelType.SEMI => "LeftSemiJoin"
      case FlinkJoinRelType.ANTI => "LeftAntiJoin"
    }
  }

  private[flink] def joinToString(
      inputType: RelDataType,
      joinCondition: RexNode,
      joinType: FlinkJoinRelType,
      expression: (RexNode, List[String], Option[List[RexNode]]) => String): String = {
    s"${joinTypeToString(joinType)}" +
      s"(where: (${joinConditionToString(inputType, joinCondition, expression)}), " +
      s"join: (${joinSelectionToString(inputType)}))"
  }

  private[flink] def joinExplainTerms(
      pw: RelWriter,
      inputType: RelDataType,
      joinCondition: RexNode,
      joinType: FlinkJoinRelType,
      joinRowType: RelDataType,
      expression: (RexNode, List[String], Option[List[RexNode]]) => String): RelWriter = {
    pw.item("where", joinConditionToString(inputType, joinCondition, expression))
      .item("join", joinSelectionToString(joinRowType))
      .item("joinType", joinTypeToString(joinType))
  }

  /**
    * Check and get left and right keys.
    */
  private[flink] def checkAndGetKeys(
      keyPairs: List[IntPair],
      left: RelNode,
      right: RelNode,
      allowEmpty: Boolean = false): (ArrayBuffer[Int], ArrayBuffer[Int]) = {
    // get the equality keys
    val leftKeys = ArrayBuffer.empty[Int]
    val rightKeys = ArrayBuffer.empty[Int]
    if (keyPairs.isEmpty) {
      if (allowEmpty) {
        (leftKeys, rightKeys)
      } else {
        throw new TableException(
          TableErrors.INST.sqlJoinEqualConditionNotFound(
            s"\tleft: ${left.toString}\n\tright: ${right.toString}"))
      }
    } else {
      // at least one equality expression
      val leftFields = left.getRowType.getFieldList
      val rightFields = right.getRowType.getFieldList

      keyPairs.foreach(pair => {
        val leftKeyType = leftFields.get(pair.source).getType.getSqlTypeName
        val rightKeyType = rightFields.get(pair.target).getType.getSqlTypeName

        // check if keys are compatible
        if (leftKeyType == rightKeyType) {
          // add key pair
          leftKeys.add(pair.source)
          rightKeys.add(pair.target)
        } else {
          throw new TableException(
            TableErrors.INST.sqlJoinEqualJoinOnIncompatibleTypes(
              s"\tLeft: ${left.toString}\n\tright: ${right.toString}"))
        }
      })
      (leftKeys, rightKeys)
    }
  }

  def getMiniBatchTrigger(tableConfig: TableConfig): CountCoBundleTrigger[BaseRow, BaseRow] = {
    new CountCoBundleTrigger[BaseRow, BaseRow](
      tableConfig.getConf.getLong(TableConfigOptions.SQL_EXEC_MINIBATCH_SIZE))
  }
}
