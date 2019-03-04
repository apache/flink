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
package org.apache.flink.table.plan.nodes

import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex.RexNode

import scala.collection.JavaConverters._

trait CommonJoin {

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

  private[flink] def joinTypeToString(joinType: JoinRelType) = {
    joinType match {
      case JoinRelType.INNER => "InnerJoin"
      case JoinRelType.LEFT=> "LeftOuterJoin"
      case JoinRelType.RIGHT => "RightOuterJoin"
      case JoinRelType.FULL => "FullOuterJoin"
    }
  }

  private[flink] def temporalJoinToString(
      inputType: RelDataType,
      joinCondition: RexNode,
      joinType: JoinRelType,
      expression: (RexNode, List[String], Option[List[RexNode]]) => String): String = {

    "Temporal" + joinToString(inputType, joinCondition, joinType, expression)
  }

  private[flink] def joinToString(
      inputType: RelDataType,
      joinCondition: RexNode,
      joinType: JoinRelType,
      expression: (RexNode, List[String], Option[List[RexNode]]) => String): String = {

    s"${joinTypeToString(joinType)}" +
      s"(where: (${joinConditionToString(inputType, joinCondition, expression)}), " +
      s"join: (${joinSelectionToString(inputType)}))"
  }

  private[flink] def joinExplainTerms(
      pw: RelWriter,
      inputType: RelDataType,
      joinCondition: RexNode,
      joinType: JoinRelType,
      expression: (RexNode, List[String], Option[List[RexNode]]) => String): RelWriter = {

    pw.item("where", joinConditionToString(inputType, joinCondition, expression))
      .item("join", joinSelectionToString(inputType))
      .item("joinType", joinTypeToString(joinType))
  }
}
