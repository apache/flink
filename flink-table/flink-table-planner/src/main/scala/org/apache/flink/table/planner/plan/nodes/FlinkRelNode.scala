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

package org.apache.flink.table.planner.plan.nodes

import org.apache.flink.table.planner.plan.utils.ExpressionFormat.ExpressionFormat
import org.apache.flink.table.planner.plan.utils.{ExpressionFormat, FlinkRexUtil, RelDescriptionWriterImpl}

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex._

import java.io.{PrintWriter, StringWriter}

/**
  * Base class for flink relational expression.
  */
trait FlinkRelNode extends RelNode {

  /**
    * Returns a string which describes the detailed information of relational expression
    * with attributes which contribute to the plan output.
    *
    * This method leverages [[RelNode#explain]] with
    * [[org.apache.calcite.sql.SqlExplainLevel.EXPPLAN_ATTRIBUTES]] explain level to generate
    * the description.
    */
  def getRelDetailedDescription: String = {
    val sw = new StringWriter
    val pw = new PrintWriter(sw)
    val relWriter = new RelDescriptionWriterImpl(pw)
    this.explain(relWriter)
    sw.toString
  }

  private[flink] def getExpressionString(
      expr: RexNode,
      inFields: List[String],
      localExprsTable: Option[List[RexNode]]): String = {
    getExpressionString(expr, inFields, localExprsTable, ExpressionFormat.Prefix)
  }

  private[flink] def getExpressionString(
      expr: RexNode,
      inFields: List[String],
      localExprsTable: Option[List[RexNode]],
      expressionFormat: ExpressionFormat): String = {
    FlinkRexUtil.getExpressionString(expr, inFields, localExprsTable, expressionFormat)
  }
}



