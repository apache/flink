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

package org.apache.flink.api.table.plan.nodes

import org.apache.calcite.rex._
import scala.collection.JavaConversions._

trait FlinkRel {

  private[flink] def getExpressionString(
    expr: RexNode,
    inFields: List[String],
    localExprsTable: Option[List[RexNode]]): String = {

    expr match {
      case i: RexInputRef => inFields.get(i.getIndex)
      case l: RexLiteral => l.toString
      case l: RexLocalRef if localExprsTable.isEmpty =>
        throw new IllegalArgumentException("Encountered RexLocalRef without local expression table")
      case l: RexLocalRef =>
        val lExpr = localExprsTable.get(l.getIndex)
        getExpressionString(lExpr, inFields, localExprsTable)
      case c: RexCall => {
        val op = c.getOperator.toString
        val ops = c.getOperands.map(getExpressionString(_, inFields, localExprsTable))
        s"$op(${ops.mkString(", ")})"
      }
      case _ => throw new IllegalArgumentException("Unknown expression type: " + expr)
    }
  }
}
