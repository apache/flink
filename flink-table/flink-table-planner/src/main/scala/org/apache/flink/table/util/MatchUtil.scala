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

package org.apache.flink.table.util

import org.apache.calcite.rex.{RexCall, RexNode, RexPatternFieldRef}
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.plan.util.RexDefaultVisitor
import scala.collection.JavaConverters._

object MatchUtil {
  val ALL_PATTERN_VARIABLE = "*"

  class AggregationPatternVariableFinder extends RexDefaultVisitor[Option[String]] {

    override def visitPatternFieldRef(patternFieldRef: RexPatternFieldRef): Option[String] = Some(
      patternFieldRef.getAlpha)

    override def visitCall(call: RexCall): Option[String] = {
      if (call.operands.size() == 0) {
        Some(ALL_PATTERN_VARIABLE)
      } else {
        call.operands.asScala.map(n => n.accept(this)).reduce((op1, op2) => (op1, op2) match {
          case (None, None) => None
          case (x, None) => x
          case (None, x) => x
          case (Some(var1), Some(var2)) if var1.equals(var2) =>
            Some(var1)
          case _ =>
            throw new ValidationException(s"Aggregation must be applied to a single pattern " +
              s"variable. Malformed expression: $call")
        })
      }
    }

    override def visitNode(rexNode: RexNode): Option[String] = None
  }
}
