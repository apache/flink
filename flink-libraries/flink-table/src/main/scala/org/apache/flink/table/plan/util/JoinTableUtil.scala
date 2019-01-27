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

import org.apache.calcite.rex.{RexNode, RexProgram}

import scala.collection.JavaConversions._

object JoinTableUtil {

  def isDeterministic(
      calcProgram: Option[RexProgram],
      period: Option[RexNode],
      joinCondition: Option[RexNode]): Boolean = {
    calcProgram match {
      case Some(program) =>
        if (program.getCondition != null) {
          val condition = program.expandLocalRef(program.getCondition)
          if (!FlinkRexUtil.isDeterministicOperator(condition)) {
            return false
          }
        }
        val projection = program.getProjectList.map(program.expandLocalRef)
        if (!projection.forall(p => FlinkRexUtil.isDeterministicOperator(p))) {
          return false
        }
      case _ => // do nothing
    }
    period match {
      case Some(p) =>
        if (!FlinkRexUtil.isDeterministicOperator(p)) {
          return false
        }
      case _ => // do nothing
    }
    joinCondition match {
      case Some(condition) => FlinkRexUtil.isDeterministicOperator(condition)
      case _ => true
    }
  }

}
