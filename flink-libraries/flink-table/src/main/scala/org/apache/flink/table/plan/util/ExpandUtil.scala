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

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexInputRef, RexLiteral, RexNode}

import java.util

import scala.collection.JavaConversions._

object ExpandUtil {

  def projectsToString(
      projects: util.List[util.List[RexNode]],
      inputRowType: RelDataType,
      outputRowType: RelDataType): String = {
    val inFieldNames = inputRowType.getFieldNames
    val outFieldNames = outputRowType.getFieldNames
    projects.map { project =>
      project.zipWithIndex.map {
        case (r: RexInputRef, i) =>
          val inputFieldName = inFieldNames.get(r.getIndex)
          val outputFieldName = outFieldNames.get(i)
          if (inputFieldName != outputFieldName) {
            s"$inputFieldName AS $outputFieldName"
          } else {
            outputFieldName
          }
        case (l: RexLiteral, i) => s"${l.getValue3} AS ${outFieldNames.get(i)}"
        case (_, i) => outFieldNames.get(i)
      }.mkString("{", ", ", "}")
    }.mkString(", ")
  }

  def isDeterministic(projects: util.List[util.List[RexNode]]): Boolean = {
    projects.forall(_.forall(r => FlinkRexUtil.isDeterministicOperator(r)))
  }

}
