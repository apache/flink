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

package org.apache.flink.api.table.expressions.analysis

import org.apache.flink.api.table.expressions.{Naming, ResolvedFieldReference}
import org.apache.flink.api.table.input.{AdaptiveTableSource, TableSource}
import org.apache.flink.api.table.plan._

object FieldBacktracker {

  /**
   * Tracks a field back to its Root and returns its original name and AdaptiveTableSource
   * if possible.
   * This only happens if the field is forwarded unmodified. Renaming operations are reverted.
   *
   * @param op start operator
   * @param fieldName field name at start operator
   * @return original field name with corresponding AdaptiveTableSource or null
   */
  def resolveFieldNameAndTableSource(op: PlanNode, fieldName: String)
    : (AdaptiveTableSource, String) = {
    op match {
      case s@Select(input, selection) =>
        var resolvedField: (AdaptiveTableSource, String) = null
        // only follow unmodified fields
        selection.foreach {
          case ResolvedFieldReference(name, _) if name == fieldName =>
            resolvedField = resolveFieldNameAndTableSource(input, fieldName)
          case n@Naming(ResolvedFieldReference(oldName,_), newName) if newName == fieldName =>
            resolvedField = resolveFieldNameAndTableSource(input, oldName)
          case _ => // do nothing
        }
        resolvedField
      case Filter(input, _) =>
        resolveFieldNameAndTableSource(input, fieldName)
      case Join(leftPlanNode, rightPlanNode) =>
        if (leftPlanNode.outputFields.map(_._1).contains(fieldName)) {
          resolveFieldNameAndTableSource(leftPlanNode, fieldName)
        }
        else if (rightPlanNode.outputFields.map(_._1).contains(fieldName)) {
          resolveFieldNameAndTableSource(rightPlanNode, fieldName)
        }
        else {
          null
        }
      case As(input, newFieldNames) =>
        val oldName = input.outputFields(newFieldNames.indexOf(fieldName))._1
        resolveFieldNameAndTableSource(input, oldName)
      case GroupBy(input, fields) if fields.filter(_.isInstanceOf[ResolvedFieldReference])
        .map(_.name).contains(fieldName) =>
        resolveFieldNameAndTableSource(input, fieldName)
      // original AdaptiveTableSource name can be resolved
      case r@Root(tableSource: AdaptiveTableSource, outputFields) if outputFields.map(_._1)
        .contains(fieldName) =>
        (tableSource, fieldName)
      case _ => null
    }
  }
}
