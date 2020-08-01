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

package org.apache.flink.table.planner.expressions

import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.table.planner.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.planner.validate.{ValidationFailure, ValidationSuccess}

trait WindowProperty {

  def toNamedWindowProperty(name: String): NamedWindowProperty

  def resultType: TypeInformation[_]

}

abstract class AbstractWindowProperty(child: PlannerExpression)
  extends UnaryExpression
  with WindowProperty {

  override def toString = s"WindowProperty($child)"

  override private[flink] def validateInput() =
    if (child.isInstanceOf[WindowReference]) {
      ValidationSuccess
    } else {
      ValidationFailure("Child must be a window reference.")
    }

  def toNamedWindowProperty(name: String): NamedWindowProperty = NamedWindowProperty(name, this)
}

case class WindowStart(child: PlannerExpression) extends AbstractWindowProperty(child) {

  override def resultType = SqlTimeTypeInfo.TIMESTAMP

  override def toString: String = s"start($child)"
}

case class WindowEnd(child: PlannerExpression) extends AbstractWindowProperty(child) {

  override def resultType = SqlTimeTypeInfo.TIMESTAMP

  override def toString: String = s"end($child)"
}
