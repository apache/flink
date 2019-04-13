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

package org.apache.flink.table.expressions

import org.apache.flink.table.`type`.{InternalType, InternalTypes, TimestampType}
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty

import java.util
import java.util.Collections

trait WindowProperty {

  def toNamedWindowProperty(name: String): NamedWindowProperty

  def resultType: InternalType

}

abstract class AbstractWindowProperty(child: Expression)
  extends Expression
  with WindowProperty {

  override def toString = s"WindowProperty($child)"

  override def accept[T](exprVisitor: ExpressionVisitor[T]): T =
    exprVisitor.visit(this)

  override def getChildren: util.List[Expression] = Collections.emptyList()

  def toNamedWindowProperty(name: String): NamedWindowProperty = NamedWindowProperty(name, this)
}

case class WindowStart(child: Expression) extends AbstractWindowProperty(child) {

  override def resultType: TimestampType = InternalTypes.TIMESTAMP

  override def toString: String = s"start($child)"
}

case class WindowEnd(child: Expression) extends AbstractWindowProperty(child) {

  override def resultType: TimestampType = InternalTypes.TIMESTAMP

  override def toString: String = s"end($child)"
}
