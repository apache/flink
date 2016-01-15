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
package org.apache.flink.api.table.expressions

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.table.ExpressionException


abstract sealed class Aggregation extends UnaryExpression { self: Product =>
  def typeInfo = {
    child.typeInfo match {
      case BasicTypeInfo.LONG_TYPE_INFO => // ok
      case BasicTypeInfo.INT_TYPE_INFO =>
      case BasicTypeInfo.DOUBLE_TYPE_INFO =>
      case BasicTypeInfo.FLOAT_TYPE_INFO =>
      case BasicTypeInfo.BYTE_TYPE_INFO =>
      case BasicTypeInfo.SHORT_TYPE_INFO =>
      case _ =>
      throw new ExpressionException(s"Unsupported type ${child.typeInfo} for " +
        s"aggregation $this. Only numeric data types supported.")
    }
    child.typeInfo
  }

  override def toString = s"Aggregate($child)"
}

case class Sum(child: Expression) extends Aggregation {
  override def toString = s"($child).sum"
}

case class Min(child: Expression) extends Aggregation {
  override def toString = s"($child).min"
}

case class Max(child: Expression) extends Aggregation {
  override def toString = s"($child).max"
}

case class Count(child: Expression) extends Aggregation {
  override def toString = s"($child).count"
}

case class Avg(child: Expression) extends Aggregation {
  override def toString = s"($child).avg"
}
