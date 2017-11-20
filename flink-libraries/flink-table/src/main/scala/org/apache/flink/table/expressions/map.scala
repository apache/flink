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

import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{GenericTypeInfo, MapTypeInfo}
import org.apache.flink.table.calcite.{FlinkRelBuilder, FlinkTypeFactory}
import org.apache.flink.table.plan.schema.MapRelDataType
import org.apache.flink.table.validate.{ValidationFailure, ValidationResult, ValidationSuccess}

import scala.collection.JavaConverters._

case class MapConstructor(elements: Seq[Expression]) extends Expression {
  override private[flink] def children: Seq[Expression] = elements

  private[flink] var mapResultType: TypeInformation[_] = new MapTypeInfo(
    new GenericTypeInfo[AnyRef](classOf[AnyRef]),
    new GenericTypeInfo[AnyRef](classOf[AnyRef]))

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    val typeFactory = relBuilder.asInstanceOf[FlinkRelBuilder].getTypeFactory
    val relDataType = typeFactory.createMapType(
      typeFactory.createTypeFromTypeInfo(elements.head.resultType, isNullable = true),
      typeFactory.createTypeFromTypeInfo(elements.last.resultType, isNullable = true)
    )
    val values = elements.map(_.toRexNode).toList.asJava
    relBuilder
      .getRexBuilder
      .makeCall(relDataType, SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, values)
  }

  override def toString = s"map(${elements
    .grouped(2)
    .map(x => s"[${x.mkString(": ")}]").mkString(", ")})"

  override private[flink] def resultType: TypeInformation[_] = new MapTypeInfo(
    elements.head.resultType,
    elements.last.resultType
  )

  override private[flink] def validateInput(): ValidationResult = {
    if (elements.isEmpty) {
      return ValidationFailure("Empty maps are not supported yet.")
    }
    if (elements.size % 2 != 0) {
      return ValidationFailure("Maps must have even number of elements to form key value pairs.")
    }
    if (!elements.grouped(2).forall(_.head.resultType == elements.head.resultType)) {
      return ValidationFailure("Not all key elements of the map literal have the same type.")
    }
    if (!elements.grouped(2).forall(_.last.resultType == elements.last.resultType)) {
      return ValidationFailure("Not all value elements of the map literal have the same type.")
    }
    ValidationSuccess
  }
}

