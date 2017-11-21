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
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.table.typeutils.TypeCheckUtils.{isArray, isMap}
import org.apache.flink.table.validate.{ValidationFailure, ValidationResult, ValidationSuccess}

case class Cardinality(container: Expression) extends Expression {

  override private[flink] def children: Seq[Expression] = Seq(container)

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder
      .getRexBuilder
      .makeCall(SqlStdOperatorTable.CARDINALITY, container.toRexNode)
  }

  override def toString = s"($container).cardinality()"

  override private[flink] def resultType = BasicTypeInfo.INT_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult = {
    container.resultType match {
      case mti: TypeInformation[_] if isMap(mti) => ValidationSuccess
      case ati: TypeInformation[_] if isArray(ati) => ValidationSuccess
      case other@_ => ValidationFailure(s"Array expected but was '$other'.")
    }
  }
}

