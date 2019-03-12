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
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.typeutils.TypeCoercion
import org.apache.flink.table.validate._

case class Cast(child: PlannerExpression, resultType: TypeInformation[_])
  extends UnaryExpression {

  override def toString = s"$child.cast($resultType)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    val typeFactory = relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val childRexNode = child.toRexNode
    relBuilder
      .getRexBuilder
      // we use abstract cast here because RelBuilder.cast() has to many side effects
      .makeAbstractCast(
        typeFactory.createTypeFromTypeInfo(resultType, childRexNode.getType.isNullable),
        childRexNode)
  }

  override private[flink] def makeCopy(anyRefs: Array[AnyRef]): this.type = {
    val child: PlannerExpression = anyRefs.head.asInstanceOf[PlannerExpression]
    copy(child, resultType).asInstanceOf[this.type]
  }

  override private[flink] def validateInput(): ValidationResult = {
    if (TypeCoercion.canCast(child.resultType, resultType)) {
      ValidationSuccess
    } else {
      ValidationFailure(s"Unsupported cast from ${child.resultType} to $resultType")
    }
  }
}
