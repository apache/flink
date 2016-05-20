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

import org.apache.calcite.rex.RexNode
import org.apache.calcite.tools.RelBuilder

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.validate._

abstract class Ordering extends UnaryExpression {
  override def validateInput(): ExprValidationResult = {
    if (!child.isInstanceOf[NamedExpression]) {
      ValidationFailure(s"Sort should only based on field reference")
    } else {
      ValidationSuccess
    }
  }
}

case class Asc(child: Expression) extends Ordering {
  override def toString: String = s"($child).asc"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    child.toRexNode
  }

  override def resultType: TypeInformation[_] = child.resultType
}

case class Desc(child: Expression) extends Ordering {
  override def toString: String = s"($child).desc"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.desc(child.toRexNode)
  }

  override def resultType: TypeInformation[_] = child.resultType
}
