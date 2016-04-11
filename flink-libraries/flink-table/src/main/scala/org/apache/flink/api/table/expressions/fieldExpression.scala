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

case class UnresolvedFieldReference(override val name: String) extends LeafExpression {
  override def toString = "\"" + name

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.field(name)
  }
}

case class ResolvedFieldReference(override val name: String) extends LeafExpression {
  override def toString = s"'$name"
}

case class Naming(child: Expression, override val name: String) extends UnaryExpression {
  override def toString = s"$child as '$name"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.alias(child.toRexNode, name)
  }

  override def makeCopy(anyRefs: Seq[AnyRef]): this.type = {
    val child: Expression = anyRefs.head.asInstanceOf[Expression]
    copy(child, name).asInstanceOf[this.type]
  }
}
