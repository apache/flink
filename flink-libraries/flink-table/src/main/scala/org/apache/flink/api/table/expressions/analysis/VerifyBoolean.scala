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

import org.apache.flink.api.table.expressions.{NopExpression, Expression}
import org.apache.flink.api.table.trees.Rule
import org.apache.flink.api.table.{_}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo

import scala.collection.mutable

/**
 * [[Rule]] that verifies that the result type of an [[Expression]] is Boolean. This is required
 * for filter/join predicates.
 */
class VerifyBoolean extends Rule[Expression] {

  def apply(expr: Expression) = {
    if (!expr.isInstanceOf[NopExpression] && expr.typeInfo != BasicTypeInfo.BOOLEAN_TYPE_INFO) {
      throw new ExpressionException(s"Expression $expr of type ${expr.typeInfo} is not boolean.")
    }

    expr
  }
}
