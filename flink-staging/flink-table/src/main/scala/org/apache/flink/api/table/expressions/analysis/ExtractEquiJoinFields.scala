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

import org.apache.flink.api.table.expressions._
import org.apache.flink.api.common.typeutils.CompositeType

import scala.collection.mutable

/**
 * Equi-join field extractor for Join Predicates and CoGroup predicates. The result is a modified
 * expression without the equi-join predicates together with indices of the join fields
 * from both the left and right input.
 */
object ExtractEquiJoinFields {
  def apply(leftType: CompositeType[_], rightType: CompositeType[_], predicate: Expression) = {

    val joinFieldsLeft = mutable.MutableList[Int]()
    val joinFieldsRight = mutable.MutableList[Int]()

    val equiJoinExprs = mutable.MutableList[EqualTo]()
    // First get all `===` expressions that are not below an `Or`
    predicate.transformPre {
      case or@Or(_, _) => NopExpression()
      case eq@EqualTo(le: ResolvedFieldReference, re: ResolvedFieldReference) =>
        if (leftType.hasField(le.name) && rightType.hasField(re.name)) {
          joinFieldsLeft += leftType.getFieldIndex(le.name)
          joinFieldsRight += rightType.getFieldIndex(re.name)
        } else if (leftType.hasField(re.name) && rightType.hasField(le.name)) {
          joinFieldsLeft += leftType.getFieldIndex(re.name)
          joinFieldsRight += rightType.getFieldIndex(le.name)
        } else {
          // not an equi-join predicate
        }
        equiJoinExprs += eq
        eq
    }

    // then remove the equi join expressions from the predicate
    val resultExpr = predicate.transformPost {
      // For OR, we can eliminate the OR since the equi join
      // predicate is evaluated before the expression is evaluated
      case or@Or(NopExpression(), _) => NopExpression()
      case or@Or(_, NopExpression()) => NopExpression()
      // For AND we replace it with the other expression, since the
      // equi join predicate will always be true
      case and@And(NopExpression(), other) => other
      case and@And(other, NopExpression()) => other
      case eq : EqualTo if equiJoinExprs.contains(eq) =>
        NopExpression()
    }

    (resultExpr, joinFieldsLeft.toArray, joinFieldsRight.toArray)
  }
}
