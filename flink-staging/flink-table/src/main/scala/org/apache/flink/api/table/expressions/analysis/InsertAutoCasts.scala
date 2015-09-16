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
import org.apache.flink.api.common.typeinfo.{IntegerTypeInfo, BasicTypeInfo}
import org.apache.flink.api.table.trees.Rule

/**
 * [[Rule]] that adds casts in arithmetic operations.
 */
class InsertAutoCasts extends Rule[Expression] {

  def apply(expr: Expression) = {
    val result = expr.transformPost {

      case plus@Plus(o1, o2) =>
        // Plus is special case since we can cast anything to String for String concat
        if (o1.typeInfo != o2.typeInfo && o1.typeInfo.isBasicType && o2.typeInfo.isBasicType) {
          if (o1.typeInfo.asInstanceOf[BasicTypeInfo[_]].shouldAutocastTo(
            o2.typeInfo.asInstanceOf[BasicTypeInfo[_]])) {
            Plus(Cast(o1, o2.typeInfo), o2)
          } else if (o2.typeInfo.asInstanceOf[BasicTypeInfo[_]].shouldAutocastTo(
            o1.typeInfo.asInstanceOf[BasicTypeInfo[_]])) {
            Plus(o1, Cast(o2, o1.typeInfo))
          } else if (o1.typeInfo == BasicTypeInfo.STRING_TYPE_INFO) {
            Plus(o1, Cast(o2, BasicTypeInfo.STRING_TYPE_INFO))
          } else if (o2.typeInfo == BasicTypeInfo.STRING_TYPE_INFO) {
            Plus(Cast(o1, BasicTypeInfo.STRING_TYPE_INFO), o2)
          } else {
            plus
          }
        } else {
          plus
        }

      case ba: BinaryExpression if ba.isInstanceOf[BinaryArithmetic] ||
        ba.isInstanceOf[BinaryComparison] =>
        val o1 = ba.left
        val o2 = ba.right
        if (o1.typeInfo != o2.typeInfo && o1.typeInfo.isBasicType && o2.typeInfo.isBasicType) {
          if (o1.typeInfo.asInstanceOf[BasicTypeInfo[_]].shouldAutocastTo(
            o2.typeInfo.asInstanceOf[BasicTypeInfo[_]])) {
            ba.makeCopy(Seq(Cast(o1, o2.typeInfo), o2))
          } else if (o2.typeInfo.asInstanceOf[BasicTypeInfo[_]].shouldAutocastTo(
            o1.typeInfo.asInstanceOf[BasicTypeInfo[_]])) {
            ba.makeCopy(Seq(o1, Cast(o2, o1.typeInfo)))
          } else {
            ba
          }
        } else {
          ba
        }

      case ba: BinaryExpression if ba.isInstanceOf[BitwiseBinaryArithmetic] =>
        val o1 = ba.left
        val o2 = ba.right
        if (o1.typeInfo != o2.typeInfo && o1.typeInfo.isInstanceOf[IntegerTypeInfo[_]] &&
          o2.typeInfo.isInstanceOf[IntegerTypeInfo[_]]) {
          if (o1.typeInfo.asInstanceOf[BasicTypeInfo[_]].shouldAutocastTo(
            o2.typeInfo.asInstanceOf[BasicTypeInfo[_]])) {
            ba.makeCopy(Seq(Cast(o1, o2.typeInfo), o2))
          } else if (o2.typeInfo.asInstanceOf[BasicTypeInfo[_]].shouldAutocastTo(
            o1.typeInfo.asInstanceOf[BasicTypeInfo[_]])) {
            ba.makeCopy(Seq(o1, Cast(o2, o1.typeInfo)))
          } else {
            ba
          }
        } else {
          ba
        }
    }

    result
  }
}
