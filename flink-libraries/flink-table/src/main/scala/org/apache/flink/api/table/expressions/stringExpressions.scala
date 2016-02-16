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

import org.apache.flink.api.table.ExpressionException
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, IntegerTypeInfo}

case class Substring(
    str: Expression,
    beginIndex: Expression,
    endIndex: Expression) extends Expression {
  def typeInfo = {
    if (str.typeInfo != BasicTypeInfo.STRING_TYPE_INFO) {
      throw new ExpressionException(
        s"""Operand must be of type String in $this, is ${str.typeInfo}.""")
    }
    if (!beginIndex.typeInfo.isInstanceOf[IntegerTypeInfo[_]]) {
      throw new ExpressionException(
        s"""Begin index must be an integer type in $this, is ${beginIndex.typeInfo}.""")
    }
    if (!endIndex.typeInfo.isInstanceOf[IntegerTypeInfo[_]]) {
      throw new ExpressionException(
        s"""End index must be an integer type in $this, is ${endIndex.typeInfo}.""")
    }

    BasicTypeInfo.STRING_TYPE_INFO
  }

  override def children: Seq[Expression] = Seq(str, beginIndex, endIndex)
  override def toString = s"($str).substring($beginIndex, $endIndex)"
}
