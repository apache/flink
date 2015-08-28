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

import org.apache.flink.api.table.expressions.{ResolvedFieldReference,
UnresolvedFieldReference, Expression}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table._

import scala.collection.mutable

import org.apache.flink.api.table.trees.Rule

/**
 * Rule that resolved field references. This rule verifies that field references point to existing
 * fields of the input operation and creates [[ResolvedFieldReference]]s that hold the field
 * [[TypeInformation]] in addition to the field name.
 */
class ResolveFieldReferences(inputFields: Seq[(String, TypeInformation[_])])
  extends Rule[Expression] {

  def apply(expr: Expression) = {
    val errors = mutable.MutableList[String]()

    val result = expr.transformPost {
      case fe@UnresolvedFieldReference(fieldName) =>
        inputFields.find { _._1 == fieldName } match {
          case Some((_, tpe)) => ResolvedFieldReference(fieldName, tpe)

          case None =>
            errors +=
              s"Field '$fieldName' is not valid for input fields ${inputFields.mkString(",")}"
            fe
        }
    }

    if (errors.length > 0) {
      throw new ExpressionException(
        s"""Invalid expression "$expr": ${errors.mkString(" ")}""")
    }

    result

  }
}
