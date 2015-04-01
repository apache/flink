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
package org.apache.flink.api.table.codegen

import org.apache.flink.api.table.tree.Expression
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.table.typeinfo.RowTypeInfo
import org.apache.flink.api.java.typeutils.{TupleTypeInfo, PojoTypeInfo}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo

/**
 * Base class for unary and binary result assembler code generators.
 */
abstract class GenerateResultAssembler[R](
    inputs: Seq[(String, CompositeType[_])],
    cl: ClassLoader)
  extends ExpressionCodeGenerator[R](inputs, cl = cl) {
  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._

  def createResult[T](
      resultTypeInfo: CompositeType[T],
      outputFields: Seq[Expression]): Tree = {

    val resultType = typeTermForTypeInfo(resultTypeInfo)

    val fieldsCode = outputFields.map(generateExpression)

    val block = resultTypeInfo match {
      case ri: RowTypeInfo =>
        val resultSetters: Seq[Tree] = fieldsCode.zipWithIndex map {
          case (fieldCode, i) =>
            q"""
              out.setField($i, { ..${fieldCode.code}; ${fieldCode.resultTerm} })
            """
        }

        q"""
          ..$resultSetters
          out
        """

      case pj: PojoTypeInfo[_] =>
        val resultSetters: Seq[Tree] = fieldsCode.zip(outputFields) map {
        case (fieldCode, expr) =>
          val fieldName = newTermName(expr.name)
          q"""
              out.$fieldName = { ..${fieldCode.code}; ${fieldCode.resultTerm} }
            """
        }

        q"""
          ..$resultSetters
          out
        """

      case tup: TupleTypeInfo[_] =>
        val resultSetters: Seq[Tree] = fieldsCode.zip(outputFields) map {
          case (fieldCode, expr) =>
            val fieldName = newTermName(expr.name)
            q"""
              out.$fieldName = { ..${fieldCode.code}; ${fieldCode.resultTerm} }
            """
        }

        q"""
          ..$resultSetters
          out
        """

      case cc: CaseClassTypeInfo[_] =>
        val resultFields: Seq[Tree] = fieldsCode map {
          fieldCode =>
            q"{ ..${fieldCode.code}; ${fieldCode.resultTerm}}"
        }
        q"""
          new $resultType(..$resultFields)
        """
    }

    block
  }

}
