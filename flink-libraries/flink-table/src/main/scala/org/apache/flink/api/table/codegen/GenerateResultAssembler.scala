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

import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.api.table.TableConfig
import org.apache.flink.api.table.expressions.Expression
import org.apache.flink.api.table.typeinfo.RowTypeInfo

/**
 * Base class for unary and binary result assembler code generators.
 */
abstract class GenerateResultAssembler[R](
    inputs: Seq[(String, CompositeType[_])],
    cl: ClassLoader,
    config: TableConfig)
  extends ExpressionCodeGenerator[R](inputs, cl = cl, config) {

  def reuseCode[A](resultTypeInfo: CompositeType[A]) = {
      val resultTpe = typeTermForTypeInfo(resultTypeInfo)
      resultTypeInfo match {
        case pj: PojoTypeInfo[_] =>
          super.reuseMemberCode() +
            s"$resultTpe out = new ${pj.getTypeClass.getCanonicalName}();"

        case row: RowTypeInfo =>
          super.reuseMemberCode() +
            s"org.apache.flink.api.table.Row out =" +
            s" new org.apache.flink.api.table.Row(${row.getArity});"

        case _ => ""
      }
  }

  def createResult[T](
      resultTypeInfo: CompositeType[T],
      outputFields: Seq[Expression],
      result: String => String): String = {

    val resultType = typeTermForTypeInfo(resultTypeInfo)

    val fieldsCode = outputFields.map(generateExpression)

    val block = resultTypeInfo match {
      case ri: RowTypeInfo =>
        val resultSetters: String = fieldsCode.zipWithIndex map {
          case (fieldCode, i) =>
            s"""
              |${fieldCode.code}
              |out.setField($i, ${fieldCode.resultTerm});
            """.stripMargin
        } mkString("\n")

        s"""
          |$resultSetters
          |${result("out")}
        """.stripMargin

      case pj: PojoTypeInfo[_] =>
        val resultSetters: String = fieldsCode.zip(outputFields) map {
        case (fieldCode, expr) =>
          val fieldName = expr.name
          s"""
            |${fieldCode.code}
            |out.$fieldName = ${fieldCode.resultTerm};
          """.stripMargin
        } mkString("\n")

        s"""
          |$resultSetters
          |${result("out")}
        """.stripMargin

      case tup: TupleTypeInfo[_] =>
        val resultSetters: String = fieldsCode.zip(outputFields) map {
          case (fieldCode, expr) =>
            val fieldName = expr.name
            s"""
              |${fieldCode.code}
              |out.$fieldName = ${fieldCode.resultTerm};
            """.stripMargin
        } mkString("\n")

        s"""
          |$resultSetters
          |${result("out")}
        """.stripMargin

      case cc: CaseClassTypeInfo[_] =>
        val fields: String = fieldsCode.map(_.code).mkString("\n")
        val ctorParams: String = fieldsCode.map(_.resultTerm).mkString(",")

        s"""
          |$fields
          |return new $resultType($ctorParams);
        """.stripMargin
    }

    block
  }

}
