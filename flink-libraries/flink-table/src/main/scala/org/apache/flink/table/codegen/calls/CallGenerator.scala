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

package org.apache.flink.table.codegen.calls

import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.CodeGeneratorContext.BINARY_STRING
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedExpression}

trait CallGenerator {

  def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: InternalType,
      nullCheck: Boolean): GeneratedExpression
}

object CallGenerator {

  def generateUnaryOperatorIfNotNull(
      ctx: CodeGeneratorContext,
      nullCheck: Boolean,
      returnType: InternalType,
      operand: GeneratedExpression,
      primitiveNullable: Boolean = false)
  (expr: String => String)
    : GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, nullCheck, returnType, Seq(operand), primitiveNullable) {
      args => expr(args.head)
    }
  }

  def generateOperatorIfNotNull(
      ctx: CodeGeneratorContext,
      nullCheck: Boolean,
      returnType: InternalType,
      left: GeneratedExpression,
      right: GeneratedExpression)
      (expr: (String, String) => String)
    : GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, nullCheck, returnType, Seq(left, right)) {
      args => expr(args.head, args(1))
    }
  }

  def generateReturnStringCallIfArgsNotNull(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression])
      (call: Seq[String] => String): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.STRING, operands) {
      args => s"$BINARY_STRING.fromString(${call(args)})"
    }
  }

  def generateReturnStringCallWithStmtIfArgsNotNull(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression])
      (call: Seq[String] => (String, String)): GeneratedExpression = {
    generateCallWithStmtIfArgsNotNull(ctx, nullCheck = true, DataTypes.STRING, operands) {
      args =>
        val (stmt, result) = call(args)
        (stmt, s"$BINARY_STRING.fromString($result)")
    }
  }

  def generateCallIfArgsNotNull(
      ctx: CodeGeneratorContext,
      nullCheck: Boolean,
      returnType: InternalType,
      operands: Seq[GeneratedExpression],
      primitiveNullable: Boolean = false)
      (call: Seq[String] => String): GeneratedExpression = {
    generateCallWithStmtIfArgsNotNull(ctx, nullCheck, returnType, operands, primitiveNullable) {
      args => ("", call(args))
    }
  }

  def generateCallWithStmtIfArgsNotNull(
      ctx: CodeGeneratorContext,
      nullCheck: Boolean,
      returnType: InternalType,
      operands: Seq[GeneratedExpression],
      primitiveNullable: Boolean = false)
      (call: Seq[String] => (String, String)): GeneratedExpression = {
    val resultTypeTerm =
      if (primitiveNullable) {
        boxedTypeTermForType(returnType)
      }
      else {
        primitiveTypeTermForType(returnType)
      }
    val nullTerm = ctx.newReusableField("isNull", "boolean")
    val resultTerm = ctx.newReusableField("result", resultTypeTerm)
    val defaultValue = primitiveDefaultValue(returnType)
    val nullResultCode = if (nullCheck
      && isReference(returnType)
      && !isInternalPrimitive(returnType)
      || nullCheck
      && primitiveNullable) {
      s"""
         |if ($resultTerm == null) {
         |  $nullTerm = true;
         |  $resultTerm = $defaultValue;
         |}
       """.stripMargin
    } else {
      ""
    }

    val (stmt, result) = call(operands.map(_.resultTerm))

    val resultCode = if (nullCheck && operands.nonEmpty) {
      s"""
         |${operands.map(_.code).mkString("\n")}
         |$nullTerm = ${operands.map(_.nullTerm).mkString(" || ")};
         |$resultTerm = $defaultValue;
         |if (!$nullTerm) {
         |  $stmt
         |  $resultTerm = $result;
         |  $nullResultCode
         |}
         |""".stripMargin
    } else if (nullCheck && operands.isEmpty) {
      s"""
         |${operands.map(_.code).mkString("\n")}
         |$nullTerm = false;
         |$stmt
         |$resultTerm = $result;
         |""".stripMargin
    } else{
      s"""
         |$nullTerm = false;
         |${operands.map(_.code).mkString("\n")}
         |$stmt
         |$resultTerm = $result;
         |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, resultCode, returnType)
  }

  def generateCallIfArgsNullable(
      ctx: CodeGeneratorContext,
      nullCheck: Boolean,
      returnType: InternalType,
      operands: Seq[GeneratedExpression],
      primitiveNullable: Boolean = false)
      (call: Seq[String] => String): GeneratedExpression = {

    val resultTypeTerm = if (primitiveNullable) {
      boxedTypeTermForType(returnType)
    } else {
      primitiveTypeTermForType(returnType)
    }
    val defaultValue = primitiveDefaultValue(returnType)

    val nullTerm = ctx.newReusableField("isNull", "boolean")
    val resultTerm = ctx.newReusableField("result", resultTypeTerm)
    val nullCode = if (nullCheck && (isReference(returnType) || primitiveNullable)) {
      s"$nullTerm = $resultTerm == null;"
    } else {
      s"$nullTerm = false;"
    }

    val parameters = operands.map(x =>
      if (x.resultType.equals(DataTypes.STRING)){
        "( " + x.nullTerm + " ) ? null : (" + x.resultTerm + ")"
      } else {
        x.resultTerm
      })


    val resultCode = if (nullCheck) {
      s"""
         |${operands.map(_.code).mkString("\n")}
         |$resultTerm = ${call(parameters)};
         |$nullCode
         |if ($nullTerm) {
         |  $resultTerm = $defaultValue;
         |}
       """.stripMargin
    } else {
      s"""
         |${operands.map(_.code).mkString("\n")}
         |$resultTerm = ${call(parameters)};
         |$nullCode
       """.stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, resultCode, returnType)
  }
}
