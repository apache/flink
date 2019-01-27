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

package org.apache.flink.table.codegen

import org.apache.flink.table.api.types._
import org.apache.flink.table.codegen.CodeGenUtils.newName
import org.apache.flink.table.codegen.CodeGeneratorContext.BASE_ROW
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.dataformat.util.BinaryRowUtil

/**
  * CodeGenerator for projection.
  */
object HashCodeGenerator {

  /**
    * A sequence of prime numbers to be used for salting the computed hash values.
    * Based on some empirical evidence, we are using a 32-element subsequence of the
    * OEIS sequence #A068652 (numbers such that every cyclic permutation is a prime).
    *
    * @see <a href="http://en.wikipedia.org/wiki/List_of_prime_numbers">
    *        http://en.wikipedia.org/wiki/List_of_prime_numbers</a>
    * @see <a href="http://oeis.org/A068652">http://oeis.org/A068652</a>
    */
  val HASH_SALT: Array[Int] = Array[Int](
    73, 79, 97, 113, 131, 197, 199, 311, 337, 373, 719, 733, 919, 971, 991, 1193, 1931, 3119,
    3779, 7793, 7937, 9311, 9377, 11939, 19391, 19937, 37199, 39119, 71993, 91193, 93719, 93911)

  def generateRowHash(
      ctx: CodeGeneratorContext,
      input: InternalType,
      name: String,
      hashFields: Array[Int]): GeneratedHashFunc = {
    val className = newName(name)
    val baseClass = classOf[HashFunc]
    val inputTerm = CodeGeneratorContext.DEFAULT_INPUT1_TERM

    val accessExprs = hashFields.map(
      idx => CodeGenUtils.generateFieldAccess(ctx, input, inputTerm, idx, nullCheck = true))

    val (hashBody, resultTerm) = generateCodeBody(accessExprs)
    val code =
      j"""
      public class $className extends ${baseClass.getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $className() throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public int apply($BASE_ROW $inputTerm) {
          ${ctx.reuseFieldCode()}
          $hashBody
          return $resultTerm;
        }
      }
    """.stripMargin

    GeneratedHashFunc(className, code)
  }

  private def generateCodeBody(accessExprs: Seq[GeneratedExpression]): (String, String) = {
    val hashIntTerm = CodeGenUtils.newName("hashCode")
    var i = -1
    val hashBodyCode = accessExprs.map((expr) => {
      i = i + 1
      s"""
         |$hashIntTerm *= ${HASH_SALT(i & 0x1F)};
         |${expr.code}
         |if (!${expr.nullTerm}) {
         | $hashIntTerm += ${hashExpr(expr)};
         |}
         |""".stripMargin

    }).mkString("\n")
    (s"""
        |int $hashIntTerm = 0;
        |$hashBodyCode""".stripMargin, hashIntTerm)
  }

  def generateMultiFieldsHash(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val (hashBody, resultTerm) = generateCodeBody(operands)
    GeneratedExpression(resultTerm, "false", hashBody, DataTypes.INT)
  }

  def hashExpr(expr: GeneratedExpression): String = {
    val binaryUtil = classOf[BinaryRowUtil].getCanonicalName
    s"$binaryUtil.hash${hashNameInBinaryUtil(expr.resultType)}(${expr.resultTerm})"
  }

  def hashNameInBinaryUtil(t: InternalType): String = t match {
    case DataTypes.INT => "Int"
    case DataTypes.LONG => "Long"
    case DataTypes.SHORT => "Short"
    case DataTypes.BYTE => "Byte"
    case DataTypes.FLOAT => "Float"
    case DataTypes.DOUBLE => "Double"
    case DataTypes.BOOLEAN => "Boolean"
    case DataTypes.CHAR => "Char"
    case DataTypes.STRING => "String"
    // decimal
    case _: DecimalType => "Decimal"
    // Sql time is Unboxing.
    case _: DateType => "Int"
    case DataTypes.TIME => "Int"
    case _: TimestampType => "Long"
    case DataTypes.BYTE_ARRAY => "ByteArray"
    case _: ArrayType => throw new IllegalArgumentException(s"Not support type to hash: $t")
    case _ => "Object"
  }
}

abstract class HashFunc {
  def apply(row: BaseRow): Int
}
