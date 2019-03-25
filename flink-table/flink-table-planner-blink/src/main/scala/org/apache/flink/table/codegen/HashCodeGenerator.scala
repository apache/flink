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

import org.apache.flink.table.`type`.{ArrayType, DateType, DecimalType, InternalType, InternalTypes, TimestampType}
import org.apache.flink.table.codegen.CodeGenUtils.{BASE_ROW, newName}
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.dataformat.util.HashUtil
import org.apache.flink.table.generated.{GeneratedHashFunction, HashFunction}
import org.apache.flink.util.MathUtils

/**
  * CodeGenerator for hash code [[BaseRow]], Calculate a hash value based on some fields
  * of [[BaseRow]].
  * NOTE: If you need a hash value that is more evenly distributed, call [[MathUtils.murmurHash]]
  * outside to scatter.
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
      hashFields: Array[Int]): GeneratedHashFunction = {
    val className = newName(name)
    val baseClass = classOf[HashFunction]
    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM

    val accessExprs = hashFields.map(
      idx => GenerateUtils.generateFieldAccess(ctx, input, inputTerm, idx))

    val (hashBody, resultTerm) = generateCodeBody(accessExprs)
    val code =
      j"""
      public class $className implements ${baseClass.getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $className(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public int hashCode($BASE_ROW $inputTerm) {
          ${ctx.reuseLocalVariableCode()}
          $hashBody
          return $resultTerm;
        }
      }
    """.stripMargin

    new GeneratedHashFunction(className, code, ctx.references.toArray)
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

  private def hashExpr(expr: GeneratedExpression): String = {
    val util = classOf[HashUtil].getCanonicalName
    s"$util.hash${hashNameInHashUtil(expr.resultType)}(${expr.resultTerm})"
  }

  private def hashNameInHashUtil(t: InternalType): String = t match {
    case InternalTypes.INT => "Int"
    case InternalTypes.LONG => "Long"
    case InternalTypes.SHORT => "Short"
    case InternalTypes.BYTE => "Byte"
    case InternalTypes.FLOAT => "Float"
    case InternalTypes.DOUBLE => "Double"
    case InternalTypes.BOOLEAN => "Boolean"
    case InternalTypes.CHAR => "Char"
    case InternalTypes.STRING => "String"
    // decimal
    case _: DecimalType => "Decimal"
    // Sql time is Unboxing.
    case _: DateType => "Int"
    case InternalTypes.TIME => "Int"
    case _: TimestampType => "Long"
    case InternalTypes.BINARY => "Binary"
    case _: ArrayType => throw new IllegalArgumentException(s"Not support type to hash: $t")
    case _ => "Object"
  }
}
