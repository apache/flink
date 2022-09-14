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
package org.apache.flink.table.planner.codegen

import org.apache.flink.table.planner.codegen.CodeGenUtils.{hashCodeForType, newName, primitiveTypeTermForType, rowFieldReadAccess, ARRAY_DATA, MAP_DATA, ROW_DATA}
import org.apache.flink.table.planner.codegen.Indenter.toISC
import org.apache.flink.table.runtime.generated.{GeneratedHashFunction, HashFunction}
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.util.MathUtils

/**
 * CodeGenerator for hash code RowData/ArrayData/MapData, Calculate a hash value based on some
 * fields of RowData. NOTE: If you need a hash value that is more evenly distributed, call
 * [[MathUtils.murmurHash]] outside to scatter.
 */
object HashCodeGenerator {

  /**
   * A sequence of prime numbers to be used for salting the computed hash values. Based on some
   * empirical evidence, we are using a 32-element subsequence of the OEIS sequence #A068652
   * (numbers such that every cyclic permutation is a prime).
   *
   * @see
   *   <a href="http://en.wikipedia.org/wiki/List_of_prime_numbers">
   *   http://en.wikipedia.org/wiki/List_of_prime_numbers</a>
   * @see
   *   <a href="http://oeis.org/A068652">http://oeis.org/A068652</a>
   */
  val HASH_SALT: Array[Int] = Array[Int](73, 79, 97, 113, 131, 197, 199, 311, 337, 373, 719, 733,
    919, 971, 991, 1193, 1931, 3119, 3779, 7793, 7937, 9311, 9377, 11939, 19391, 19937, 37199,
    39119, 71993, 91193, 93719, 93911)

  def generateRowHash(
      ctx: CodeGeneratorContext,
      input: LogicalType,
      name: String,
      hashFields: Array[Int]): GeneratedHashFunction = {
    val className = newName(name)
    val baseClass = classOf[HashFunction]
    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM

    val accessExprs =
      hashFields.map(idx => GenerateUtils.generateFieldAccess(ctx, input, inputTerm, idx))

    val (hashBody, resultTerm) = generateCodeBody(ctx, accessExprs)
    val code =
      j"""
      public class $className implements ${baseClass.getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $className(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public int hashCode(Object _in) {
          $ROW_DATA $inputTerm = ($ROW_DATA) _in;
          ${ctx.reuseLocalVariableCode()}
          $hashBody
          return $resultTerm;
        }

        ${ctx.reuseInnerClassDefinitionCode()}
      }
    """.stripMargin

    new GeneratedHashFunction(className, code, ctx.references.toArray, ctx.tableConfig)
  }

  def generateArrayHash(
      ctx: CodeGeneratorContext,
      elementType: LogicalType,
      name: String): GeneratedHashFunction = {
    val className = newName(name)
    val baseClass = classOf[HashFunction]
    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM

    val typeTerm = primitiveTypeTermForType(elementType)
    val isNull = newName("isNull")
    val fieldTerm = newName("fieldTerm")
    val elementHashTerm = newName("elementHashCode")
    val hashIntTerm = newName("hashCode")
    val i = newName("i")

    // Generate element hash code firstly
    val elementHashBody = hashCodeForType(ctx, elementType, fieldTerm)
    val code =
      j"""
      public class $className implements ${baseClass.getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $className(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public int hashCode(Object _in) {
          $ARRAY_DATA $inputTerm = ($ARRAY_DATA) _in;
          int $hashIntTerm = 1;
          // This is inspired by hive & presto
          for (int $i = 0; $i < $inputTerm.size(); $i++) {
            boolean $isNull = $inputTerm.isNullAt($i);
            int $elementHashTerm = 0;
            if (!$isNull) {
              $typeTerm $fieldTerm = ${rowFieldReadAccess(i, inputTerm, elementType)};
              $elementHashTerm = $elementHashBody;
            }
             $hashIntTerm = 31 * $hashIntTerm + $elementHashTerm;
          }

          return $hashIntTerm;
        }

        ${ctx.reuseInnerClassDefinitionCode()}
      }
    """.stripMargin

    new GeneratedHashFunction(className, code, ctx.references.toArray, ctx.tableConfig)
  }

  def generateMapHash(
      ctx: CodeGeneratorContext,
      keyType: LogicalType,
      valueType: LogicalType,
      name: String): GeneratedHashFunction = {
    val className = newName(name)
    val baseClass = classOf[HashFunction]
    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM

    val keyTypeTerm = primitiveTypeTermForType(keyType)
    val valueTypeTerm = primitiveTypeTermForType(valueType)
    val keys = newName("keys")
    val values = newName("values")
    val keyIsNull = newName("keyIsNull")
    val keyFieldTerm = newName("keyFieldTerm")
    val valueIsNull = newName("valueIsNull")
    val valueFieldTerm = newName("valueFieldTerm")
    val keyHashTerm = newName("keyHashCode")
    val valueHashTerm = newName("valueHashCode")
    val hashIntTerm = newName("hashCode")
    val i = newName("i")

    // Generate key and value hash code body firstly
    val keyElementHashBody = hashCodeForType(ctx, keyType, keyFieldTerm)
    val valueElementHashBody = hashCodeForType(ctx, valueType, valueFieldTerm)
    val code =
      j"""
      public class $className implements ${baseClass.getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $className(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public int hashCode(Object _in) {
          $MAP_DATA $inputTerm = ($MAP_DATA) _in;
          $ARRAY_DATA $keys = $inputTerm.keyArray();
          $ARRAY_DATA $values = $inputTerm.valueArray();

          int $keyHashTerm = 0;
          int $valueHashTerm = 0;
          int $hashIntTerm = 0;
          
          // This is inspired by hive & presto
          for (int $i = 0; $i < $inputTerm.size(); $i++) {
            boolean $keyIsNull = $keys.isNullAt($i);
            $keyHashTerm = 0;
            if (!$keyIsNull) {
              $keyTypeTerm $keyFieldTerm = ${rowFieldReadAccess(i, keys, keyType)};
              $keyHashTerm = $keyElementHashBody;
            }

            boolean $valueIsNull = $values.isNullAt($i);
            $valueHashTerm = 0;
            if(!$valueIsNull) {
              $valueTypeTerm $valueFieldTerm = ${rowFieldReadAccess(i, values, valueType)};
              $valueHashTerm = $valueElementHashBody;
            }
            
            $hashIntTerm += $keyHashTerm ^ $valueHashTerm;
          }

          return $hashIntTerm;
        }

        ${ctx.reuseInnerClassDefinitionCode()}
      }
    """.stripMargin

    new GeneratedHashFunction(className, code, ctx.references.toArray, ctx.tableConfig)
  }

  private def generateCodeBody(
      ctx: CodeGeneratorContext,
      accessExprs: Seq[GeneratedExpression]): (String, String) = {
    val hashIntTerm = newName("hashCode")
    var i = -1
    val hashBodyCode = accessExprs
      .map(
        expr => {
          i = i + 1
          s"""
             |$hashIntTerm *= ${HASH_SALT(i & 0x1f)};
             |${expr.code}
             |if (!${expr.nullTerm}) {
             | $hashIntTerm += ${hashCodeForType(ctx, expr.resultType, expr.resultTerm)};
             |}
             |""".stripMargin

        })
      .mkString("\n")
    (
      s"""
         |int $hashIntTerm = 0;
         |$hashBodyCode""".stripMargin,
      hashIntTerm)
  }
}
