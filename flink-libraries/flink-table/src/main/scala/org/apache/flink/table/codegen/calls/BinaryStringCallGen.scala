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

import org.apache.calcite.runtime.SqlFunctions
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.sql.fun.SqlTrimFunction.Flag.{BOTH, LEADING, TRAILING}
import org.apache.flink.table.api.types.{DataTypes, InternalType, MapType}
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.CodeGeneratorContext.BINARY_STRING
import org.apache.flink.table.codegen.calls.CallGenerator.{
  generateCallIfArgsNotNull, generateCallIfArgsNullable, generateReturnStringCallIfArgsNotNull}
import org.apache.flink.table.codegen.calls.ScalarOperators.generateNot
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.dataformat.BinaryMap
import org.apache.flink.table.functions.sql.ScalarSqlFunctions
import org.apache.flink.table.runtime.conversion.DataStructureConverters.genToInternal
import org.apache.flink.table.runtime.functions.{BuildInScalarFunctions, ScalarFunctions}

/**
  * Code generator for call with string parameters or return value.
  * 1.Some specific optimization of BinaryString.
  * 2.Deal with conversions between Java String and internal String.
  *
  * <p>TODO Need to rewrite most of the methods here, calculated directly on the BinaryString
  * instead of convert BinaryString to String.
  */
object BinaryStringCallGen {

  def generateCallExpression(
      ctx: CodeGeneratorContext,
      operator: SqlOperator,
      operands: Seq[GeneratedExpression],
      returnType: InternalType): Option[GeneratedExpression] = {
    val generator = operator match {

      case SqlStdOperatorTable.LIKE =>
        new LikeCallGen().generate(ctx, operands, DataTypes.BOOLEAN, nullCheck = true)

      case SqlStdOperatorTable.NOT_LIKE => generateNot(ctx, nullCheck = true,
        new LikeCallGen().generate(ctx, operands, DataTypes.BOOLEAN, nullCheck = true))

      case ScalarSqlFunctions.SUBSTRING | ScalarSqlFunctions.SUBSTR =>
        generateSubString(ctx, operands)

      case ScalarSqlFunctions.LEFT => generateLeft(ctx, operands.head, operands(1))

      case ScalarSqlFunctions.RIGHT => generateRight(ctx, operands.head, operands(1))

      case CHAR_LENGTH | CHARACTER_LENGTH | ScalarSqlFunctions.LENGTH =>
        generateCharLength(ctx, operands)

      case SIMILAR_TO => generateSimilarTo(ctx, operands)

      case NOT_SIMILAR_TO => generateNot(ctx, nullCheck = true, generateSimilarTo(ctx, operands))

      case ScalarSqlFunctions.REGEXP_EXTRACT => generateRegexpExtract(ctx, operands)

      case ScalarSqlFunctions.REGEXP_REPLACE => generateRegexpReplace(ctx, operands)

      case ScalarSqlFunctions.IS_DECIMAL => generateIsDecimal(ctx, operands)

      case ScalarSqlFunctions.IS_DIGIT => generateIsDigit(ctx, operands)

      case ScalarSqlFunctions.IS_ALPHA => generateIsAlpha(ctx, operands)

      case UPPER => generateUpper(ctx, operands)

      case LOWER => generateLower(ctx, operands)

      case INITCAP => generateInitcap(ctx, operands)

      case POSITION => generatePosition(ctx, operands)

      case ScalarSqlFunctions.LOCATE => generateLocate(ctx, operands)

      case OVERLAY => generateOverlay(ctx, operands)

      case ScalarSqlFunctions.LPAD => generateLpad(ctx, operands)

      case ScalarSqlFunctions.RPAD => generateRpad(ctx, operands)

      case ScalarSqlFunctions.REPEAT => generateRepeat(ctx, operands)

      case ScalarSqlFunctions.REVERSE => generateReverse(ctx, operands)

      case ScalarSqlFunctions.REPLACE => generateReplace(ctx, operands)

      case ScalarSqlFunctions.SPLIT_INDEX => generateSplitIndex(ctx, operands)

      case ScalarSqlFunctions.KEYVALUE => generateKeyValue(ctx, operands)

      case ScalarSqlFunctions.HASH_CODE if operands.head.resultType == DataTypes.STRING =>
        generateHashCode(ctx, operands)

      case ScalarSqlFunctions.MD5 => generateMd5(ctx, operands)

      case ScalarSqlFunctions.SHA1 => generateSha1(ctx, operands)

      case ScalarSqlFunctions.SHA224 => generateSha224(ctx, operands)

      case ScalarSqlFunctions.SHA256 => generateSha256(ctx, operands)

      case ScalarSqlFunctions.SHA384 => generateSha384(ctx, operands)

      case ScalarSqlFunctions.SHA512 => generateSha512(ctx, operands)

      case ScalarSqlFunctions.SHA2 => generateSha2(ctx, operands)

      case ScalarSqlFunctions.PARSE_URL => generateParserUrl(ctx, operands)

      case ScalarSqlFunctions.FROM_BASE64 => generateFromBase64(ctx, operands)

      case ScalarSqlFunctions.TO_BASE64 => generateToBase64(ctx, operands)

      case ScalarSqlFunctions.CHR => generateChr(ctx, operands)

      case ScalarSqlFunctions.REGEXP => generateRegExp(ctx, operands)

      case ScalarSqlFunctions.JSON_VALUE => generateJsonValue(ctx, operands)

      case ScalarSqlFunctions.BIN => generateBin(ctx, operands)

      case ScalarSqlFunctions.CONCAT =>
        operands.foreach(requireString(_, operator.getName))
        generateConcat(ctx, nullCheck = true, operands)

      case ScalarSqlFunctions.CONCAT_WS =>
        operands.foreach(requireString(_, operator.getName))
        generateConcatWs(ctx, operands)

      case ScalarSqlFunctions.STR_TO_MAP => generateStrToMap(ctx, operands)

      case TRIM => generateTrim(ctx, operands)

      case ScalarSqlFunctions.LTRIM => generateTrimLeft(ctx, operands)

      case ScalarSqlFunctions.RTRIM => generateTrimRight(ctx, operands)

      case CONCAT =>
        val left = operands.head
        val right = operands(1)
        requireString(left, operator.getName)
        generateArithmeticConcat(ctx, left, right)

      case ScalarSqlFunctions.UUID => generateUuid(ctx, operands)

      case ScalarSqlFunctions.ASCII => generateAscii(ctx, operands.head)

      case ScalarSqlFunctions.ENCODE => generateEncode(ctx, operands.head, operands(1))

      case ScalarSqlFunctions.DECODE => generateDecode(ctx, operands.head, operands(1))

      case ScalarSqlFunctions.INSTR => generateInstr(ctx, operands)

      case _ => null
    }

    Option(generator)
  }

  private def toStringTerms(terms: Seq[String], operands: Seq[GeneratedExpression]) = {
    terms.zipWithIndex.map { case (term, index) =>
      if (operands(index).resultType == DataTypes.STRING) {
        s"$term.toString()"
      } else {
        term
      }
    }.mkString(",")
  }

  private def safeToStringTerms(terms: Seq[String], operands: Seq[GeneratedExpression]) = {
    terms.zipWithIndex.map { case (term, index) =>
      if (operands(index).resultType == DataTypes.STRING) {
        s"$BINARY_STRING.safeToString($term)"
      } else {
        term
      }
    }.mkString(",")
  }

  def generateConcat(
      ctx: CodeGeneratorContext,
      nullCheck: Boolean,
      operands: Seq[GeneratedExpression]): GeneratedExpression = {
    generateCallIfArgsNullable(ctx, nullCheck, DataTypes.STRING, operands) {
      terms => s"$BINARY_STRING.concat(${terms.mkString(", ")})"
    }
  }

  def generateConcatWs(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression]): GeneratedExpression = {
    generateCallIfArgsNullable(ctx, nullCheck = true, DataTypes.STRING, operands) {
      terms => s"$BINARY_STRING.concatWs(${terms.mkString(", ")})"
    }
  }

  /**
    * Optimization: use BinaryString equals instead of compare.
    */
  def generateStringEquals(
      ctx: CodeGeneratorContext,
      left: GeneratedExpression,
      right: GeneratedExpression): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.BOOLEAN, Seq(left, right)) {
      terms => s"(${terms.head}.equals(${terms(1)}))"
    }
  }

  /**
    * Optimization: use BinaryString equals instead of compare.
    */
  def generateStringNotEquals(
      ctx: CodeGeneratorContext,
      left: GeneratedExpression,
      right: GeneratedExpression): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.BOOLEAN, Seq(left, right)) {
      terms => s"!(${terms.head}.equals(${terms(1)}))"
    }
  }

  def generateSubString(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.STRING, operands) {
      terms => s"${terms.head}.substringSQL(${terms.drop(1).mkString(", ")})"
    }
  }

  def generateLeft(
    ctx: CodeGeneratorContext,
    str: GeneratedExpression,
    len: GeneratedExpression): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.STRING, Seq(str, len)) {
      val emptyString = s"$BINARY_STRING.EMPTY_UTF8"
      terms => s"${terms(1)} <= 0 ? $emptyString : ${terms.head}.substringSQL(1, ${terms(1)})"
    }
  }

  def generateRight(
      ctx: CodeGeneratorContext,
      str: GeneratedExpression,
      len: GeneratedExpression): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.STRING, Seq(str, len)) {
      terms =>
        s"""
           |${terms(1)} <= 0 ?
           |  $BINARY_STRING.EMPTY_UTF8 :
           |  ${terms(1)} >= ${terms.head}.numChars() ?
           |  ${terms.head} :
           |  ${terms.head}.substringSQL(-${terms(1)})
       """.stripMargin
    }
  }

  def generateCharLength(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression]): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.INT, operands) {
      terms => s"${terms.head}.numChars()"
    }
  }

  def generateSimilarTo(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctions].getCanonicalName
    generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.BOOLEAN, operands) {
      terms => s"$className.similar(${toStringTerms(terms, operands)})"
    }
  }

  def generateRegexpExtract(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[ScalarFunctions].getCanonicalName
    generateCallIfArgsNullable(ctx, nullCheck = true, DataTypes.STRING, operands) {
      terms =>
        s"$BINARY_STRING.fromString($className.regExpExtract(${
          safeToStringTerms(terms, operands)}))"
    }
  }

  def generateRegexpReplace(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[ScalarFunctions].getCanonicalName
    generateReturnStringCallIfArgsNotNull(ctx, operands) {
      terms => s"$className.regExpReplace(${toStringTerms(terms, operands)})"
    }
  }

  def generateIsDecimal(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[BuildInScalarFunctions].getCanonicalName
    generateCallIfArgsNullable(ctx, nullCheck = true, DataTypes.BOOLEAN, operands) {
      terms => s"$className.isDecimal(${safeToStringTerms(terms, operands)})"
    }
  }

  def generateIsDigit(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[BuildInScalarFunctions].getCanonicalName
    generateCallIfArgsNullable(ctx, nullCheck = true, DataTypes.BOOLEAN, operands) {
      terms => s"$className.isDigit(${safeToStringTerms(terms, operands)})"
    }
  }

  def generateAscii(
    ctx: CodeGeneratorContext,
    str: GeneratedExpression): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.INT, Seq(str)) {
      terms => s"${terms.head}.numBytes() <= 0 ? 0 : (int) ${terms.head}.getByte(0)"
    }
  }

  def generateIsAlpha(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[BuildInScalarFunctions].getCanonicalName
    generateCallIfArgsNullable(ctx, nullCheck = true, DataTypes.BOOLEAN, operands) {
      terms => s"$className.isAlpha(${safeToStringTerms(terms, operands)})"
    }
  }

  def generateUpper(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.STRING, operands) {
      terms => s"${terms.head}.toUpperCase()"
    }
  }

  def generateLower(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.STRING, operands) {
      terms => s"${terms.head}.toLowerCase()"
    }
  }

  def generateInitcap(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctions].getCanonicalName
    generateReturnStringCallIfArgsNotNull(ctx, operands) {
      terms => s"$className.initcap(${terms.head}.toString())"
    }
  }

  def generatePosition(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[ScalarFunctions].getCanonicalName
    generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.INT, operands) {
      terms => s"$className.position(${terms.mkString(",")})"
    }
  }

  def generateLocate(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[ScalarFunctions].getCanonicalName
    generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.INT, operands) {
      terms => s"$className.position(${terms.mkString(",")})"
    }
  }

  def generateInstr(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[ScalarFunctions].getCanonicalName
    generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.INT, operands) {
      terms =>
        val startPosition = if (operands.length < 3) 1 else terms(2)
        val nthAppearance = if (operands.length < 4) 1 else terms(3)
          s"$className.instr(${terms.head}, ${terms(1)}, " +
          s"$startPosition, $nthAppearance)"
    }
  }

  def generateOverlay(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[ScalarFunctions].getCanonicalName
    generateReturnStringCallIfArgsNotNull(ctx, operands) {
      terms => s"$className.overlay(${toStringTerms(terms, operands)})"
    }
  }

  def generateArithmeticConcat(
      ctx: CodeGeneratorContext,
      left: GeneratedExpression,
      right: GeneratedExpression): GeneratedExpression = {
    generateReturnStringCallIfArgsNotNull(ctx, Seq(left, right)) {
      terms => s"${terms.head}.toString() + String.valueOf(${terms(1)})"
    }
  }

  def generateLpad(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[ScalarFunctions].getCanonicalName
    generateReturnStringCallIfArgsNotNull(ctx, operands) {
      terms =>
        s"$className.lpad(${toStringTerms(terms, operands)})"
    }
  }

  def generateRpad(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[ScalarFunctions].getCanonicalName
    generateReturnStringCallIfArgsNotNull(ctx, operands) {
      terms =>
        s"$className.rpad(${toStringTerms(terms, operands)})"
    }
  }

  def generateRepeat(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[ScalarFunctions].getCanonicalName
    generateReturnStringCallIfArgsNotNull(ctx, operands) {
      terms => s"$className.repeat(${toStringTerms(terms, operands)})"
    }
  }

  def generateReverse(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.STRING, operands) {
      terms => s"${terms.head}.reverse()"
    }
  }

  def generateReplace(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[ScalarFunctions].getCanonicalName
    generateReturnStringCallIfArgsNotNull(ctx, operands) {
      terms => s"$className.replace(${toStringTerms(terms, operands)})"
    }
  }

  def generateSplitIndex(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[ScalarFunctions].getCanonicalName
    generateReturnStringCallIfArgsNotNull(ctx, operands) {
      terms => s"$className.splitIndex(${toStringTerms(terms, operands)})"
    }
  }

  def generateKeyValue(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[ScalarFunctions].getCanonicalName
    generateCallIfArgsNullable(ctx, nullCheck = true, DataTypes.STRING, operands) {
      terms =>
        s"$className.keyValue(${terms.mkString(",")})"
    }
  }

  def generateHashCode(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[BuildInScalarFunctions].getCanonicalName
    generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.INT, operands) {
      terms => s"$className.hashCode(${terms.head}.toString())"
    }
  }

  def generateMd5(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression]): GeneratedExpression =
    generateHashInternal(ctx, "MD5", operands)

  def generateHashInternal(
      ctx: CodeGeneratorContext,
      algorithm: String,
      operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val digestTerm = ctx.addReusableMessageDigest(algorithm)
    if (operands.length == 1) {
      generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.STRING, operands) {
        terms =>s"${terms.head}.hash($digestTerm)"
      }
    } else {
      val className = classOf[ScalarFunctions].getCanonicalName
      generateReturnStringCallIfArgsNotNull(ctx, operands) {
        terms => s"$className.hash($digestTerm, ${toStringTerms(terms, operands)})"
      }
    }
  }

  def generateSha1(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression]): GeneratedExpression =
    generateHashInternal(ctx, "SHA", operands)

  def generateSha224(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression]): GeneratedExpression =
    generateHashInternal(ctx, "SHA-224", operands)

  def generateSha256(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression]): GeneratedExpression =
    generateHashInternal(ctx, "SHA-256", operands)

  def generateSha384(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression]): GeneratedExpression =
    generateHashInternal(ctx, "SHA-384", operands)

  def generateSha512(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression]): GeneratedExpression =
    generateHashInternal(ctx, "SHA-512", operands)

  def generateSha2(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[ScalarFunctions].getCanonicalName
    if (operands.last.literal) {
      val digestTerm = ctx.addReusableSha2MessageDigest(operands.last, nullCheck = true)
      if (operands.length == 2) {
        generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.STRING, operands) {
          terms =>s"${terms.head}.hash($digestTerm)"
        }
      } else {
        generateReturnStringCallIfArgsNotNull(ctx, operands) {
          terms =>
            s"$className.hash($digestTerm," +
                s"${toStringTerms(terms.dropRight(1), operands.dropRight(1))})"
        }
      }
    } else {
      if (operands.length == 2) {
        generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.STRING, operands) {
          terms =>
            s"""${terms.head}.hash("SHA-" + ${terms.last})"""
        }
      } else {
        generateReturnStringCallIfArgsNotNull(ctx, operands) {
          terms => {
            val strTerms = toStringTerms(terms.dropRight(1), operands.dropRight(1))
            s"""$className.hash("SHA-" + ${terms.last}, $strTerms)"""
          }
        }
      }
    }
  }

  def generateParserUrl(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[ScalarFunctions].getCanonicalName
    generateCallIfArgsNullable(ctx, nullCheck = true, DataTypes.STRING, operands) {
      terms =>
        s"$BINARY_STRING.fromString($className.parseUrl(${safeToStringTerms(terms, operands)}))"
    }
  }

  def generateFromBase64(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[BuildInScalarFunctions].getCanonicalName
    generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.BYTE_ARRAY, operands) {
      terms => s"$className.fromBase64(${terms.head}.toString())"
    }
  }

  def generateToBase64(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[BuildInScalarFunctions].getCanonicalName
    generateReturnStringCallIfArgsNotNull(ctx, operands) {
      terms => s"$className.toBase64(${terms.head})"
    }
  }

  def generateChr(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[ScalarFunctions].getCanonicalName
    generateReturnStringCallIfArgsNotNull(ctx, operands) {
      terms => s"$className.chr(${terms.head})"
    }
  }

  def generateRegExp(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[BuildInScalarFunctions].getCanonicalName
    generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.BOOLEAN, operands) {
      terms => s"$className.regExp(${toStringTerms(terms, operands)})"
    }
  }

  def generateJsonValue(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[BuildInScalarFunctions].getCanonicalName
    generateCallIfArgsNullable(ctx, nullCheck = true, DataTypes.STRING, operands) {
      terms =>
        s"$BINARY_STRING.fromString($className.jsonValue(${safeToStringTerms(terms, operands)}))"
    }
  }

  def generateBin(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    generateReturnStringCallIfArgsNotNull(ctx, operands) {
      terms => s"Long.toBinaryString(${terms.head})"
    }
  }

  def generateTrim(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.STRING, operands) {
      terms =>
        val leading = compareEnum(terms.head, BOTH) || compareEnum(terms.head, LEADING)
        val trailing = compareEnum(terms.head, BOTH) || compareEnum(terms.head, TRAILING)
        val args = s"$leading, $trailing, ${terms(1)}"
        s"${terms(2)}.trim($args)"
    }
  }

  def generateTrimLeft(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.STRING, operands) {
      terms => s"${terms.head}.trimLeft(${terms.drop(1).mkString(", ")})"
    }
  }

  def generateTrimRight(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.STRING, operands) {
      terms => s"${terms.head}.trimRight(${terms.drop(1).mkString(", ")})"
    }
  }

  def generateUuid(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[BuildInScalarFunctions].getCanonicalName
    generateReturnStringCallIfArgsNotNull(ctx, operands) {
      terms => s"$className.uuid(${terms.mkString(",")})"
    }
  }

  def generateStrToMap(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[BuildInScalarFunctions].getCanonicalName
    val t = new MapType(DataTypes.STRING, DataTypes.STRING)
    val convertFunc = genToInternal(ctx, t)
    generateCallIfArgsNotNull(ctx, nullCheck = true, t, operands) {
      terms =>
        val map = s"$className.strToMap(${toStringTerms(terms, operands)})"
        s"(${classOf[BinaryMap].getCanonicalName}) ${convertFunc(map)}"
    }
  }

  def generateEncode(
      ctx: CodeGeneratorContext,
      str: GeneratedExpression,
      charset: GeneratedExpression): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.BYTE_ARRAY, Seq(str, charset)) {
      terms => s"${terms.head}.toString().getBytes(${terms(1)}.toString())"
    }
  }

  def generateDecode(
      ctx: CodeGeneratorContext,
      binary: GeneratedExpression,
      charset: GeneratedExpression): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, nullCheck = true, DataTypes.STRING, Seq(binary, charset)) {
      terms =>
        s"$BINARY_STRING.fromString(new String(${terms.head}, ${terms(1)}.toString()))"
    }
  }

}
