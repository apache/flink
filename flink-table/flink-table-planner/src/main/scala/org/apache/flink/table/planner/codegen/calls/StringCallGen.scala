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
package org.apache.flink.table.planner.codegen.calls

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.data.util.DataFormatConverters
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.GenerateUtils.{generateCallIfArgsNotNull, generateCallIfArgsNullable, generateNonNullField, generateStringResultCallIfArgsNotNull}
import org.apache.flink.table.planner.codegen.calls.ScalarOperatorGens._
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable._
import org.apache.flink.table.runtime.functions.SqlFunctionUtils
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils.{isCharacterString, isTimestamp, isTimestampWithLocalZone}
import org.apache.flink.table.types.logical._

import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.fun.SqlTrimFunction.Flag.{BOTH, LEADING, TRAILING}

import java.lang.reflect.Method

/**
 * Code generator for call with string parameters or return value.
 * 1.Some specific optimization of StringData. 2.Deal with conversions between Java String and
 * internal String.
 *
 * <p>TODO Need to rewrite most of the methods here, calculated directly on the StringData instead
 * of convert StringData to String.
 */
object StringCallGen {

  def generateCallExpression(
      ctx: CodeGeneratorContext,
      operator: SqlOperator,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): Option[GeneratedExpression] = {

    def methodGen(method: Method): GeneratedExpression = {
      new MethodCallGen(method).generate(ctx, operands, returnType)
    }

    def fallibleMethodGen(method: Method): GeneratedExpression = {
      new MethodCallGen(method, wrapTryCatch = true).generate(ctx, operands, returnType)
    }

    val generator = operator match {

      case LIKE =>
        new LikeCallGen().generate(ctx, operands, returnType)

      case NOT_LIKE =>
        generateNot(ctx, new LikeCallGen().generate(ctx, operands, returnType), returnType)

      case SUBSTR | SUBSTRING => generateSubString(ctx, operands, returnType)

      case LEFT => generateLeft(ctx, operands.head, operands(1), returnType)

      case RIGHT => generateRight(ctx, operands.head, operands(1), returnType)

      case CHAR_LENGTH | CHARACTER_LENGTH => generateCharLength(ctx, operands, returnType)

      case SIMILAR_TO => generateSimilarTo(ctx, operands, returnType)

      case NOT_SIMILAR_TO =>
        generateNot(ctx, generateSimilarTo(ctx, operands, returnType), returnType)

      case REGEXP_EXTRACT => generateRegexpExtract(ctx, operands, returnType)

      case REGEXP_REPLACE => generateRegexpReplace(ctx, operands, returnType)

      case IS_DECIMAL => generateIsDecimal(ctx, operands, returnType)

      case IS_DIGIT => generateIsDigit(ctx, operands, returnType)

      case IS_ALPHA => generateIsAlpha(ctx, operands, returnType)

      case UPPER => generateUpper(ctx, operands, returnType)

      case LOWER => generateLower(ctx, operands, returnType)

      case INITCAP => generateInitcap(ctx, operands, returnType)

      case POSITION => generatePosition(ctx, operands, returnType)

      case LOCATE => generateLocate(ctx, operands, returnType)

      case OVERLAY => generateOverlay(ctx, operands, returnType)

      case LPAD => generateLpad(ctx, operands, returnType)

      case RPAD => generateRpad(ctx, operands, returnType)

      case REPEAT => generateRepeat(ctx, operands, returnType)

      case REVERSE => generateReverse(ctx, operands, returnType)

      case REPLACE => generateReplace(ctx, operands, returnType)

      case SPLIT_INDEX => generateSplitIndex(ctx, operands, returnType)

      case HASH_CODE if isCharacterString(operands.head.resultType) =>
        generateHashCode(ctx, operands, returnType)

      case MD5 => generateMd5(ctx, operands, returnType)

      case SHA1 => generateSha1(ctx, operands, returnType)

      case SHA224 => generateSha224(ctx, operands, returnType)

      case SHA256 => generateSha256(ctx, operands, returnType)

      case SHA384 => generateSha384(ctx, operands, returnType)

      case SHA512 => generateSha512(ctx, operands, returnType)

      case SHA2 => generateSha2(ctx, operands, returnType)

      case PARSE_URL => generateParserUrl(ctx, operands, returnType)

      case FROM_BASE64 => generateFromBase64(ctx, operands, returnType)

      case TO_BASE64 => generateToBase64(ctx, operands, returnType)

      case CHR => generateChr(ctx, operands, returnType)

      case REGEXP => generateRegExp(ctx, operands, returnType)

      case BIN => generateBin(ctx, operands, returnType)

      case CONCAT_FUNCTION =>
        operands.foreach(requireCharacterString)
        generateConcat(ctx, operands, returnType)

      case CONCAT_WS =>
        operands.foreach(requireCharacterString)
        generateConcatWs(ctx, operands, returnType)

      case STR_TO_MAP => generateStrToMap(ctx, operands, returnType)

      case TRIM => generateTrim(ctx, operands, returnType)

      case LTRIM => generateTrimLeft(ctx, operands, returnType)

      case RTRIM => generateTrimRight(ctx, operands, returnType)

      case CONCAT =>
        val left = operands.head
        val right = operands(1)
        requireCharacterString(left)
        generateArithmeticConcat(ctx, left, right, returnType)

      case UUID => generateUuid(ctx, operands, returnType)

      case ASCII => generateAscii(ctx, operands.head, returnType)

      case ENCODE => generateEncode(ctx, operands.head, operands(1), returnType)

      case DECODE => generateDecode(ctx, operands.head, operands(1), returnType)

      case INSTR => generateInstr(ctx, operands, returnType)

      case PRINT => new PrintCallGen().generate(ctx, operands, returnType)

      case IF =>
        requireBoolean(operands.head)
        new IfCallGen().generate(ctx, operands, returnType)

      // Date/Time & StringData Converting -- start

      case TO_DATE if operands.size == 1 && isCharacterString(operands.head.resultType) =>
        methodGen(BuiltInMethods.STRING_TO_DATE)

      case TO_DATE
          if operands.size == 2 &&
            isCharacterString(operands.head.resultType) &&
            isCharacterString(operands(1).resultType) =>
        methodGen(BuiltInMethods.STRING_TO_DATE_WITH_FORMAT)

      case TO_TIMESTAMP if operands.size == 1 && isCharacterString(operands.head.resultType) =>
        fallibleMethodGen(BuiltInMethods.STRING_TO_TIMESTAMP)

      case TO_TIMESTAMP
          if operands.size == 2 &&
            isCharacterString(operands.head.resultType) &&
            isCharacterString(operands(1).resultType) =>
        fallibleMethodGen(BuiltInMethods.STRING_TO_TIMESTAMP_WITH_FORMAT)

      case UNIX_TIMESTAMP if operands.size == 1 && isCharacterString(operands.head.resultType) =>
        methodGen(BuiltInMethods.UNIX_TIMESTAMP_STR)

      case UNIX_TIMESTAMP
          if operands.size == 2 &&
            isCharacterString(operands.head.resultType) &&
            isCharacterString(operands(1).resultType) =>
        methodGen(BuiltInMethods.UNIX_TIMESTAMP_FORMAT)

      case DATE_FORMAT
          if operands.size == 2 &&
            isTimestamp(operands.head.resultType) &&
            isCharacterString(operands(1).resultType) =>
        methodGen(BuiltInMethods.FORMAT_TIMESTAMP_DATA)

      case DATE_FORMAT
          if operands.size == 2 &&
            isTimestampWithLocalZone(operands.head.resultType) &&
            isCharacterString(operands(1).resultType) =>
        methodGen(BuiltInMethods.FORMAT_TIMESTAMP_DATA_WITH_TIME_ZONE)

      case DATE_FORMAT
          if operands.size == 2 &&
            isCharacterString(operands.head.resultType) &&
            isCharacterString(operands(1).resultType) =>
        methodGen(BuiltInMethods.FORMAT_TIMESTAMP_STRING_FORMAT_STRING_STRING)

      case CONVERT_TZ
          if operands.size == 3 &&
            isCharacterString(operands.head.resultType) &&
            isCharacterString(operands(1).resultType) &&
            isCharacterString(operands(2).resultType) =>
        methodGen(BuiltInMethods.CONVERT_TZ)

      case CURRENT_DATABASE =>
        val currentDatabase = ctx.addReusableQueryLevelCurrentDatabase()
        generateNonNullField(returnType, currentDatabase)

      case _ => null
    }

    Option(generator)
  }

  private def toStringTerms(terms: Seq[String], operands: Seq[GeneratedExpression]) = {
    terms.zipWithIndex
      .map {
        case (term, index) =>
          if (isCharacterString(operands(index).resultType)) {
            s"$term.toString()"
          } else {
            term
          }
      }
      .mkString(",")
  }

  private def safeToStringTerms(terms: Seq[String], operands: Seq[GeneratedExpression]) = {
    terms.zipWithIndex
      .map {
        case (term, index) =>
          if (isCharacterString(operands(index).resultType)) {
            s"$BINARY_STRING_UTIL.safeToString($term)"
          } else {
            term
          }
      }
      .mkString(",")
  }

  def generateConcat(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    generateCallIfArgsNullable(ctx, returnType, operands) {
      terms => s"$BINARY_STRING_UTIL.concat(${terms.mkString(", ")})"
    }
  }

  def generateConcatWs(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    generateCallIfArgsNullable(ctx, returnType, operands) {
      terms => s"$BINARY_STRING_UTIL.concatWs(${terms.mkString(", ")})"
    }
  }

  /** Optimization: use StringData equals instead of compare. */
  def generateStringEquals(
      ctx: CodeGeneratorContext,
      left: GeneratedExpression,
      right: GeneratedExpression,
      resultType: LogicalType): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, resultType, Seq(left, right)) {
      terms => s"(${terms.head}.equals(${terms(1)}))"
    }
  }

  /** Optimization: use StringData equals instead of compare. */
  def generateStringNotEquals(
      ctx: CodeGeneratorContext,
      left: GeneratedExpression,
      right: GeneratedExpression,
      resultType: LogicalType): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, resultType, Seq(left, right)) {
      terms => s"!(${terms.head}.equals(${terms(1)}))"
    }
  }

  def generateSubString(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, returnType, operands) {
      terms => s"$BINARY_STRING_UTIL.substringSQL(${terms.head}, ${terms.drop(1).mkString(", ")})"
    }
  }

  def generateLeft(
      ctx: CodeGeneratorContext,
      str: GeneratedExpression,
      len: GeneratedExpression,
      returnType: LogicalType): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, returnType, Seq(str, len)) {
      val emptyString = s"$BINARY_STRING.EMPTY_UTF8"
      terms =>
        s"${terms(1)} <= 0 ? $emptyString :" +
          s" $BINARY_STRING_UTIL.substringSQL(${terms.head}, 1, ${terms(1)})"
    }
  }

  def generateRight(
      ctx: CodeGeneratorContext,
      str: GeneratedExpression,
      len: GeneratedExpression,
      returnType: LogicalType): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, returnType, Seq(str, len)) {
      terms =>
        s"""
           |${terms(1)} <= 0 ?
           |  $BINARY_STRING.EMPTY_UTF8 :
           |  ${terms(1)} >= ${terms.head}.numChars() ?
           |  ${terms.head} :
           |  $BINARY_STRING_UTIL.substringSQL(${terms.head}, -${terms(1)})
       """.stripMargin
    }
  }

  def generateCharLength(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      resultType: LogicalType): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, resultType, operands)(terms => s"${terms.head}.numChars()")
  }

  def generateSimilarTo(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      resultType: LogicalType): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, resultType, operands) {
      terms => s"${qualifyMethod(BuiltInMethods.STRING_SIMILAR)}(${toStringTerms(terms, operands)})"
    }
  }

  def generateRegexpExtract(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      resultType: LogicalType): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands, resultType) {
      terms => s"$className.regexpExtract(${safeToStringTerms(terms, operands)})"
    }
  }

  def generateRegexpReplace(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      resultType: LogicalType): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands, resultType) {
      terms => s"$className.regexpReplace(${toStringTerms(terms, operands)})"
    }
  }

  def generateIsDecimal(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      resultType: LogicalType): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateCallIfArgsNullable(ctx, resultType, operands) {
      terms => s"$className.isDecimal(${safeToStringTerms(terms, operands)})"
    }
  }

  def generateIsDigit(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      resultType: LogicalType): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateCallIfArgsNullable(ctx, resultType, operands) {
      terms => s"$className.isDigit(${safeToStringTerms(terms, operands)})"
    }
  }

  def generateAscii(
      ctx: CodeGeneratorContext,
      str: GeneratedExpression,
      resultType: LogicalType): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, resultType, Seq(str)) {
      terms => s"${terms.head}.getSizeInBytes() <= 0 ? 0 : (int) ${terms.head}.byteAt(0)"
    }
  }

  def generateIsAlpha(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      resultType: LogicalType): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateCallIfArgsNullable(ctx, resultType, operands) {
      terms => s"$className.isAlpha(${safeToStringTerms(terms, operands)})"
    }
  }

  def generateUpper(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, returnType, operands)(terms => s"${terms.head}.toUpperCase()")
  }

  def generateLower(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, returnType, operands)(terms => s"${terms.head}.toLowerCase()")
  }

  def generateInitcap(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    generateStringResultCallIfArgsNotNull(ctx, operands, returnType) {
      terms => s"${qualifyMethod(BuiltInMethods.STRING_INITCAP)}(${terms.head}.toString())"
    }
  }

  def generatePosition(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      resultType: LogicalType): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateCallIfArgsNotNull(ctx, resultType, operands) {
      terms => s"$className.position(${terms.mkString(",")})"
    }
  }

  def generateLocate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      resultType: LogicalType): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateCallIfArgsNotNull(ctx, resultType, operands) {
      terms => s"$className.position(${terms.mkString(",")})"
    }
  }

  def generateInstr(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      resultType: LogicalType): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateCallIfArgsNotNull(ctx, resultType, operands) {
      terms =>
        val startPosition = if (operands.length < 3) 1 else terms(2)
        val nthAppearance = if (operands.length < 4) 1 else terms(3)
        s"$className.instr(${terms.head}, ${terms(1)}, " +
          s"$startPosition, $nthAppearance)"
    }
  }

  def generateOverlay(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands, returnType) {
      terms => s"$className.overlay(${toStringTerms(terms, operands)})"
    }
  }

  def generateArithmeticConcat(
      ctx: CodeGeneratorContext,
      left: GeneratedExpression,
      right: GeneratedExpression,
      returnType: LogicalType): GeneratedExpression = {
    generateStringResultCallIfArgsNotNull(ctx, Seq(left, right), returnType) {
      terms => s"${terms.head}.toString() + String.valueOf(${terms(1)})"
    }
  }

  def generateLpad(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands, returnType) {
      terms => s"$className.lpad(${toStringTerms(terms, operands)})"
    }
  }

  def generateRpad(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands, returnType) {
      terms => s"$className.rpad(${toStringTerms(terms, operands)})"
    }
  }

  def generateRepeat(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands, returnType) {
      terms => s"$className.repeat(${toStringTerms(terms, operands)})"
    }
  }

  def generateReverse(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, returnType, operands) {
      terms => s"$BINARY_STRING_UTIL.reverse(${terms.head})"
    }
  }

  def generateReplace(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands, returnType) {
      terms => s"$className.replace(${toStringTerms(terms, operands)})"
    }
  }

  def generateSplitIndex(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands, returnType) {
      terms => s"$className.splitIndex(${toStringTerms(terms, operands)})"
    }
  }

  def generateKeyValue(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateCallIfArgsNullable(ctx, returnType, operands) {
      terms => s"$className.keyValue(${terms.mkString(",")})"
    }
  }

  def generateHashCode(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      resultType: LogicalType): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateCallIfArgsNotNull(ctx, resultType, operands) {
      terms => s"$className.hashCode(${terms.head}.toString())"
    }
  }

  def generateMd5(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression =
    generateHashInternal(ctx, "MD5", operands, returnType)

  def generateHashInternal(
      ctx: CodeGeneratorContext,
      algorithm: String,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    val digestTerm = ctx.addReusableMessageDigest(algorithm)
    if (operands.length == 1) {
      generateCallIfArgsNotNull(ctx, returnType, operands) {
        terms => s"$BINARY_STRING_UTIL.hash(${terms.head}, $digestTerm)"
      }
    } else {
      val className = classOf[SqlFunctionUtils].getCanonicalName
      generateStringResultCallIfArgsNotNull(ctx, operands, returnType) {
        terms => s"$className.hash($digestTerm, ${toStringTerms(terms, operands)})"
      }
    }
  }

  def generateSha1(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression =
    generateHashInternal(ctx, "SHA", operands, returnType)

  def generateSha224(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression =
    generateHashInternal(ctx, "SHA-224", operands, returnType)

  def generateSha256(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression =
    generateHashInternal(ctx, "SHA-256", operands, returnType)

  def generateSha384(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression =
    generateHashInternal(ctx, "SHA-384", operands, returnType)

  def generateSha512(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression =
    generateHashInternal(ctx, "SHA-512", operands, returnType)

  def generateSha2(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    if (operands.last.literal) {
      val digestTerm = ctx.addReusableSha2MessageDigest(operands.last)
      if (operands.length == 2) {
        generateCallIfArgsNotNull(ctx, returnType, operands) {
          terms => s"$BINARY_STRING_UTIL.hash(${terms.head}, $digestTerm)"
        }
      } else {
        generateStringResultCallIfArgsNotNull(ctx, operands, returnType) {
          terms =>
            s"$className.hash($digestTerm," +
              s"${toStringTerms(terms.dropRight(1), operands.dropRight(1))})"
        }
      }
    } else {
      if (operands.length == 2) {
        generateCallIfArgsNotNull(ctx, returnType, operands) {
          terms => s"""$BINARY_STRING_UTIL.hash(${terms.head}, "SHA-" + ${terms.last})"""
        }
      } else {
        generateStringResultCallIfArgsNotNull(ctx, operands, returnType) {
          terms =>
            {
              val strTerms = toStringTerms(terms.dropRight(1), operands.dropRight(1))
              s"""$className.hash("SHA-" + ${terms.last}, $strTerms)"""
            }
        }
      }
    }
  }

  def generateParserUrl(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands, returnType) {
      terms => s"$className.parseUrl(${safeToStringTerms(terms, operands)})"
    }
  }

  def generateFromBase64(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateCallIfArgsNotNull(ctx, returnType, operands) {
      terms => s"$className.fromBase64(${terms.head})"
    }
  }

  def generateToBase64(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands, returnType) {
      terms => s"$className.toBase64(${terms.head})"
    }
  }

  def generateChr(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands, returnType) {
      terms => s"$className.chr(${terms.head})"
    }
  }

  def generateRegExp(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateCallIfArgsNotNull(ctx, returnType, operands) {
      terms => s"$className.regExp(${toStringTerms(terms, operands)})"
    }
  }

  def generateJsonValue(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands, returnType) {
      terms => s"$className.jsonValue(${safeToStringTerms(terms, operands)})"
    }
  }

  def generateBin(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    generateStringResultCallIfArgsNotNull(ctx, operands, returnType) {
      terms => s"Long.toBinaryString(${terms.head})"
    }
  }

  def generateTrim(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, returnType, operands) {
      terms =>
        val leading = compareEnum(terms.head, BOTH) || compareEnum(terms.head, LEADING)
        val trailing = compareEnum(terms.head, BOTH) || compareEnum(terms.head, TRAILING)
        val args = s"$leading, $trailing, ${terms(1)}"
        s"$BINARY_STRING_UTIL.trim(${terms(2)}, $args)"
    }
  }

  def generateTrimLeft(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, returnType, operands) {
      terms => s"$BINARY_STRING_UTIL.trimLeft(${terms.mkString(", ")})"
    }
  }

  def generateTrimRight(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, returnType, operands) {
      terms => s"$BINARY_STRING_UTIL.trimRight(${terms.mkString(", ")})"
    }
  }

  def generateUuid(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands, returnType) {
      terms => s"$className.uuid(${terms.mkString(",")})"
    }
  }

  def generateStrToMap(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      resultType: LogicalType): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    val converter = DataFormatConverters.getConverterForDataType(
      DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
    val converterTerm = ctx.addReusableObject(converter, "mapConverter")
    generateCallIfArgsNotNull(ctx, resultType, operands) {
      terms =>
        val map = s"$className.strToMap(${toStringTerms(terms, operands)})"
        s"($BINARY_MAP) $converterTerm.toInternal($map)"
    }
  }

  def generateEncode(
      ctx: CodeGeneratorContext,
      str: GeneratedExpression,
      charset: GeneratedExpression,
      returnType: LogicalType): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, returnType, Seq(str, charset)) {
      terms => s"${terms.head}.toString().getBytes(${terms(1)}.toString())"
    }
  }

  def generateDecode(
      ctx: CodeGeneratorContext,
      binary: GeneratedExpression,
      charset: GeneratedExpression,
      returnType: LogicalType): GeneratedExpression = {
    generateStringResultCallIfArgsNotNull(ctx, Seq(binary, charset), returnType) {
      terms => s"new String(${terms.head}, ${terms(1)}.toString())"
    }
  }
}
