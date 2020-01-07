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
import org.apache.flink.table.dataformat.DataFormatConverters
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.GenerateUtils.{generateCallIfArgsNotNull, generateCallIfArgsNullable, generateStringResultCallIfArgsNotNull}
import org.apache.flink.table.planner.codegen.calls.ScalarOperatorGens._
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable._
import org.apache.flink.table.runtime.functions.SqlFunctionUtils
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils.{isCharacterString, isTimestamp, isTimestampWithLocalZone}
import org.apache.flink.table.types.logical.{BooleanType, IntType, LogicalType, MapType, VarBinaryType, VarCharType}

import org.apache.calcite.runtime.SqlFunctions
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.fun.SqlTrimFunction.Flag.{BOTH, LEADING, TRAILING}

import java.lang.reflect.Method

/**
  * Code generator for call with string parameters or return value.
  * 1.Some specific optimization of BinaryString.
  * 2.Deal with conversions between Java String and internal String.
  *
  * <p>TODO Need to rewrite most of the methods here, calculated directly on the BinaryString
  * instead of convert BinaryString to String.
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

    val generator = operator match {

      case LIKE =>
        new LikeCallGen().generate(ctx, operands, new BooleanType())

      case NOT_LIKE =>
        generateNot(ctx, new LikeCallGen().generate(ctx, operands, new BooleanType()))

      case SUBSTR | SUBSTRING => generateSubString(ctx, operands)

      case LEFT => generateLeft(ctx, operands.head, operands(1))

      case RIGHT => generateRight(ctx, operands.head, operands(1))

      case CHAR_LENGTH | CHARACTER_LENGTH => generateCharLength(ctx, operands)

      case SIMILAR_TO => generateSimilarTo(ctx, operands)

      case NOT_SIMILAR_TO => generateNot(ctx, generateSimilarTo(ctx, operands))

      case REGEXP_EXTRACT => generateRegexpExtract(ctx, operands)

      case REGEXP_REPLACE => generateRegexpReplace(ctx, operands)

      case IS_DECIMAL => generateIsDecimal(ctx, operands)

      case IS_DIGIT => generateIsDigit(ctx, operands)

      case IS_ALPHA => generateIsAlpha(ctx, operands)

      case UPPER => generateUpper(ctx, operands)

      case LOWER => generateLower(ctx, operands)

      case INITCAP => generateInitcap(ctx, operands)

      case POSITION => generatePosition(ctx, operands)

      case LOCATE => generateLocate(ctx, operands)

      case OVERLAY => generateOverlay(ctx, operands)

      case LPAD => generateLpad(ctx, operands)

      case RPAD => generateRpad(ctx, operands)

      case REPEAT => generateRepeat(ctx, operands)

      case REVERSE => generateReverse(ctx, operands)

      case REPLACE => generateReplace(ctx, operands)

      case SPLIT_INDEX => generateSplitIndex(ctx, operands)

      case HASH_CODE if isCharacterString(operands.head.resultType) =>
        generateHashCode(ctx, operands)

      case MD5 => generateMd5(ctx, operands)

      case SHA1 => generateSha1(ctx, operands)

      case SHA224 => generateSha224(ctx, operands)

      case SHA256 => generateSha256(ctx, operands)

      case SHA384 => generateSha384(ctx, operands)

      case SHA512 => generateSha512(ctx, operands)

      case SHA2 => generateSha2(ctx, operands)

      case PARSE_URL => generateParserUrl(ctx, operands)

      case FROM_BASE64 => generateFromBase64(ctx, operands)

      case TO_BASE64 => generateToBase64(ctx, operands)

      case CHR => generateChr(ctx, operands)

      case REGEXP => generateRegExp(ctx, operands)

      case BIN => generateBin(ctx, operands)

      case CONCAT_FUNCTION =>
        operands.foreach(requireCharacterString)
        generateConcat(ctx, operands)

      case CONCAT_WS =>
        operands.foreach(requireCharacterString)
        generateConcatWs(ctx, operands)

      case STR_TO_MAP => generateStrToMap(ctx, operands)

      case TRIM => generateTrim(ctx, operands)

      case LTRIM => generateTrimLeft(ctx, operands)

      case RTRIM => generateTrimRight(ctx, operands)

      case CONCAT =>
        val left = operands.head
        val right = operands(1)
        requireCharacterString(left)
        generateArithmeticConcat(ctx, left, right)

      case UUID => generateUuid(ctx, operands)

      case ASCII => generateAscii(ctx, operands.head)

      case ENCODE => generateEncode(ctx, operands.head, operands(1))

      case DECODE => generateDecode(ctx, operands.head, operands(1))

      case INSTR => generateInstr(ctx, operands)

      case PRINT => new PrintCallGen().generate(ctx, operands, returnType)

      case IF =>
        requireBoolean(operands.head)
        new IfCallGen().generate(ctx, operands, returnType)

      // Date/Time & BinaryString Converting -- start

      case TO_DATE if operands.size == 1 && isCharacterString(operands.head.resultType) =>
        methodGen(BuiltInMethods.STRING_TO_DATE)

      case TO_DATE if operands.size == 2 &&
          isCharacterString(operands.head.resultType) &&
          isCharacterString(operands(1).resultType) =>
        methodGen(BuiltInMethods.STRING_TO_DATE_WITH_FORMAT)

      case TO_TIMESTAMP if operands.size == 1 && isCharacterString(operands.head.resultType) =>
        methodGen(BuiltInMethods.STRING_TO_TIMESTAMP)

      case TO_TIMESTAMP if operands.size == 2 &&
          isCharacterString(operands.head.resultType) &&
          isCharacterString(operands(1).resultType) =>
        methodGen(BuiltInMethods.STRING_TO_TIMESTAMP_WITH_FORMAT)

      case UNIX_TIMESTAMP if operands.size == 1 && isCharacterString(operands.head.resultType) =>
        methodGen(BuiltInMethods.UNIX_TIMESTAMP_STR)

      case UNIX_TIMESTAMP if operands.size == 2 &&
          isCharacterString(operands.head.resultType) &&
          isCharacterString(operands(1).resultType) =>
        methodGen(BuiltInMethods.UNIX_TIMESTAMP_FORMAT)

      case DATE_FORMAT if operands.size == 2 &&
          isTimestamp(operands.head.resultType) &&
          isCharacterString(operands(1).resultType) =>
        methodGen(BuiltInMethods.DATE_FORMAT_LONG_STRING)

      case DATE_FORMAT if operands.size == 2 &&
          isTimestampWithLocalZone(operands.head.resultType) &&
          isCharacterString(operands(1).resultType) =>
        methodGen(BuiltInMethods.DATE_FORMAT_LONG_STRING_TIME_ZONE)

      case DATE_FORMAT if operands.size == 2 &&
          isCharacterString(operands.head.resultType) &&
          isCharacterString(operands(1).resultType) =>
        methodGen(BuiltInMethods.DATE_FORMAT_STIRNG_STRING)

      case CONVERT_TZ if operands.size == 3 &&
          isCharacterString(operands.head.resultType) &&
          isCharacterString(operands(1).resultType) &&
          isCharacterString(operands(2).resultType) =>
        methodGen(BuiltInMethods.CONVERT_TZ)

      case _ => null
    }

    Option(generator)
  }

  private def toStringTerms(terms: Seq[String], operands: Seq[GeneratedExpression]) = {
    terms.zipWithIndex.map { case (term, index) =>
      if (isCharacterString(operands(index).resultType)) {
        s"$term.toString()"
      } else {
        term
      }
    }.mkString(",")
  }

  private def safeToStringTerms(terms: Seq[String], operands: Seq[GeneratedExpression]) = {
    terms.zipWithIndex.map { case (term, index) =>
      if (isCharacterString(operands(index).resultType)) {
        s"$STRING_UTIL.safeToString($term)"
      } else {
        term
      }
    }.mkString(",")
  }

  def generateConcat(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression]): GeneratedExpression = {
    generateCallIfArgsNullable(ctx, new VarCharType(VarCharType.MAX_LENGTH), operands) {
      terms => s"$STRING_UTIL.concat(${terms.mkString(", ")})"
    }
  }

  def generateConcatWs(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression]): GeneratedExpression = {
    generateCallIfArgsNullable(ctx, new VarCharType(VarCharType.MAX_LENGTH), operands) {
      terms => s"$STRING_UTIL.concatWs(${terms.mkString(", ")})"
    }
  }

  /**
    * Optimization: use BinaryString equals instead of compare.
    */
  def generateStringEquals(
      ctx: CodeGeneratorContext,
      left: GeneratedExpression,
      right: GeneratedExpression): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, new BooleanType(), Seq(left, right)) {
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
    generateCallIfArgsNotNull(ctx, new BooleanType(), Seq(left, right)) {
      terms => s"!(${terms.head}.equals(${terms(1)}))"
    }
  }

  def generateSubString(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, new VarCharType(VarCharType.MAX_LENGTH), operands) {
      terms => s"$STRING_UTIL.substringSQL(${terms.head}, ${terms.drop(1).mkString(", ")})"
    }
  }

  def generateLeft(
    ctx: CodeGeneratorContext,
    str: GeneratedExpression,
    len: GeneratedExpression): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, new VarCharType(VarCharType.MAX_LENGTH), Seq(str, len)) {
      val emptyString = s"$BINARY_STRING.EMPTY_UTF8"
      terms =>
        s"${terms(1)} <= 0 ? $emptyString :" +
            s" $STRING_UTIL.substringSQL(${terms.head}, 1, ${terms(1)})"
    }
  }

  def generateRight(
      ctx: CodeGeneratorContext,
      str: GeneratedExpression,
      len: GeneratedExpression): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, new VarCharType(VarCharType.MAX_LENGTH), Seq(str, len)) {
      terms =>
        s"""
           |${terms(1)} <= 0 ?
           |  $BINARY_STRING.EMPTY_UTF8 :
           |  ${terms(1)} >= ${terms.head}.numChars() ?
           |  ${terms.head} :
           |  $STRING_UTIL.substringSQL(${terms.head}, -${terms(1)})
       """.stripMargin
    }
  }

  def generateCharLength(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression]): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, new IntType(), operands) {
      terms => s"${terms.head}.numChars()"
    }
  }

  def generateSimilarTo(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctions].getCanonicalName
    generateCallIfArgsNotNull(ctx, new BooleanType(), operands) {
      terms => s"$className.similar(${toStringTerms(terms, operands)})"
    }
  }

  def generateRegexpExtract(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands) {
      terms => s"$className.regexpExtract(${safeToStringTerms(terms, operands)})"
    }
  }

  def generateRegexpReplace(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands) {
      terms => s"$className.regexpReplace(${toStringTerms(terms, operands)})"
    }
  }

  def generateIsDecimal(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateCallIfArgsNullable(ctx, new BooleanType(), operands) {
      terms => s"$className.isDecimal(${safeToStringTerms(terms, operands)})"
    }
  }

  def generateIsDigit(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateCallIfArgsNullable(ctx, new BooleanType(), operands) {
      terms => s"$className.isDigit(${safeToStringTerms(terms, operands)})"
    }
  }

  def generateAscii(
    ctx: CodeGeneratorContext,
    str: GeneratedExpression): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, new IntType(), Seq(str)) {
      terms => s"${terms.head}.getSizeInBytes() <= 0 ? 0 : (int) ${terms.head}.byteAt(0)"
    }
  }

  def generateIsAlpha(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateCallIfArgsNullable(ctx, new BooleanType(), operands) {
      terms => s"$className.isAlpha(${safeToStringTerms(terms, operands)})"
    }
  }

  def generateUpper(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, new VarCharType(VarCharType.MAX_LENGTH), operands) {
      terms => s"${terms.head}.toUpperCase()"
    }
  }

  def generateLower(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, new VarCharType(VarCharType.MAX_LENGTH), operands) {
      terms => s"${terms.head}.toLowerCase()"
    }
  }

  def generateInitcap(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctions].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands) {
      terms => s"$className.initcap(${terms.head}.toString())"
    }
  }

  def generatePosition(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateCallIfArgsNotNull(ctx, new IntType(), operands) {
      terms => s"$className.position(${terms.mkString(",")})"
    }
  }

  def generateLocate(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateCallIfArgsNotNull(ctx, new IntType(), operands) {
      terms => s"$className.position(${terms.mkString(",")})"
    }
  }

  def generateInstr(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateCallIfArgsNotNull(ctx, new IntType(), operands) {
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
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands) {
      terms => s"$className.overlay(${toStringTerms(terms, operands)})"
    }
  }

  def generateArithmeticConcat(
      ctx: CodeGeneratorContext,
      left: GeneratedExpression,
      right: GeneratedExpression): GeneratedExpression = {
    generateStringResultCallIfArgsNotNull(ctx, Seq(left, right)) {
      terms => s"${terms.head}.toString() + String.valueOf(${terms(1)})"
    }
  }

  def generateLpad(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands) {
      terms =>
        s"$className.lpad(${toStringTerms(terms, operands)})"
    }
  }

  def generateRpad(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands) {
      terms =>
        s"$className.rpad(${toStringTerms(terms, operands)})"
    }
  }

  def generateRepeat(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands) {
      terms => s"$className.repeat(${toStringTerms(terms, operands)})"
    }
  }

  def generateReverse(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, new VarCharType(VarCharType.MAX_LENGTH), operands) {
      terms => s"$STRING_UTIL.reverse(${terms.head})"
    }
  }

  def generateReplace(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands) {
      terms => s"$className.replace(${toStringTerms(terms, operands)})"
    }
  }

  def generateSplitIndex(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands) {
      terms => s"$className.splitIndex(${toStringTerms(terms, operands)})"
    }
  }

  def generateKeyValue(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateCallIfArgsNullable(ctx, new VarCharType(VarCharType.MAX_LENGTH), operands) {
      terms => s"$className.keyValue(${terms.mkString(",")})"
    }
  }

  def generateHashCode(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateCallIfArgsNotNull(ctx, new IntType(), operands) {
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
      generateCallIfArgsNotNull(ctx, new VarCharType(VarCharType.MAX_LENGTH), operands) {
        terms =>s"$STRING_UTIL.hash(${terms.head}, $digestTerm)"
      }
    } else {
      val className = classOf[SqlFunctionUtils].getCanonicalName
      generateStringResultCallIfArgsNotNull(ctx, operands) {
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
    val className = classOf[SqlFunctionUtils].getCanonicalName
    if (operands.last.literal) {
      val digestTerm = ctx.addReusableSha2MessageDigest(operands.last)
      if (operands.length == 2) {
        generateCallIfArgsNotNull(ctx, new VarCharType(VarCharType.MAX_LENGTH), operands) {
          terms =>s"$STRING_UTIL.hash(${terms.head}, $digestTerm)"
        }
      } else {
        generateStringResultCallIfArgsNotNull(ctx, operands) {
          terms =>
            s"$className.hash($digestTerm," +
                s"${toStringTerms(terms.dropRight(1), operands.dropRight(1))})"
        }
      }
    } else {
      if (operands.length == 2) {
        generateCallIfArgsNotNull(ctx, new VarCharType(VarCharType.MAX_LENGTH), operands) {
          terms =>
            s"""$STRING_UTIL.hash(${terms.head}, "SHA-" + ${terms.last})"""
        }
      } else {
        generateStringResultCallIfArgsNotNull(ctx, operands) {
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
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands) {
      terms => s"$className.parseUrl(${safeToStringTerms(terms, operands)})"
    }
  }

  def generateFromBase64(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateCallIfArgsNotNull(ctx, new VarCharType(VarCharType.MAX_LENGTH), operands) {
      terms => s"$className.fromBase64(${terms.head})"
    }
  }

  def generateToBase64(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands) {
      terms => s"$className.toBase64(${terms.head})"
    }
  }

  def generateChr(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands) {
      terms => s"$className.chr(${terms.head})"
    }
  }

  def generateRegExp(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateCallIfArgsNotNull(ctx, new BooleanType(), operands) {
      terms => s"$className.regExp(${toStringTerms(terms, operands)})"
    }
  }

  def generateJsonValue(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands) {
      terms =>
        s"$className.jsonValue(${safeToStringTerms(terms, operands)})"
    }
  }

  def generateBin(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    generateStringResultCallIfArgsNotNull(ctx, operands) {
      terms => s"Long.toBinaryString(${terms.head})"
    }
  }

  def generateTrim(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, new VarCharType(VarCharType.MAX_LENGTH), operands) {
      terms =>
        val leading = compareEnum(terms.head, BOTH) || compareEnum(terms.head, LEADING)
        val trailing = compareEnum(terms.head, BOTH) || compareEnum(terms.head, TRAILING)
        val args = s"$leading, $trailing, ${terms(1)}"
        s"$STRING_UTIL.trim(${terms(2)}, $args)"
    }
  }

  def generateTrimLeft(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, new VarCharType(VarCharType.MAX_LENGTH), operands) {
      terms => s"$STRING_UTIL.trimLeft(${terms.mkString(", ")})"
    }
  }

  def generateTrimRight(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, new VarCharType(VarCharType.MAX_LENGTH), operands) {
      terms => s"$STRING_UTIL.trimRight(${terms.mkString(", ")})"
    }
  }

  def generateUuid(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    generateStringResultCallIfArgsNotNull(ctx, operands) {
      terms => s"$className.uuid(${terms.mkString(",")})"
    }
  }

  def generateStrToMap(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression]): GeneratedExpression = {
    val className = classOf[SqlFunctionUtils].getCanonicalName
    val t = new MapType(
      new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH))
    val converter = DataFormatConverters.getConverterForDataType(
      DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
    val converterTerm = ctx.addReusableObject(converter, "mapConverter")
    generateCallIfArgsNotNull(ctx, t, operands) {
      terms =>
        val map = s"$className.strToMap(${toStringTerms(terms, operands)})"
        s"($BINARY_MAP) $converterTerm.toInternal($map)"
    }
  }

  def generateEncode(
      ctx: CodeGeneratorContext,
      str: GeneratedExpression,
      charset: GeneratedExpression): GeneratedExpression = {
    generateCallIfArgsNotNull(
      ctx, new VarBinaryType(VarBinaryType.MAX_LENGTH), Seq(str, charset)) {
      terms => s"${terms.head}.toString().getBytes(${terms(1)}.toString())"
    }
  }

  def generateDecode(
      ctx: CodeGeneratorContext,
      binary: GeneratedExpression,
      charset: GeneratedExpression): GeneratedExpression = {
    generateStringResultCallIfArgsNotNull(ctx, Seq(binary, charset)) {
      terms =>
        s"new String(${terms.head}, ${terms(1)}.toString())"
    }
  }

}
