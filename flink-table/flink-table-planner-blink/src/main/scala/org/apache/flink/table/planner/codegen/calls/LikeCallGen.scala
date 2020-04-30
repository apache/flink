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

import org.apache.flink.table.planner.codegen.CodeGenUtils.{className, newName}
import org.apache.flink.table.planner.codegen.GenerateUtils.generateCallIfArgsNotNull
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.runtime.functions.{SqlLikeChainChecker, SqlLikeUtils}
import org.apache.flink.table.types.logical.{BooleanType, LogicalType}

import org.apache.calcite.runtime.SqlFunctions

import java.util.regex.Pattern

/**
  * Generates Like function call.
  */
class LikeCallGen extends CallGenerator {

  /**
    * Accepts simple LIKE patterns like "abc%".
    */
  private val BEGIN_PATTERN = Pattern.compile("([^%]+)%")

  /**
    * Accepts simple LIKE patterns like "%abc".
    */
  private val END_PATTERN = Pattern.compile("%([^%]+)")

  /**
    * Accepts simple LIKE patterns like "%abc%".
    */
  private val MIDDLE_PATTERN = Pattern.compile("%([^%]+)%")

  /**
    * Accepts simple LIKE patterns like "abc".
    */
  private val NONE_PATTERN = Pattern.compile("[^%]+")

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    if ((operands.size == 2 && operands(1).literal) ||
        (operands.size == 3 && operands(1).literal && operands(2).literal)) {
      generateCallIfArgsNotNull(ctx, returnType, operands) {
        terms =>
          val pattern = operands(1).literalValue.get.toString
          var newPattern: String = pattern
          val allowQuick = if (operands.length == 2) {
            !pattern.contains("_")
          } else {
            val escape = operands(2).literalValue.get.toString
            if (escape.length != 1) {
              throw SqlLikeUtils.invalidEscapeCharacter(escape)
            }
            val escapeChar = escape.charAt(0)
            var matched = true
            var i = 0
            val newBuilder = new StringBuilder
            while (i < pattern.length && matched) {
              val c = pattern.charAt(i)
              if (c == escapeChar) {
                if (i == (pattern.length - 1)) {
                  throw SqlLikeUtils.invalidEscapeSequence(pattern, i)
                }
                val nextChar = pattern.charAt(i + 1)
                if (nextChar == '%') {
                  matched = false
                } else if ((nextChar == '_') || (nextChar == escapeChar)) {
                  newBuilder.append(nextChar)
                  i += 1
                } else {
                  throw SqlLikeUtils.invalidEscapeSequence(pattern, i)
                }
              } else if (c == '_') {
                matched = false
              } else {
                newBuilder.append(c)
              }
              i += 1
            }

            if (matched) {
              newPattern = newBuilder.toString
            }
            matched
          }

          if (allowQuick) {
            val noneMatcher = NONE_PATTERN.matcher(newPattern)
            val beginMatcher = BEGIN_PATTERN.matcher(newPattern)
            val endMatcher = END_PATTERN.matcher(newPattern)
            val middleMatcher = MIDDLE_PATTERN.matcher(newPattern)

            if (noneMatcher.matches()) {
              val reusePattern = ctx.addReusableStringConstants(newPattern)
              s"${terms.head}.equals($reusePattern)"
            } else if (beginMatcher.matches()) {
              val field = ctx.addReusableStringConstants(beginMatcher.group(1))
              s"${terms.head}.startsWith($field)"
            } else if (endMatcher.matches()) {
              val field = ctx.addReusableStringConstants(endMatcher.group(1))
              s"${terms.head}.endsWith($field)"
            } else if (middleMatcher.matches()) {
              val field = ctx.addReusableStringConstants(middleMatcher.group(1))
              s"${terms.head}.contains($field)"
            } else {
              val field = className[SqlLikeChainChecker]
              val checker = newName("likeChainChecker")
              ctx.addReusableMember(s"$field $checker = new $field(${"\""}$newPattern${"\""});")
              s"$checker.check(${terms.head})"
            }
          } else {
            // Complex
            val patternClass = className[Pattern]
            val likeClass = className[SqlLikeUtils]
            val patternName = newName("pattern")
            val escape = if (operands.size == 2) {
              "null"
            } else {
              s"""
                 |"${operands(2).literalValue.get}"
               """.stripMargin
            }
            ctx.addReusableMember(
              s"""
                 |$patternClass $patternName =
                 |  $patternClass.compile(
                 |    $likeClass.sqlToRegexLike("${operands(1).literalValue.get}", $escape));
                 |""".stripMargin)
            s"$patternName.matcher(${terms.head}.toString()).matches()"
          }
      }
    } else {
      generateDynamicLike(ctx, operands)
    }
  }

  def generateDynamicLike(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression]): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, new BooleanType(), operands) {
      terms =>
        val str1 = s"${terms.head}.toString()"
        val str2 = s"${terms(1)}.toString()"
        val clsName = className[SqlFunctions]
        if (terms.length == 2) {
          s"$clsName.like($str1, $str2)"
        } else {
          val str3 = s"${terms(2)}.toString()"
          s"$clsName.like($str1, $str2, $str3)"
        }
    }
  }
}
