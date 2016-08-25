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

package org.apache.flink.api.table.expressions

import java.sql.{Date, Time, Timestamp}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.expressions.utils.ExpressionTestBase
import org.apache.flink.api.table.typeutils.RowTypeInfo
import org.apache.flink.api.table.{Row, Types}
import org.junit.Test

class ScalarFunctionsTest extends ExpressionTestBase {

  // ----------------------------------------------------------------------------------------------
  // String functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testSubstring(): Unit = {
    testAllApis(
      'f0.substring(2),
      "f0.substring(2)",
      "SUBSTRING(f0, 2)",
      "his is a test String.")

    testAllApis(
      'f0.substring(2, 5),
      "f0.substring(2, 5)",
      "SUBSTRING(f0, 2, 5)",
      "his i")

    testAllApis(
      'f0.substring(1, 'f7),
      "f0.substring(1, f7)",
      "SUBSTRING(f0, 1, f7)",
      "Thi")

    testSqlApi(
      "SUBSTRING(f0 FROM 2 FOR 1)",
      "h")

    testSqlApi(
      "SUBSTRING(f0 FROM 2)",
      "his is a test String.")
  }

  @Test
  def testTrim(): Unit = {
    testAllApis(
      'f8.trim(),
      "f8.trim()",
      "TRIM(f8)",
      "This is a test String.")

    testAllApis(
      'f8.trim(removeLeading = true, removeTrailing = true, " "),
      "trim(f8)",
      "TRIM(f8)",
      "This is a test String.")

    testAllApis(
      'f8.trim(removeLeading = false, removeTrailing = true, " "),
      "f8.trim(TRAILING, ' ')",
      "TRIM(TRAILING FROM f8)",
      " This is a test String.")

    testAllApis(
      'f0.trim(removeLeading = true, removeTrailing = true, "."),
      "trim(BOTH, '.', f0)",
      "TRIM(BOTH '.' FROM f0)",
      "This is a test String")
  }

  @Test
  def testCharLength(): Unit = {
    testAllApis(
      'f0.charLength(),
      "f0.charLength()",
      "CHAR_LENGTH(f0)",
      "22")

    testAllApis(
      'f0.charLength(),
      "charLength(f0)",
      "CHARACTER_LENGTH(f0)",
      "22")
  }

  @Test
  def testUpperCase(): Unit = {
    testAllApis(
      'f0.upperCase(),
      "f0.upperCase()",
      "UPPER(f0)",
      "THIS IS A TEST STRING.")
  }

  @Test
  def testLowerCase(): Unit = {
    testAllApis(
      'f0.lowerCase(),
      "f0.lowerCase()",
      "LOWER(f0)",
      "this is a test string.")
  }

  @Test
  def testInitCap(): Unit = {
    testAllApis(
      'f0.initCap(),
      "f0.initCap()",
      "INITCAP(f0)",
      "This Is A Test String.")
  }

  @Test
  def testConcat(): Unit = {
    testAllApis(
      'f0 + 'f0,
      "f0 + f0",
      "f0||f0",
      "This is a test String.This is a test String.")
  }

  @Test
  def testLike(): Unit = {
    testAllApis(
      'f0.like("Th_s%"),
      "f0.like('Th_s%')",
      "f0 LIKE 'Th_s%'",
      "true")

    testAllApis(
      'f0.like("%is a%"),
      "f0.like('%is a%')",
      "f0 LIKE '%is a%'",
      "true")
  }

  @Test
  def testNotLike(): Unit = {
    testAllApis(
      !'f0.like("Th_s%"),
      "!f0.like('Th_s%')",
      "f0 NOT LIKE 'Th_s%'",
      "false")

    testAllApis(
      !'f0.like("%is a%"),
      "!f0.like('%is a%')",
      "f0 NOT LIKE '%is a%'",
      "false")
  }

  @Test
  def testSimilar(): Unit = {
    testAllApis(
      'f0.similar("_*"),
      "f0.similar('_*')",
      "f0 SIMILAR TO '_*'",
      "true")

    testAllApis(
      'f0.similar("This (is)? a (test)+ Strin_*"),
      "f0.similar('This (is)? a (test)+ Strin_*')",
      "f0 SIMILAR TO 'This (is)? a (test)+ Strin_*'",
      "true")
  }

  @Test
  def testNotSimilar(): Unit = {
    testAllApis(
      !'f0.similar("_*"),
      "!f0.similar('_*')",
      "f0 NOT SIMILAR TO '_*'",
      "false")

    testAllApis(
      !'f0.similar("This (is)? a (test)+ Strin_*"),
      "!f0.similar('This (is)? a (test)+ Strin_*')",
      "f0 NOT SIMILAR TO 'This (is)? a (test)+ Strin_*'",
      "false")
  }

  @Test
  def testMod(): Unit = {
    testAllApis(
      'f4.mod('f7),
      "f4.mod(f7)",
      "MOD(f4, f7)",
      "2")

    testAllApis(
      'f4.mod(3),
      "mod(f4, 3)",
      "MOD(f4, 3)",
      "2")

    testAllApis(
      'f4 % 3,
      "mod(44, 3)",
      "MOD(44, 3)",
      "2")

  }

  @Test
  def testExp(): Unit = {
    testAllApis(
      'f2.exp(),
      "f2.exp()",
      "EXP(f2)",
      math.exp(42.toByte).toString)

    testAllApis(
      'f3.exp(),
      "f3.exp()",
      "EXP(f3)",
      math.exp(43.toShort).toString)

    testAllApis(
      'f4.exp(),
      "f4.exp()",
      "EXP(f4)",
      math.exp(44.toLong).toString)

    testAllApis(
      'f5.exp(),
      "f5.exp()",
      "EXP(f5)",
      math.exp(4.5.toFloat).toString)

    testAllApis(
      'f6.exp(),
      "f6.exp()",
      "EXP(f6)",
      math.exp(4.6).toString)

    testAllApis(
      'f7.exp(),
      "exp(3)",
      "EXP(3)",
      math.exp(3).toString)

    testAllApis(
      'f7.exp(),
      "exp(3)",
      "EXP(3)",
      math.exp(3).toString)
  }

  @Test
  def testLog10(): Unit = {
    testAllApis(
      'f2.log10(),
      "f2.log10()",
      "LOG10(f2)",
      math.log10(42.toByte).toString)

    testAllApis(
      'f3.log10(),
      "f3.log10()",
      "LOG10(f3)",
      math.log10(43.toShort).toString)

    testAllApis(
      'f4.log10(),
      "f4.log10()",
      "LOG10(f4)",
      math.log10(44.toLong).toString)

    testAllApis(
      'f5.log10(),
      "f5.log10()",
      "LOG10(f5)",
      math.log10(4.5.toFloat).toString)

    testAllApis(
      'f6.log10(),
      "f6.log10()",
      "LOG10(f6)",
      math.log10(4.6).toString)
  }

  @Test
  def testPower(): Unit = {
    testAllApis(
      'f2.power('f7),
      "f2.power(f7)",
      "POWER(f2, f7)",
      math.pow(42.toByte, 3).toString)

    testAllApis(
      'f3.power('f6),
      "f3.power(f6)",
      "POWER(f3, f6)",
      math.pow(43.toShort, 4.6D).toString)

    testAllApis(
      'f4.power('f5),
      "f4.power(f5)",
      "POWER(f4, f5)",
      math.pow(44.toLong, 4.5.toFloat).toString)

    testAllApis(
      'f4.power('f5),
      "f4.power(f5)",
      "POWER(f4, f5)",
      math.pow(44.toLong, 4.5.toFloat).toString)
  }

  @Test
  def testLn(): Unit = {
    testAllApis(
      'f2.ln(),
      "f2.ln()",
      "LN(f2)",
      math.log(42.toByte).toString)

    testAllApis(
      'f3.ln(),
      "f3.ln()",
      "LN(f3)",
      math.log(43.toShort).toString)

    testAllApis(
      'f4.ln(),
      "f4.ln()",
      "LN(f4)",
      math.log(44.toLong).toString)

    testAllApis(
      'f5.ln(),
      "f5.ln()",
      "LN(f5)",
      math.log(4.5.toFloat).toString)

    testAllApis(
      'f6.ln(),
      "f6.ln()",
      "LN(f6)",
      math.log(4.6).toString)
  }

  @Test
  def testAbs(): Unit = {
    testAllApis(
      'f2.abs(),
      "f2.abs()",
      "ABS(f2)",
      "42")

    testAllApis(
      'f3.abs(),
      "f3.abs()",
      "ABS(f3)",
      "43")

    testAllApis(
      'f4.abs(),
      "f4.abs()",
      "ABS(f4)",
      "44")

    testAllApis(
      'f5.abs(),
      "f5.abs()",
      "ABS(f5)",
      "4.5")

    testAllApis(
      'f6.abs(),
      "f6.abs()",
      "ABS(f6)",
      "4.6")

    testAllApis(
      'f9.abs(),
      "f9.abs()",
      "ABS(f9)",
      "42")

    testAllApis(
      'f10.abs(),
      "f10.abs()",
      "ABS(f10)",
      "43")

    testAllApis(
      'f11.abs(),
      "f11.abs()",
      "ABS(f11)",
      "44")

    testAllApis(
      'f12.abs(),
      "f12.abs()",
      "ABS(f12)",
      "4.5")

    testAllApis(
      'f13.abs(),
      "f13.abs()",
      "ABS(f13)",
      "4.6")

    testAllApis(
      'f15.abs(),
      "f15.abs()",
      "ABS(f15)",
      "1231.1231231321321321111")
  }

  @Test
  def testArithmeticFloorCeil(): Unit = {
    testAllApis(
      'f5.floor(),
      "f5.floor()",
      "FLOOR(f5)",
      "4.0")

    testAllApis(
     'f5.ceil(),
      "f5.ceil()",
      "CEIL(f5)",
      "5.0")

    testAllApis(
      'f3.floor(),
      "f3.floor()",
      "FLOOR(f3)",
      "43")

    testAllApis(
      'f3.ceil(),
      "f3.ceil()",
      "CEIL(f3)",
      "43")

    testAllApis(
      'f15.floor(),
      "f15.floor()",
      "FLOOR(f15)",
      "-1232")

    testAllApis(
      'f15.ceil(),
      "f15.ceil()",
      "CEIL(f15)",
      "-1231")
  }

  @Test
  def testExtract(): Unit = {
    testAllApis(
      'f16.extract(TimeIntervalUnit.YEAR),
      "f16.extract(YEAR)",
      "EXTRACT(YEAR FROM f16)",
      "1996")

    testAllApis(
      'f16.extract(TimeIntervalUnit.MONTH),
      "extract(f16, MONTH)",
      "EXTRACT(MONTH FROM f16)",
      "11")

    testAllApis(
      'f16.extract(TimeIntervalUnit.DAY),
      "f16.extract(DAY)",
      "EXTRACT(DAY FROM f16)",
      "10")

    testAllApis(
      'f18.extract(TimeIntervalUnit.YEAR),
      "f18.extract(YEAR)",
      "EXTRACT(YEAR FROM f18)",
      "1996")

    testAllApis(
      'f18.extract(TimeIntervalUnit.MONTH),
      "f18.extract(MONTH)",
      "EXTRACT(MONTH FROM f18)",
      "11")

    testAllApis(
      'f18.extract(TimeIntervalUnit.DAY),
      "f18.extract(DAY)",
      "EXTRACT(DAY FROM f18)",
      "10")

    testAllApis(
      'f18.extract(TimeIntervalUnit.HOUR),
      "f18.extract(HOUR)",
      "EXTRACT(HOUR FROM f18)",
      "6")

    testAllApis(
      'f17.extract(TimeIntervalUnit.HOUR),
      "f17.extract(HOUR)",
      "EXTRACT(HOUR FROM f17)",
      "6")

    testAllApis(
      'f18.extract(TimeIntervalUnit.MINUTE),
      "f18.extract(MINUTE)",
      "EXTRACT(MINUTE FROM f18)",
      "55")

    testAllApis(
      'f17.extract(TimeIntervalUnit.MINUTE),
      "f17.extract(MINUTE)",
      "EXTRACT(MINUTE FROM f17)",
      "55")

    testAllApis(
      'f18.extract(TimeIntervalUnit.SECOND),
      "f18.extract(SECOND)",
      "EXTRACT(SECOND FROM f18)",
      "44")

    testAllApis(
      'f17.extract(TimeIntervalUnit.SECOND),
      "f17.extract(SECOND)",
      "EXTRACT(SECOND FROM f17)",
      "44")

    testAllApis(
      'f19.extract(TimeIntervalUnit.DAY),
      "f19.extract(DAY)",
      "EXTRACT(DAY FROM f19)",
      "16979")

    testAllApis(
      'f19.extract(TimeIntervalUnit.HOUR),
      "f19.extract(HOUR)",
      "EXTRACT(HOUR FROM f19)",
      "7")

    testAllApis(
      'f19.extract(TimeIntervalUnit.MINUTE),
      "f19.extract(MINUTE)",
      "EXTRACT(MINUTE FROM f19)",
      "23")

    testAllApis(
      'f19.extract(TimeIntervalUnit.SECOND),
      "f19.extract(SECOND)",
      "EXTRACT(SECOND FROM f19)",
      "33")

    testAllApis(
      'f20.extract(TimeIntervalUnit.MONTH),
      "f20.extract(MONTH)",
      "EXTRACT(MONTH FROM f20)",
      "1")

    testAllApis(
      'f20.extract(TimeIntervalUnit.YEAR),
      "f20.extract(YEAR)",
      "EXTRACT(YEAR FROM f20)",
      "2")
  }

  // ----------------------------------------------------------------------------------------------

  def testData = {
    val testData = new Row(21)
    testData.setField(0, "This is a test String.")
    testData.setField(1, true)
    testData.setField(2, 42.toByte)
    testData.setField(3, 43.toShort)
    testData.setField(4, 44.toLong)
    testData.setField(5, 4.5.toFloat)
    testData.setField(6, 4.6)
    testData.setField(7, 3)
    testData.setField(8, " This is a test String. ")
    testData.setField(9, -42.toByte)
    testData.setField(10, -43.toShort)
    testData.setField(11, -44.toLong)
    testData.setField(12, -4.5.toFloat)
    testData.setField(13, -4.6)
    testData.setField(14, -3)
    testData.setField(15, BigDecimal("-1231.1231231321321321111").bigDecimal)
    testData.setField(16, Date.valueOf("1996-11-10"))
    testData.setField(17, Time.valueOf("06:55:44"))
    testData.setField(18, Timestamp.valueOf("1996-11-10 06:55:44.333"))
    testData.setField(19, 1467012213000L) // +16979 07:23:33.000
    testData.setField(20, 25) // +2-01
    testData
  }

  def typeInfo = {
    new RowTypeInfo(Seq(
      Types.STRING,
      Types.BOOLEAN,
      Types.BYTE,
      Types.SHORT,
      Types.LONG,
      Types.FLOAT,
      Types.DOUBLE,
      Types.INT,
      Types.STRING,
      Types.BYTE,
      Types.SHORT,
      Types.LONG,
      Types.FLOAT,
      Types.DOUBLE,
      Types.INT,
      Types.DECIMAL,
      Types.DATE,
      Types.TIME,
      Types.TIMESTAMP,
      Types.INTERVAL_MILLIS,
      Types.INTERVAL_MONTHS)).asInstanceOf[TypeInformation[Any]]
  }
}
