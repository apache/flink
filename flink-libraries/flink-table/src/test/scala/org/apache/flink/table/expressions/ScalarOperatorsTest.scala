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

package org.apache.flink.table.expressions

import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils.{ScalarOperatorsTestBase, ShouldNotExecuteFunc}
import org.junit.Test

class ScalarOperatorsTest extends ScalarOperatorsTestBase {

  @Test
  def testCasting(): Unit = {
    // test casting
    // * -> String
    testTableApi('f2.cast(Types.STRING), "f2.cast(STRING)", "1")
    testTableApi('f5.cast(Types.STRING), "f5.cast(STRING)", "1.0")
    testTableApi('f3.cast(Types.STRING), "f3.cast(STRING)", "1")
    testTableApi('f6.cast(Types.STRING), "f6.cast(STRING)", "true")
    // NUMERIC TYPE -> Boolean
    testTableApi('f2.cast(Types.BOOLEAN), "f2.cast(BOOLEAN)", "true")
    testTableApi('f7.cast(Types.BOOLEAN), "f7.cast(BOOLEAN)", "false")
    testTableApi('f3.cast(Types.BOOLEAN), "f3.cast(BOOLEAN)", "true")
    // NUMERIC TYPE -> NUMERIC TYPE
    testTableApi('f2.cast(Types.DOUBLE), "f2.cast(DOUBLE)", "1.0")
    testTableApi('f7.cast(Types.INT), "f7.cast(INT)", "0")
    testTableApi('f3.cast(Types.SHORT), "f3.cast(SHORT)", "1")
    // Boolean -> NUMERIC TYPE
    testTableApi('f6.cast(Types.DOUBLE), "f6.cast(DOUBLE)", "1.0")
    // identity casting
    testTableApi('f2.cast(Types.INT), "f2.cast(INT)", "1")
    testTableApi('f7.cast(Types.DOUBLE), "f7.cast(DOUBLE)", "0.0")
    testTableApi('f3.cast(Types.LONG), "f3.cast(LONG)", "1")
    testTableApi('f6.cast(Types.BOOLEAN), "f6.cast(BOOLEAN)", "true")
    // String -> BASIC TYPE (not String, Date, Void, Character)
    testTableApi('f2.cast(Types.BYTE), "f2.cast(BYTE)", "1")
    testTableApi('f2.cast(Types.SHORT), "f2.cast(SHORT)", "1")
    testTableApi('f2.cast(Types.INT), "f2.cast(INT)", "1")
    testTableApi('f2.cast(Types.LONG), "f2.cast(LONG)", "1")
    testTableApi('f3.cast(Types.DOUBLE), "f3.cast(DOUBLE)", "1.0")
    testTableApi('f3.cast(Types.FLOAT), "f3.cast(FLOAT)", "1.0")
    testTableApi('f5.cast(Types.BOOLEAN), "f5.cast(BOOLEAN)", "true")

    // numeric auto cast in arithmetic
    testTableApi('f0 + 1, "f0 + 1", "2")
    testTableApi('f1 + 1, "f1 + 1", "2")
    testTableApi('f2 + 1L, "f2 + 1L", "2")
    testTableApi('f3 + 1.0f, "f3 + 1.0f", "2.0")
    testTableApi('f3 + 1.0d, "f3 + 1.0d", "2.0")
    testTableApi('f5 + 1, "f5 + 1", "2.0")
    testTableApi('f3 + 1.0d, "f3 + 1.0d", "2.0")
    testTableApi('f4 + 'f0, "f4 + f0", "2.0")

    // numeric auto cast in comparison
    testTableApi(
      'f0 > 0 && 'f1 > 0 && 'f2 > 0L && 'f4 > 0.0f && 'f5 > 0.0d  && 'f3 > 0,
      "f0 > 0 && f1 > 0 && f2 > 0L && f4 > 0.0f && f5 > 0.0d  && f3 > 0",
      "true")
  }

  @Test
  def testShiftLeft(): Unit = {
    testAllApis(
      3.shiftLeft(3),
      "3.shiftLeft(3)",
      "SHIFT_LEFT(3, 3)",
      "24"
    )

    testAllApis(
      2147483647.shiftLeft(-2147483648),
      "2147483647.shiftLeft(-2147483648)",
      "SHIFT_LEFT(2147483647, -2147483648)",
      "2147483647"
    )

    testAllApis(
      -2147483648.shiftLeft(2147483647),
      "-2147483648.shiftLeft(2147483647)",
      "SHIFT_LEFT(-2147483648, 2147483647)",
      "0"
    )

    testAllApis(
      9223372036854775807L.shiftLeft(-2147483648),
      "9223372036854775807L.shiftLeft(-2147483648)",
      "SHIFT_LEFT(9223372036854775807, -2147483648)",
      "9223372036854775807"
    )

    testAllApis(
      'f3.shiftLeft(5),
      "f3.shiftLeft(5)",
      "SHIFT_LEFT(f3, 5)",
      "32"
    )

    testAllApis(
      1.shiftLeft(Null(Types.INT)),
      "1.shiftLeft(Null(INT))",
      "SHIFT_LEFT(1, CAST(NULL AS INT))",
      "null"
    )

    testAllApis(       // test tinyint
      'f0.shiftLeft(20),
      "f0.shiftLeft(20)",
      "SHIFT_LEFT(CAST(1 AS TINYINT), 20)",
      "0"
    )

    testAllApis(      // test smallint
      'f1.shiftLeft(20),
      "f1.shiftLeft(20)",
      "SHIFT_LEFT(CAST(1 AS SMALLINT), 20)",
      "0"
    )

    testAllApis(      // test long
      'f3.shiftLeft(40),
      "f3.shiftLeft(40)",
      "SHIFT_LEFT(CAST(1 AS BIGINT), 40)",
      "1099511627776"
    )

    //special params
    testAllApis(
      3.shiftLeft(-1),
      "3.shiftLeft(-1)",
      "SHIFT_LEFT(3, -1)",
      "-2147483648"
    )

    testAllApis(
      4.shiftLeft(-1),
      "4.shiftLeft(-1)",
      "SHIFT_LEFT(4, -1)",
      "0"
    )

    testAllApis(
      5.shiftLeft(-2),
      "5.shiftLeft(-2)",
      "SHIFT_LEFT(5, -2)",
      "1073741824"
    )

    testAllApis(
      -5.shiftLeft(2),
      "-5.shiftLeft(2)",
      "SHIFT_LEFT(-5, 2)",
      "-20"
    )

    testAllApis(
      -7.shiftLeft(-1),
      "-7.shiftLeft(-1)",
      "SHIFT_LEFT(-7, -1)",
      "-2147483648"
    )

    //special shift test
    testAllApis(
      'f0.shiftLeft(9),
      "f0.shiftLeft(9)",
      "SHIFT_LEFT(f0, 9)",
      "0"
    )

    testAllApis(
      'f0.shiftLeft(33),
      "f0.shiftLeft(33)",
      "SHIFT_LEFT(f0, 33)",
      "2"
    )

    testAllApis(
      'f1.shiftLeft(17),
      "f1.shiftLeft(17)",
      "SHIFT_LEFT(f1, 17)",
      "0"
    )

    testAllApis(
      'f1.shiftLeft(33),
      "f1.shiftLeft(33)",
      "SHIFT_LEFT(f1, 33)",
      "2"
    )

    testAllApis(
      'f2.shiftLeft(17),
      "f2.shiftLeft(17)",
      "SHIFT_LEFT(f2, 17)",
      "131072"
    )

    testAllApis(
      'f2.shiftLeft(33),
      "f2.shiftLeft(33)",
      "SHIFT_LEFT(f2, 33)",
      "2"
    )

    testAllApis(
      'f3.shiftLeft(33),
      "f3.shiftLeft(33)",
      "SHIFT_LEFT(f3, 33)",
      "8589934592"
    )

    testAllApis(
      'f3.shiftLeft(65),
      "f3.shiftLeft(65)",
      "SHIFT_LEFT(f3, 65)",
      "2"
    )
  }

  @Test
  def testShiftRight(): Unit = {
    testAllApis(
      1.shiftRight(1),
      "1.shiftRight(1)",
      "SHIFT_RIGHT(1,1)",
      "0"
    )

    testAllApis(
      21.shiftRight(1),
      "21.shiftRight(1)",
      "SHIFT_RIGHT(21,1)",
      "10"
    )

    testAllApis(
      2147483647.shiftRight(-2147483648),
      "2147483647.shiftRight(-2147483648)",
      "SHIFT_RIGHT(2147483647,-2147483648)",
      "2147483647"
    )

    testAllApis(
      -2147483648.shiftRight(2147483647),
      "-2147483648.shiftRight(2147483647)",
      "SHIFT_RIGHT(-2147483648,2147483647)",
      "-1"
    )

    testAllApis(
      123456789.shiftRight(-2147483648),
      "123456789.shiftRight(-2147483648)",
      "SHIFT_RIGHT(123456789,-2147483648)",
      "123456789"
    )

    testAllApis(
      'f3.shiftRight(1),
      "f3.shiftRight(1)",
      "SHIFT_RIGHT(f3,1)",
      "0"
    )

    testAllApis(
      1.shiftRight(Null(Types.INT)),
      "1.shiftRight(Null(INT))",
      "SHIFT_RIGHT(1, CAST(NULL AS INT))",
      "null"
    )

    val a: Byte = 7
    testAllApis(            // test tinyint
      a.cast(Types.BYTE).shiftRight(1),
      s"$a.cast(BYTE).shiftRight(1)",
      s"SHIFT_RIGHT(CAST($a AS TINYINT),1)",
      "3"
    )

    val b: Short = 100
    testAllApis(            // test smallint
      b.cast(Types.SHORT).shiftRight(1),
      s"$b.cast(SHORT).shiftRight(1)",
      s"SHIFT_RIGHT(CAST($b AS SMALLINT),1)",
      "50"
    )

    val c: Long = 1099511627776L
    testAllApis(            // test long
      c.cast(Types.LONG).shiftRight(30),
      s"${c}L.cast(LONG).shiftRight(30)",
      s"SHIFT_RIGHT(CAST($c AS BIGINT),30)",
      "1024"
    )

    //special params
    testAllApis(
      32.shiftRight(-1),
      "32.shiftRight(-1)",
      "SHIFT_RIGHT(32,-1)",
      "0"
    )

    testAllApis(
      -32.shiftRight(1),
      "-32.shiftRight(1)",
      "SHIFT_RIGHT(-32,1)",
      "-16"
    )

    testAllApis(
      -64.shiftRight(-1),
      "-64.shiftRight(-1)",
      "SHIFT_RIGHT(-64,-1)",
      "-1"
    )

    //special shift test
    val b1 = Byte.MinValue
    testAllApis(
      b1.cast(Types.BYTE).shiftRight(9),
      s"$b1.cast(BYTE).shiftRight(9)",
      s"SHIFT_RIGHT(CAST($b1 AS TINYINT), 9)",
      "-1"
    )

    testAllApis(
      b1.cast(Types.BYTE).shiftRight(33),
      s"$b1.cast(BYTE).shiftRight(33)",
      s"SHIFT_RIGHT(CAST($b1 AS TINYINT), 33)",
      "-64"
    )

    val s1 = Short.MinValue
    testAllApis(
      s1.cast(Types.SHORT).shiftRight(17),
      s"$s1.cast(SHORT).shiftRight(17)",
      s"SHIFT_RIGHT(CAST($s1 AS SMALLINT), 17)",
      "-1"
    )

    testAllApis(
      s1.cast(Types.SHORT).shiftRight(33),
      s"$s1.cast(SHORT).shiftRight(33)",
      s"SHIFT_RIGHT(CAST($s1 AS SMALLINT), 33)",
      "-16384"
    )

    val i1 = Int.MinValue
    testAllApis(
      i1.shiftRight(17),
      s"$i1.shiftRight(17)",
      s"SHIFT_RIGHT($i1, 17)",
      "-16384"
    )

    testAllApis(
      i1.shiftRight(33),
      s"$i1.shiftRight(33)",
      s"SHIFT_RIGHT($i1, 33)",
      "-1073741824"
    )

    val l1 = Long.MinValue
    testAllApis(
      l1.cast(Types.LONG).shiftRight(33),
      s"${l1}L.cast(LONG).shiftRight(33)",
      s"SHIFT_RIGHT(CAST($l1 AS BIGINT), 33)",
      "-1073741824"
    )

    testAllApis(
      l1.cast(Types.LONG).shiftRight(65),
      s"${l1}L.cast(LONG).shiftRight(65)",
      s"SHIFT_RIGHT(CAST($l1 AS BIGINT), 65)",
      "-4611686018427387904"
    )

  }

  @Test
  def testShiftRightUnsigned(): Unit = {
    testAllApis(
      64.shiftRightUnsigned(3),
      "64.shiftRightUnsigned(3)",
      "SHIFT_RIGHT_UNSIGNED(64,3)",
      "8"
    )

    testAllApis(
      -2.shiftRightUnsigned(1),
      "-2.shiftRightUnsigned(1)",
      "SHIFT_RIGHT_UNSIGNED(-2,1)",
      "2147483647"
    )

    testAllApis(
      -5.shiftRightUnsigned(2),
      "-5.shiftRightUnsigned(2)",
      "SHIFT_RIGHT_UNSIGNED(-5,2)",
      "1073741822"
    )

    testAllApis(
      -64.shiftRightUnsigned(-1),
      "-64.shiftRightUnsigned(-1)",
      "SHIFT_RIGHT_UNSIGNED(-64,-1)",
      "1"
    )

    //special shift test
    val b1 = Byte.MinValue
    testAllApis(
      b1.cast(Types.BYTE).shiftRightUnsigned(9),
      s"$b1.cast(BYTE).shiftRightUnsigned(9)",
      s"SHIFT_RIGHT_UNSIGNED(CAST($b1 AS TINYINT), 9)",
      "-1"
    )

    testAllApis(
      b1.cast(Types.BYTE).shiftRightUnsigned(33),
      s"$b1.cast(BYTE).shiftRightUnsigned(33)",
      s"SHIFT_RIGHT_UNSIGNED(CAST($b1 AS TINYINT), 33)",
      "-64"
    )

    val s1 = Short.MinValue
    testAllApis(
      s1.cast(Types.SHORT).shiftRightUnsigned(17),
      s"$s1.cast(SHORT).shiftRightUnsigned(17)",
      s"SHIFT_RIGHT_UNSIGNED(CAST($s1 AS SMALLINT), 17)",
      "32767"
    )

    testAllApis(
      s1.cast(Types.SHORT).shiftRightUnsigned(33),
      s"$s1.cast(SHORT).shiftRightUnsigned(33)",
      s"SHIFT_RIGHT_UNSIGNED(CAST($s1 AS SMALLINT), 33)",
      "-16384"
    )

    val i1 = Int.MinValue
    testAllApis(
      i1.shiftRightUnsigned(17),
      s"$i1.shiftRightUnsigned(17)",
      s"SHIFT_RIGHT_UNSIGNED($i1, 17)",
      "16384"
    )

    testAllApis(
      i1.shiftRightUnsigned(33),
      s"$i1.shiftRightUnsigned(33)",
      s"SHIFT_RIGHT_UNSIGNED($i1, 33)",
      "1073741824"
    )

    val l1 = Long.MinValue
    testAllApis(
      l1.cast(Types.LONG).shiftRightUnsigned(33),
      s"${l1}L.cast(LONG).shiftRightUnsigned(33)",
      s"SHIFT_RIGHT_UNSIGNED(CAST($l1 AS BIGINT), 33)",
      "1073741824"
    )

    testAllApis(
      l1.cast(Types.LONG).shiftRightUnsigned(65),
      s"${l1}L.cast(LONG).shiftRightUnsigned(65)",
      s"SHIFT_RIGHT_UNSIGNED(CAST($l1 AS BIGINT), 65)",
      "4611686018427387904"
    )
  }

  @Test
  def testArithmetic(): Unit = {

    // math arithmetic

    // test addition
    testAllApis(
      1514356320000L + 6000,
      "1514356320000L + 6000",
      "1514356320000 + 6000",
      "1514356326000")

    testAllApis(
      'f20 + 6,
      "f20 + 6",
      "f20 + 6",
      "1514356320006")

    testAllApis(
      'f20 + 'f20,
      "f20 + f20",
      "f20 + f20",
      "3028712640000")

    // test subtraction
    testAllApis(
      1514356320000L - 6000,
      "1514356320000L - 6000",
      "1514356320000 - 6000",
      "1514356314000")

    testAllApis(
      'f20 - 6,
      "f20 - 6",
      "f20 - 6",
      "1514356319994")

    testAllApis(
      'f20 - 'f20,
      "f20 - f20",
      "f20 - f20",
      "0")

    // test multiplication
    testAllApis(
      1514356320000L * 60000,
      "1514356320000L * 60000",
      "1514356320000 * 60000",
      "90861379200000000")

    testAllApis(
      'f20 * 6,
      "f20 * 6",
      "f20 * 6",
      "9086137920000")

    testAllApis(
      'f20 * 'f20,
      "f20 * f20",
      "f20 * f20",
      "2293275063923942400000000")

    // test division
    testAllApis(
      1514356320000L / 60000,
      "1514356320000L / 60000",
      "1514356320000 / 60000",
      "25239272")

    testAllApis(
      'f20 / 6,
      "f20 / 6",
      "f20 / 6",
      "252392720000")

    testAllApis(
      'f20 / 'f20,
      "f20 / f20",
      "f20 / f20",
      "1")

    // test modulo
    testAllApis(
      1514356320000L % 60000,
      "1514356320000L % 60000",
      "mod(1514356320000,60000)",
      "0")

    testAllApis(
      'f20.mod('f20),
      "f20.mod(f20)",
      "mod(f20,f20)",
      "0")

    testAllApis(
      'f20.mod(6),
      "f20.mod(6)",
      "mod(f20,6)",
      "0")

    testAllApis(
      'f3.mod('f2),
      "f3.mod(f2)",
      "MOD(f3, f2)",
      "0")

    testAllApis(
      'f3.mod(3),
      "mod(f3, 3)",
      "MOD(f3, 3)",
      "1")

    // other math arithmetic
    testTableApi(-'f8, "-f8", "-5")
    testTableApi( +'f8, "+f8", "5") // additional space before "+" required because of checkstyle
    testTableApi(3.toExpr + 'f8, "3 + f8", "8")

    // boolean arithmetic: AND
    testTableApi('f6 && true, "f6 && true", "true")      // true && true
    testTableApi('f6 && false, "f6 && false", "false")   // true && false
    testTableApi('f11 && true, "f11 && true", "false")   // false && true
    testTableApi('f11 && false, "f11 && false", "false") // false && false
    testTableApi('f6 && 'f12, "f6 && f12", "null")       // true && null
    testTableApi('f11 && 'f12, "f11 && f12", "false")    // false && null
    testTableApi('f12 && true, "f12 && true", "null")    // null && true
    testTableApi('f12 && false, "f12 && false", "false") // null && false
    testTableApi('f12 && 'f12, "f12 && f12", "null")     // null && null
    testTableApi('f11 && ShouldNotExecuteFunc('f10),     // early out
      "f11 && ShouldNotExecuteFunc(f10)", "false")
    testTableApi('f6 && 'f11 && ShouldNotExecuteFunc('f10),  // early out
      "f6 && f11 && ShouldNotExecuteFunc(f10)", "false")

    // boolean arithmetic: OR
    testTableApi('f6 || true, "f6 || true", "true")      // true || true
    testTableApi('f6 || false, "f6 || false", "true")    // true || false
    testTableApi('f11 || true, "f11 || true", "true")    // false || true
    testTableApi('f11 || false, "f11 || false", "false") // false || false
    testTableApi('f6 || 'f12, "f6 || f12", "true")       // true || null
    testTableApi('f11 || 'f12, "f11 || f12", "null")     // false || null
    testTableApi('f12 || true, "f12 || true", "true")    // null || true
    testTableApi('f12 || false, "f12 || false", "null")  // null || false
    testTableApi('f12 || 'f12, "f12 || f12", "null")     // null || null
    testTableApi('f6 || ShouldNotExecuteFunc('f10),      // early out
      "f6 || ShouldNotExecuteFunc(f10)", "true")
    testTableApi('f11 || 'f6 || ShouldNotExecuteFunc('f10),  // early out
      "f11 || f6 || ShouldNotExecuteFunc(f10)", "true")

    // boolean arithmetic: NOT
    testTableApi(!'f6, "!f6", "false")

    // comparison
    testTableApi('f8 > 'f2, "f8 > f2", "true")
    testTableApi('f8 >= 'f8, "f8 >= f8", "true")
    testTableApi('f8 < 'f2, "f8 < f2", "false")
    testTableApi('f8.isNull, "f8.isNull", "false")
    testTableApi('f8.isNotNull, "f8.isNotNull", "true")
    testTableApi(12.toExpr <= 'f8, "12 <= f8", "false")

    // string arithmetic
    testTableApi(42.toExpr + 'f10 + 'f9, "42 + f10 + f9", "42String10")
    testTableApi('f10 + 'f9, "f10 + f9", "String10")
  }

  @Test
  def testIn(): Unit = {
    testAllApis(
      'f2.in(1, 2, 42),
      "f2.in(1, 2, 42)",
      "f2 IN (1, 2, 42)",
      "true"
    )

    testAllApis(
      'f0.in(BigDecimal(42.0), BigDecimal(2.00), BigDecimal(3.01), BigDecimal(1.000000)),
      "f0.in(42.0p, 2.00p, 3.01p, 1.000000p)",
      "CAST(f0 AS DECIMAL) IN (42.0, 2.00, 3.01, 1.000000)", // SQL would downcast otherwise
      "true"
    )

    testAllApis(
      'f10.in("This is a test String.", "String", "Hello world", "Comment#1"),
      "f10.in('This is a test String.', 'String', 'Hello world', 'Comment#1')",
      "f10 IN ('This is a test String.', 'String', 'Hello world', 'Comment#1')",
      "true"
    )

    testAllApis(
      'f14.in("This is a test String.", "Hello world"),
      "f14.in('This is a test String.', 'Hello world')",
      "f14 IN ('This is a test String.', 'String', 'Hello world')",
      "null"
    )

    testAllApis(
      'f15.in("1996-11-10".toDate),
      "f15.in('1996-11-10'.toDate)",
      "f15 IN (DATE '1996-11-10')",
      "true"
    )

    testAllApis(
      'f15.in("1996-11-10".toDate, "1996-11-11".toDate),
      "f15.in('1996-11-10'.toDate, '1996-11-11'.toDate)",
      "f15 IN (DATE '1996-11-10', DATE '1996-11-11')",
      "true"
    )

    testAllApis(
      'f7.in('f16, 'f17),
      "f7.in(f16, f17)",
      "f7 IN (f16, f17)",
      "true"
    )

    // we do not test SQL here as this expression would be converted into values + join operations
    testTableApi(
      'f7.in(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21),
      "f7.in(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21)",
      "false"
    )

    testTableApi(
      'f10.in("This is a test String.", "String", "Hello world", "Comment#1", Null(Types.STRING)),
      "f10.in('This is a test String.', 'String', 'Hello world', 'Comment#1', Null(STRING))",
      "true"
    )

    testTableApi(
      'f10.in("FAIL", "FAIL"),
      "f10.in('FAIL', 'FAIL')",
      "false"
    )

    testTableApi(
      'f10.in("FAIL", "FAIL", Null(Types.STRING)),
      "f10.in('FAIL', 'FAIL', Null(STRING))",
      "null"
    )
  }

  @Test
  def testOtherExpressions(): Unit = {
    // nested field null type
    testSqlApi("CASE WHEN f13.f1 IS NULL THEN 'a' ELSE 'b' END", "a")
    testSqlApi("CASE WHEN f13.f1 IS NOT NULL THEN 'a' ELSE 'b' END", "b")
    testAllApis('f13.isNull, "f13.isNull", "f13 IS NULL", "false")
    testAllApis('f13.isNotNull, "f13.isNotNull", "f13 IS NOT NULL", "true")
    testAllApis('f13.get("f0").isNull, "f13.get('f0').isNull", "f13.f0 IS NULL", "false")
    testAllApis('f13.get("f0").isNotNull, "f13.get('f0').isNotNull", "f13.f0 IS NOT NULL", "true")
    testAllApis('f13.get("f1").isNull, "f13.get('f1').isNull", "f13.f1 IS NULL", "true")
    testAllApis('f13.get("f1").isNotNull, "f13.get('f1').isNotNull", "f13.f1 IS NOT NULL", "false")

    // array element access test
    testSqlApi("CASE WHEN f18 IS NOT NULL THEN f18[1] ELSE NULL END", "1")
    testSqlApi("CASE WHEN f19 IS NOT NULL THEN f19[1] ELSE NULL END", "(1,a)")

    // boolean literals
    testAllApis(
      true,
      "true",
      "true",
      "true")

    testAllApis(
      false,
      "False",
      "fAlse",
      "false")

    testAllApis(
      true,
      "TrUe",
      "tRuE",
      "true")

    // null
    testAllApis(Null(Types.INT), "Null(INT)", "CAST(NULL AS INT)", "null")
    testAllApis(
      Null(Types.STRING) === "",
      "Null(STRING) === ''",
      "CAST(NULL AS VARCHAR) = ''",
      "null")

    // if
    testTableApi(('f6 && true).?("true", "false"), "(f6 && true).?('true', 'false')", "true")
    testTableApi(false.?("true", "false"), "false.?('true', 'false')", "false")
    testTableApi(
      true.?(true.?(true.?(10, 4), 4), 4),
      "true.?(true.?(true.?(10, 4), 4), 4)",
      "10")
    testTableApi(true, "?((f6 && true), 'true', 'false')", "true")
    testTableApi(
      If('f9 > 'f8, 'f9 - 1, 'f9),
      "If(f9 > f8, f9 - 1, f9)",
      "9"
    )
    testSqlApi("CASE 11 WHEN 1 THEN 'a' ELSE 'b' END", "b")
    testSqlApi("CASE 2 WHEN 1 THEN 'a' ELSE 'b' END", "b")
    testSqlApi(
      "CASE 1 WHEN 1, 2 THEN '1 or 2' WHEN 2 THEN 'not possible' WHEN 3, 2 THEN '3' " +
      "ELSE 'none of the above' END",
      "1 or 2           ")
    testSqlApi("CASE WHEN 'a'='a' THEN 1 END", "1")
    testSqlApi("CASE 2 WHEN 1 THEN 'a' WHEN 2 THEN 'bcd' END", "bcd")
    testSqlApi("CASE f2 WHEN 1 THEN 11 WHEN 2 THEN 4 ELSE NULL END", "11")
    testSqlApi("CASE f7 WHEN 1 THEN 11 WHEN 2 THEN 4 ELSE NULL END", "null")
    testSqlApi("CASE 42 WHEN 1 THEN 'a' WHEN 2 THEN 'bcd' END", "null")
    testSqlApi("CASE 1 WHEN 1 THEN true WHEN 2 THEN false ELSE NULL END", "true")

    // case insensitive as
    testTableApi(5 as 'test, "5 As test", "5")

    // complex expressions
    testTableApi('f0.isNull.isNull, "f0.isNull().isNull", "false")
    testTableApi(
      'f8.abs() + 'f8.abs().abs().abs().abs(),
      "f8.abs() + f8.abs().abs().abs().abs()",
      "10")
    testTableApi(
      'f8.cast(Types.STRING) + 'f8.cast(Types.STRING),
      "f8.cast(STRING) + f8.cast(STRING)",
      "55")
    testTableApi('f8.isNull.cast(Types.INT), "CAST(ISNULL(f8), INT)", "0")
    testTableApi(
      'f8.cast(Types.INT).abs().isNull === false,
      "ISNULL(CAST(f8, INT).abs()) === false",
      "true")
    testTableApi(
      (((true === true) || false).cast(Types.STRING) + "X ").trim(),
      "((((true) === true) || false).cast(STRING) + 'X ').trim",
      "trueX")
    testTableApi(12.isNull, "12.isNull", "false")
  }

  @Test
  def testBetween(): Unit = {
    // between
    testAllApis(
      4.between(Null(Types.INT), 3),
      "4.between(Null(INT), 3)",
      "4 BETWEEN NULL AND 3",
      "false"
    )
    testAllApis(
      4.between(Null(Types.INT), 12),
      "4.between(Null(INT), 12)",
      "4 BETWEEN NULL AND 12",
      "null"
    )
    testAllApis(
      4.between(Null(Types.INT), 3),
      "4.between(Null(INT), 3)",
      "4 BETWEEN 5 AND NULL",
      "false"
    )
    testAllApis(
      4.between(Null(Types.INT), 12),
      "4.between(Null(INT), 12)",
      "4 BETWEEN 0 AND NULL",
      "null"
    )
    testAllApis(
      4.between(1, 3),
      "4.between(1, 3)",
      "4 BETWEEN 1 AND 3",
      "false"
    )
    testAllApis(
      2.between(1, 3),
      "2.between(1, 3)",
      "2 BETWEEN 1 AND 3",
      "true"
    )
    testAllApis(
      2.between(2, 2),
      "2.between(2, 2)",
      "2 BETWEEN 2 AND 2",
      "true"
    )
    testAllApis(
      2.1.between(2.0, 3.0),
      "2.1.between(2.0, 3.0)",
      "2.1 BETWEEN 2.0 AND 3.0",
      "true"
    )
    testAllApis(
      2.1.between(2.1, 2.1),
      "2.1.between(2.1, 2.1)",
      "2.1 BETWEEN 2.1 AND 2.1",
      "true"
    )
    testAllApis(
      "b".between("a", "c"),
      "'b'.between('a', 'c')",
      "'b' BETWEEN 'a' AND 'c'",
      "true"
    )
    testAllApis(
      "b".between("b", "c"),
      "'b'.between('b', 'c')",
      "'b' BETWEEN 'b' AND 'c'",
      "true"
    )
    testAllApis(
      "2018-05-05".toDate.between("2018-05-01".toDate, "2018-05-10".toDate),
      "'2018-05-05'.toDate.between('2018-05-01'.toDate, '2018-05-10'.toDate)",
      "DATE '2018-05-05' BETWEEN DATE '2018-05-01' AND DATE '2018-05-10'",
      "true"
    )

    // not between
    testAllApis(
      2.notBetween(Null(Types.INT), 3),
      "2.notBetween(Null(INT), 3)",
      "2 NOT BETWEEN NULL AND 3",
      "null"
    )
    testAllApis(
      2.notBetween(0, 1),
      "2.notBetween(0, 1)",
      "2 NOT BETWEEN 0 AND 1",
      "true"
    )
    testAllApis(
      2.notBetween(1, 3),
      "2.notBetween(1, 3)",
      "2 NOT BETWEEN 1 AND 3",
      "false"
    )
    testAllApis(
      2.notBetween(2, 2),
      "2.notBetween(2, 2)",
      "2 NOT BETWEEN 2 AND 2",
      "false"
    )
    testAllApis(
      2.1.notBetween(2.0, 3.0),
      "2.1.notBetween(2.0, 3.0)",
      "2.1 NOT BETWEEN 2.0 AND 3.0",
      "false"
    )
    testAllApis(
      2.1.notBetween(2.1, 2.1),
      "2.1.notBetween(2.1, 2.1)",
      "2.1 NOT BETWEEN 2.1 AND 2.1",
      "false"
    )
    testAllApis(
      "b".notBetween("a", "c"),
      "'b'.notBetween('a', 'c')",
      "'b' NOT BETWEEN 'a' AND 'c'",
      "false"
    )
    testAllApis(
      "b".notBetween("b", "c"),
      "'b'.notBetween('b', 'c')",
      "'b' NOT BETWEEN 'b' AND 'c'",
      "false"
    )
    testAllApis(
      "2018-05-05".toDate.notBetween("2018-05-01".toDate, "2018-05-10".toDate),
      "'2018-05-05'.toDate.notBetween('2018-05-01'.toDate, '2018-05-10'.toDate)",
      "DATE '2018-05-05' NOT BETWEEN DATE '2018-05-01' AND DATE '2018-05-10'",
      "false"
    )
  }
}
