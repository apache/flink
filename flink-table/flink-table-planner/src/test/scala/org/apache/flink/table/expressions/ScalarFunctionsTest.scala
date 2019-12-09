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
import org.apache.flink.table.expressions.utils.ScalarTypesTestBase
import org.junit.Test

class ScalarFunctionsTest extends ScalarTypesTestBase {

  // ----------------------------------------------------------------------------------------------
  // String functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testOverlay(): Unit = {
    testAllApis(
      "xxxxxtest".overlay("xxxx", 6),
      "'xxxxxtest'.overlay('xxxx', 6)",
      "OVERLAY('xxxxxtest' PLACING 'xxxx' FROM 6)",
      "xxxxxxxxx")

    testAllApis(
      "xxxxxtest".overlay("xxxx", 6, 2),
      "'xxxxxtest'.overlay('xxxx', 6, 2)",
      "OVERLAY('xxxxxtest' PLACING 'xxxx' FROM 6 FOR 2)",
      "xxxxxxxxxst")
  }

  @Test
  def testPosition(): Unit = {
    testAllApis(
      "test".position("xxxtest"),
      "'test'.position('xxxtest')",
      "POSITION('test' IN 'xxxtest')",
      "4")

    testAllApis(
      "testx".position("xxxtest"),
      "'testx'.position('xxxtest')",
      "POSITION('testx' IN 'xxxtest')",
      "0")
  }

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

    testAllApis(
      'f0.substring(1.cast(Types.BYTE), 'f7),
      "f0.substring(1.cast(BYTE), f7)",
      "SUBSTRING(f0, CAST(1 AS TINYINT), f7)",
      "Thi")

    testSqlApi(
      "SUBSTRING(f0 FROM 2 FOR 1)",
      "h")

    testSqlApi(
      "SUBSTRING(f0 FROM 2)",
      "his is a test String.")
  }

  @Test
  def testReplace(): Unit = {
    testAllApis(
      'f0.replace(" ", "_"),
      "f0.replace(' ', '_')",
      "REPLACE(f0, ' ', '_')",
      "This_is_a_test_String.")

    testAllApis(
      'f0.replace("i", ""),
      "f0.replace('i', '')",
      "REPLACE(f0, 'i', '')",
      "Ths s a test Strng.")

    testAllApis(
      'f33.replace("i", ""),
      "f33.replace('i', '')",
      "REPLACE(f33, 'i', '')",
      "null")

    testAllApis(
      'f0.replace(nullOf(Types.STRING), ""),
      "f0.replace(nullOf(STRING), '')",
      "REPLACE(f0, NULLIF('', ''), '')",
      "null")

    testAllApis(
      'f0.replace(" ", nullOf(Types.STRING)),
      "f0.replace(' ', nullOf(STRING))",
      "REPLACE(f0, ' ', NULLIF('', ''))",
      "null")
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
  def testLikeWithEscape(): Unit = {
    testSqlApi(
      "f23 LIKE '&%Th_s%' ESCAPE '&'",
      "true")

    testSqlApi(
      "f23 LIKE '&%%is a%' ESCAPE '&'",
      "true")

    testSqlApi(
      "f0 LIKE 'Th_s%' ESCAPE '&'",
      "true")

    testSqlApi(
      "f0 LIKE '%is a%' ESCAPE '&'",
      "true")
  }

  @Test
  def testNotLikeWithEscape(): Unit = {
    testSqlApi(
      "f23 NOT LIKE '&%Th_s%' ESCAPE '&'",
      "false")

    testSqlApi(
      "f23 NOT LIKE '&%%is a%' ESCAPE '&'",
      "false")

    testSqlApi(
      "f0 NOT LIKE 'Th_s%' ESCAPE '&'",
      "false")

    testSqlApi(
      "f0 NOT LIKE '%is a%' ESCAPE '&'",
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
  def testSimilarWithEscape(): Unit = {
    testSqlApi(
      "f24 SIMILAR TO '&*&__*' ESCAPE '&'",
      "true")

    testSqlApi(
      "f0 SIMILAR TO '_*' ESCAPE '&'",
      "true")

    testSqlApi(
      "f24 SIMILAR TO '&*&_This (is)? a (test)+ Strin_*' ESCAPE '&'",
      "true")

    testSqlApi(
      "f0 SIMILAR TO 'This (is)? a (test)+ Strin_*' ESCAPE '&'",
      "true")
  }

  @Test
  def testNotSimilarWithEscape(): Unit = {
    testSqlApi(
      "f24 NOT SIMILAR TO '&*&__*' ESCAPE '&'",
      "false")

    testSqlApi(
      "f0 NOT SIMILAR TO '_*' ESCAPE '&'",
      "false")

    testSqlApi(
      "f24 NOT SIMILAR TO '&*&_This (is)? a (test)+ Strin_*' ESCAPE '&'",
      "false")

    testSqlApi(
      "f0 NOT SIMILAR TO 'This (is)? a (test)+ Strin_*' ESCAPE '&'",
      "false")
  }

  @Test
  def testMultiConcat(): Unit = {
    testAllApis(concat("xx", 'f33), "concat('xx', f33)", "CONCAT('xx', f33)", "null")
    testAllApis(
      concat("AA", "BB", "CC", "---"),
      "concat('AA','BB','CC','---')",
      "CONCAT('AA','BB','CC','---')",
      "AABBCC---")
    testAllApis(
      concat("x~x", "b~b", "c~~~~c", "---"),
      "concat('x~x','b~b','c~~~~c','---')",
      "CONCAT('x~x','b~b','c~~~~c','---')",
      "x~xb~bc~~~~c---")
  }

  @Test
  def testConcatWs(): Unit = {
    testAllApis(
      concat_ws('f33, "AA"),
      "concat_ws(f33, 'AA')",
      "CONCAT_WS(f33, 'AA')",
      "null")
    testAllApis(
      concat_ws("~~~~", "AA"),
      "concat_ws('~~~~','AA')",
      "concat_ws('~~~~','AA')",
      "AA")
    testAllApis(
      concat_ws("~", "AA", "BB"),
      "concat_ws('~','AA','BB')",
      "concat_ws('~','AA','BB')",
      "AA~BB")
    testAllApis(
      concat_ws("~", 'f33, "AA", "BB", "", 'f33, "CC"),
      "concat_ws('~',f33, 'AA','BB','',f33, 'CC')",
      "concat_ws('~',f33, 'AA','BB','',f33, 'CC')",
      "AA~BB~~CC")
    testAllApis(
      concat_ws("~~~~", "Flink", 'f33, "xx", 'f33, 'f33),
      "concat_ws('~~~~','Flink', f33, 'xx', f33, f33)",
      "CONCAT_WS('~~~~','Flink', f33, 'xx', f33, f33)",
      "Flink~~~~xx")
  }

  @Test
  def testLPad(): Unit = {
    testSqlApi("LPAD('hi',4,'??')", "??hi")
    testSqlApi("LPAD('hi',1,'??')", "h")
    testSqlApi("LPAD('',1,'??')", "?")
    testSqlApi("LPAD('',30,'??')", "??????????????????????????????")
    testSqlApi("LPAD('111',-2,'??')", "null")
    testSqlApi("LPAD(f33,1,'??')", "null")
    testSqlApi("LPAD('\u0061\u0062',1,'??')", "a") // the unicode of ab is \u0061\u0062
    testSqlApi("LPAD('⎨⎨',1,'??')", "⎨")
    testSqlApi("LPAD('äääääääää',2,'??')", "ää")
    testSqlApi("LPAD('äääääääää',10,'??')", "?äääääääää")
    testSqlApi("LPAD('Hello', -1, 'x') IS NULL", "true")
    testSqlApi("LPAD('Hello', -1, 'x') IS NOT NULL", "false")

    testAllApis(
      "äää".lpad(13, "12345"),
      "'äää'.lpad(13, '12345')",
      "LPAD('äää',13,'12345')",
      "1234512345äää")
  }

  @Test
  def testRPad(): Unit = {
    testSqlApi("RPAD('hi',4,'??')", "hi??")
    testSqlApi("RPAD('hi',1,'??')", "h")
    testSqlApi("RPAD('',1,'??')", "?")
    testSqlApi("RPAD('1',30,'??')", "1?????????????????????????????")
    testSqlApi("RPAD('111',-2,'??')", "null")
    testSqlApi("RPAD(f33,1,'??')", "null")
    testSqlApi("RPAD('\u0061\u0062',1,'??')", "a") // the unicode of ab is \u0061\u0062
    testSqlApi("RPAD('üö',1,'??')", "ü")

    testAllApis(
      "äää".rpad(13, "12345"),
      "'äää'.rpad(13, '12345')",
      "RPAD('äää',13,'12345')",
      "äää1234512345")
  }

  @Test
  def testHex(): Unit = {
    testAllApis(
      100.hex(),
      "100.hex()",
      "HEX(100)",
      "64")

    testAllApis(
      'f2.hex(),
      "f2.hex()",
      "HEX(f2)",
      "2A")

    testAllApis(
      nullOf(Types.BYTE).hex(),
      "hex(nullOf(BYTE))",
      "HEX(CAST(NULL AS TINYINT))",
      "null")

    testAllApis(
      'f3.hex(),
      "f3.hex()",
      "HEX(f3)",
      "2B")

    testAllApis(
      'f4.hex(),
      "f4.hex()",
      "HEX(f4)",
      "2C")

    testAllApis(
      'f7.hex(),
      "f7.hex()",
      "HEX(f7)",
      "3")

    testAllApis(
      12.hex(),
      "12.hex()",
      "HEX(12)",
      "C")

    testAllApis(
      10.hex(),
      "10.hex()",
      "HEX(10)",
      "A")

    testAllApis(
      0.hex(),
      "0.hex()",
      "HEX(0)",
      "0")

    testAllApis(
      "ö".hex(),
      "'ö'.hex()",
      "HEX('ö')",
      "C3B6")

    testAllApis(
      'f32.hex(),
      "f32.hex()",
      "HEX(f32)",
      "FFFFFFFFFFFFFFFF")

    testAllApis(
      'f0.hex(),
      "f0.hex()",
      "HEX(f0)",
      "546869732069732061207465737420537472696E672E")

    testAllApis(
      'f8.hex(),
      "f8.hex()",
      "HEX(f8)",
      "20546869732069732061207465737420537472696E672E20")

    testAllApis(
      'f23.hex(),
      "f23.hex()",
      "HEX(f23)",
      "25546869732069732061207465737420537472696E672E")

    testAllApis(
      'f24.hex(),
      "f24.hex()",
      "HEX(f24)",
      "2A5F546869732069732061207465737420537472696E672E")

    testAllApis(
      "你好".hex(),
      "'你好'.hex()",
      "HEX('你好')",
      "E4BDA0E5A5BD"
    )
  }

  @Test
  def testBin(): Unit = {

    testAllApis(
      nullOf(Types.BYTE).bin(),
      "bin(nullOf(BYTE))",
      "BIN((CAST(NULL AS TINYINT)))",
      "null")

    testAllApis(
      'f2.bin(),
      "f2.bin()",
      "BIN(f2)",
      "101010")

    testAllApis(
      'f3.bin(),
      "f3.bin()",
      "BIN(f3)",
      "101011")

    testAllApis(
      'f4.bin(),
      "f4.bin()",
      "BIN(f4)",
      "101100")

    testAllApis(
      'f7.bin(),
      "f7.bin()",
      "BIN(f7)",
      "11")

    testAllApis(
      12.bin(),
      "12.bin()",
      "BIN(12)",
      "1100")

    testAllApis(
      10.bin(),
      "10.bin()",
      "BIN(10)",
      "1010")

    testAllApis(
      0.bin(),
      "0.bin()",
      "BIN(0)",
      "0")

    testAllApis(
      'f32.bin(),
      "f32.bin()",
      "BIN(f32)",
      "1111111111111111111111111111111111111111111111111111111111111111")
  }

  @Test
  def testRegexpReplace(): Unit = {

    testAllApis(
      "foobar".regexpReplace("oo|ar", "abc"),
      "'foobar'.regexpReplace('oo|ar', 'abc')",
      "regexp_replace('foobar', 'oo|ar', 'abc')",
      "fabcbabc")

    testAllApis(
      "foofar".regexpReplace("^f", ""),
      "'foofar'.regexpReplace('^f', '')",
      "regexp_replace('foofar', '^f', '')",
      "oofar")

    testAllApis(
      "foobar".regexpReplace("^f*.*r$", ""),
      "'foobar'.regexpReplace('^f*.*r$', '')",
      "regexp_replace('foobar', '^f*.*r$', '')",
      "")

    testAllApis(
      "foo1bar2".regexpReplace("\\d", ""),
      "'foo1bar2'.regexpReplace('\\d', '')",
      "regexp_replace('foobar', '\\d', '')",
      "foobar")

    testAllApis(
      "foobar".regexpReplace("\\w", ""),
      "'foobar'.regexpReplace('\\w', '')",
      "regexp_replace('foobar', '\\w', '')",
      "")

    testAllApis(
      "fooobar".regexpReplace("oo", "$"),
      "'fooobar'.regexpReplace('oo', '$')",
      "regexp_replace('fooobar', 'oo', '$')",
      "f$obar")

    testAllApis(
      "foobar".regexpReplace("oo", "\\"),
      "'foobar'.regexpReplace('oo', '\\')",
      "regexp_replace('foobar', 'oo', '\\')",
      "f\\bar")

    testAllApis(
      'f33.regexpReplace("oo|ar", ""),
      "f33.regexpReplace('oo|ar', '')",
      "REGEXP_REPLACE(f33, 'oo|ar', '')",
      "null")

    testAllApis(
      "foobar".regexpReplace('f33, ""),
      "'foobar'.regexpReplace(f33, '')",
      "REGEXP_REPLACE('foobar', f33, '')",
      "null")

    testAllApis(
      "foobar".regexpReplace("oo|ar", 'f33),
      "'foobar'.regexpReplace('oo|ar', f33)",
      "REGEXP_REPLACE('foobar', 'oo|ar', f33)",
      "null")

    // This test was added for the null literal problem in string expression parsing (FLINK-10463).
    testAllApis(
      nullOf(Types.STRING).regexpReplace("oo|ar", 'f33),
      "nullOf(STRING).regexpReplace('oo|ar', f33)",
      "REGEXP_REPLACE(CAST(NULL AS VARCHAR), 'oo|ar', f33)",
      "null")
  }

  @Test
  def testRegexpExtract(): Unit = {
    testAllApis(
      "foothebar".regexpExtract("foo(.*?)(bar)", 2),
      "'foothebar'.regexpExtract('foo(.*?)(bar)', 2)",
      "REGEXP_EXTRACT('foothebar', 'foo(.*?)(bar)', 2)",
      "bar")

    testAllApis(
      "foothebar".regexpExtract("foo(.*?)(bar)", 0),
      "'foothebar'.regexpExtract('foo(.*?)(bar)', 0)",
      "REGEXP_EXTRACT('foothebar', 'foo(.*?)(bar)', 0)",
      "foothebar")

    testAllApis(
      "foothebar".regexpExtract("foo(.*?)(bar)", 1),
      "'foothebar'.regexpExtract('foo(.*?)(bar)', 1)",
      "REGEXP_EXTRACT('foothebar', 'foo(.*?)(bar)', 1)",
      "the")

    testAllApis(
      "foothebar".regexpExtract("foo([\\w]+)", 1),
      "'foothebar'.regexpExtract('foo([\\w]+)', 1)",
      "REGEXP_EXTRACT('foothebar', 'foo([\\w]+)', 1)",
      "thebar")

    testAllApis(
      "foothebar".regexpExtract("foo([\\d]+)", 1),
      "'foothebar'.regexpExtract('foo([\\d]+)', 1)",
      "REGEXP_EXTRACT('foothebar', 'foo([\\d]+)', 1)",
      "null")

    testAllApis(
      'f33.regexpExtract("foo(.*?)(bar)", 2),
      "f33.regexpExtract('foo(.*?)(bar)', 2)",
      "REGEXP_EXTRACT(f33, 'foo(.*?)(bar)', 2)",
      "null")

    testAllApis(
      "foothebar".regexpExtract('f33, 2),
      "'foothebar'.regexpExtract(f33, 2)",
      "REGEXP_EXTRACT('foothebar', f33, 2)",
      "null")

    //test for optional group index
    testAllApis(
      "foothebar".regexpExtract("foo(.*?)(bar)"),
      "'foothebar'.regexpExtract('foo(.*?)(bar)')",
      "REGEXP_EXTRACT('foothebar', 'foo(.*?)(bar)')",
      "foothebar")
  }

  @Test
  def testFromBase64(): Unit = {
    testAllApis(
      'f35.fromBase64(),
      "f35.fromBase64()",
      "from_base64(f35)",
      "hello world")

    testAllApis(
      'f35.fromBase64(),
      "f35.fromBase64()",
      "FROM_BASE64(f35)",
      "hello world")

    //null test
    testAllApis(
      'f33.fromBase64(),
      "f33.fromBase64()",
      "FROM_BASE64(f33)",
      "null")

    testAllApis(
      "5L2g5aW9".fromBase64(),
      "'5L2g5aW9'.fromBase64()",
      "FROM_BASE64('5L2g5aW9')",
      "你好"
    )
  }

  @Test
  def testToBase64(): Unit = {
    testAllApis(
      'f0.toBase64(),
      "f0.toBase64()",
      "TO_BASE64(f0)",
      "VGhpcyBpcyBhIHRlc3QgU3RyaW5nLg==")

    testAllApis(
      'f8.toBase64(),
      "f8.toBase64()",
      "TO_BASE64(f8)",
      "IFRoaXMgaXMgYSB0ZXN0IFN0cmluZy4g")

    testAllApis(
      "".toBase64(),
      "''.toBase64()",
      "TO_BASE64('')",
      "")

    //null test
    testAllApis(
      'f33.toBase64(),
      "f33.toBase64()",
      "TO_BASE64(f33)",
      "null")

    testAllApis(
      "你好".toBase64(),
      "'你好'.toBase64()",
      "TO_BASE64('你好')",
      "5L2g5aW9"
    )
  }

  @Test
  def testUUID(): Unit = {
    testAllApis(
      uuid().charLength(),
      "uuid().charLength",
      "CHARACTER_LENGTH(UUID())",
      "36")

    testAllApis(
      uuid().substring(9, 1),
      "uuid().substring(9, 1)",
      "SUBSTRING(UUID(), 9, 1)",
      "-")

    testAllApis(
      uuid().substring(14, 1),
      "uuid().substring(14, 1)",
      "SUBSTRING(UUID(), 14, 1)",
      "-")

    testAllApis(
      uuid().substring(19, 1),
      "uuid().substring(19, 1)",
      "SUBSTRING(UUID(), 19, 1)",
      "-")

    testAllApis(
      uuid().substring(24, 1),
      "uuid().substring(24, 1)",
      "SUBSTRING(UUID(), 24, 1)",
      "-")
  }

  @Test
  def testLTrim(): Unit = {
    testAllApis(
      'f8.ltrim(),
      "f8.ltrim",
      "LTRIM(f8)",
      "This is a test String. ")

    testAllApis(
      'f0.ltrim(),
      "f0.ltrim",
      "LTRIM(f0)",
      "This is a test String.")

    testAllApis(
      "".ltrim(),
      "''.ltrim()",
      "LTRIM('')",
      "")

    testAllApis(
      'f33.ltrim(),
      "f33.ltrim",
      "LTRIM(f33)",
      "null")
  }

  @Test
  def testRTrim(): Unit = {
    testAllApis(
      'f8.rtrim(),
      "f8.rtrim",
      "RTRIM(f8)",
      " This is a test String.")

    testAllApis(
      'f0.rtrim(),
      "f0.rtrim",
      "RTRIM(f0)",
      "This is a test String.")

    testAllApis(
      "".rtrim(),
      "''.rtrim()",
      "RTRIM('')",
      "")

    testAllApis(
      'f33.rtrim(),
      "f33.rtrim",
      "RTRIM(f33)",
      "null")
  }

  @Test
  def testRepeat(): Unit = {
    testAllApis(
      'f0.repeat(1),
      "f0.repeat(1)",
      "REPEAT(f0, 1)",
      "This is a test String.")

    testAllApis(
      'f0.repeat(2),
      "f0.repeat(2)",
      "REPEAT(f0, 2)",
      "This is a test String.This is a test String.")

    testAllApis(
      'f0.repeat(0),
      "f0.repeat(0)",
      "REPEAT(f0, 0)",
      "")

    testAllApis(
      'f0.repeat(-1),
      "f0.repeat(-1)",
      "REPEAT(f0, -1)",
      "")

    testAllApis(
      'f33.repeat(2),
      "f33.repeat(2)",
      "REPEAT(f33, 2)",
      "null")

    testAllApis(
      "".repeat(1),
      "''.repeat(1)",
      "REPEAT('', 2)",
      "")
  }

  // ----------------------------------------------------------------------------------------------
  // Math functions
  // ----------------------------------------------------------------------------------------------

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
      "exp(f7)",
      "EXP(f7)",
      math.exp(3).toString)

    testAllApis(
      3.exp(),
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
  def testLog2(): Unit = {
    testAllApis(
      'f6.log2(),
      "f6.log2",
      "LOG2(f6)",
     "2.2016338611696504")

    testAllApis(
      ('f6 - 'f6 + 100).log2(),
      "(f6 - f6 + 100).log2()",
      "LOG2(f6 - f6 + 100)",
      "6.643856189774725")

    testAllApis(
      ('f6 + 20).log2(),
      "(f6+20).log2",
      "LOG2(f6+20)",
      "4.620586410451877")

    testAllApis(
      10.log2(),
      "10.log2",
      "LOG2(10)",
      "3.3219280948873626")
  }

  @Test
  def testPower(): Unit = {
    // f7: int , f4: long, f6: double
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

    // f5: float
    testAllApis('f5.power('f5),
      "f5.power(f5)",
      "power(f5, f5)",
      math.pow(4.5F, 4.5F).toString)

    testAllApis('f5.power('f6),
      "f5.power(f6)",
      "power(f5, f6)",
      math.pow(4.5F, 4.6D).toString)

    testAllApis('f5.power('f7),
      "f5.power(f7)",
      "power(f5, f7)",
      math.pow(4.5F, 3).toString)

    testAllApis('f5.power('f4),
      "f5.power(f4)",
      "power(f5, f4)",
      math.pow(4.5F, 44L).toString)

    // f22: bigDecimal
    // TODO delete casting in SQL when CALCITE-1467 is fixed
    testAllApis(
      'f22.cast(Types.DOUBLE).power('f5),
      "f22.cast(DOUBLE).power(f5)",
      "power(CAST(f22 AS DOUBLE), f5)",
      math.pow(2, 4.5F).toString)

    testAllApis(
      'f22.cast(Types.DOUBLE).power('f6),
      "f22.cast(DOUBLE).power(f6)",
      "power(CAST(f22 AS DOUBLE), f6)",
      math.pow(2, 4.6D).toString)

    testAllApis(
      'f22.cast(Types.DOUBLE).power('f7),
      "f22.cast(DOUBLE).power(f7)",
      "power(CAST(f22 AS DOUBLE), f7)",
      math.pow(2, 3).toString)

    testAllApis(
      'f22.cast(Types.DOUBLE).power('f4),
      "f22.cast(DOUBLE).power(f4)",
      "power(CAST(f22 AS DOUBLE), f4)",
      math.pow(2, 44L).toString)

    testAllApis(
      'f6.power('f22.cast(Types.DOUBLE)),
      "f6.power(f22.cast(DOUBLE))",
      "power(f6, f22)",
      math.pow(4.6D, 2).toString)
  }

  @Test
  def testSqrt(): Unit = {
    testAllApis(
      'f6.sqrt(),
      "f6.sqrt",
      "SQRT(f6)",
      math.sqrt(4.6D).toString)

    testAllApis(
      'f7.sqrt(),
      "f7.sqrt",
      "SQRT(f7)",
      math.sqrt(3).toString)

    testAllApis(
      'f4.sqrt(),
      "f4.sqrt",
      "SQRT(f4)",
      math.sqrt(44L).toString)

    testAllApis(
      'f22.cast(Types.DOUBLE).sqrt(),
      "f22.cast(DOUBLE).sqrt",
      "SQRT(CAST(f22 AS DOUBLE))",
      math.sqrt(2.0).toString)

    testAllApis(
      'f5.sqrt(),
      "f5.sqrt",
      "SQRT(f5)",
      math.pow(4.5F, 0.5).toString)

    testAllApis(
      25.sqrt(),
      "25.sqrt()",
      "SQRT(25)",
      "5.0")

    testAllApis(
      2.2.sqrt(),
      "2.2.sqrt()",
      "POWER(CAST(2.2 AS DOUBLE), CAST(0.5 AS DOUBLE))", // TODO fix FLINK-4621
      math.sqrt(2.2).toString)
  }

  @Test
  def testCosh(): Unit = {
    testAllApis(
      0.cosh(),
      "0.cosh()",
      "COSH(0)",
      math.cosh(0).toString
    )

    testAllApis(
      -1.cosh(),
      "-1.cosh()",
      "COSH(-1)",
      math.cosh(-1).toString
    )

    testAllApis(
      'f4.cosh(),
      "f4.cosh",
      "COSH(f4)",
      math.cosh(44L).toString)

    testAllApis(
      'f6.cosh(),
      "f6.cosh",
      "COSH(f6)",
      math.cosh(4.6D).toString)

    testAllApis(
      'f7.cosh(),
      "f7.cosh",
      "COSH(f7)",
      math.cosh(3).toString)

    testAllApis(
      'f22.cosh(),
      "f22.cosh",
      "COSH(f22)",
      math.cosh(2.0).toString)
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
  def testSin(): Unit = {
    testAllApis(
      'f2.sin(),
      "f2.sin()",
      "SIN(f2)",
      math.sin(42.toByte).toString)

    testAllApis(
      'f3.sin(),
      "f3.sin()",
      "SIN(f3)",
      math.sin(43.toShort).toString)

    testAllApis(
      'f4.sin(),
      "f4.sin()",
      "SIN(f4)",
      math.sin(44.toLong).toString)

    testAllApis(
      'f5.sin(),
      "f5.sin()",
      "SIN(f5)",
      math.sin(4.5.toFloat).toString)

    testAllApis(
      'f6.sin(),
      "f6.sin()",
      "SIN(f6)",
      math.sin(4.6).toString)

    testAllApis(
      'f15.sin(),
      "sin(f15)",
      "SIN(f15)",
      math.sin(-1231.1231231321321321111).toString)
  }

  @Test
  def testCos(): Unit = {
    testAllApis(
      'f2.cos(),
      "f2.cos()",
      "COS(f2)",
      math.cos(42.toByte).toString)

    testAllApis(
      'f3.cos(),
      "f3.cos()",
      "COS(f3)",
      math.cos(43.toShort).toString)

    testAllApis(
      'f4.cos(),
      "f4.cos()",
      "COS(f4)",
      math.cos(44.toLong).toString)

    testAllApis(
      'f5.cos(),
      "f5.cos()",
      "COS(f5)",
      math.cos(4.5.toFloat).toString)

    testAllApis(
      'f6.cos(),
      "f6.cos()",
      "COS(f6)",
      math.cos(4.6).toString)

    testAllApis(
      'f15.cos(),
      "cos(f15)",
      "COS(f15)",
      math.cos(-1231.1231231321321321111).toString)
  }

  @Test
  def testSinh(): Unit = {
    testAllApis(
      0.sinh(),
      "0.sinh()",
      "SINH(0)",
      math.sinh(0).toString)

    testAllApis(
      -1.sinh(),
      "-1.sinh()",
      "SINH(-1)",
      math.sinh(-1).toString)

    testAllApis(
      'f4.sinh(),
      "f4.sinh",
      "SINH(f4)",
      math.sinh(44L).toString)

    testAllApis(
      'f6.sinh(),
      "f6.sinh",
      "SINH(f6)",
      math.sinh(4.6D).toString)

    testAllApis(
      'f7.sinh(),
      "f7.sinh",
      "SINH(f7)",
      math.sinh(3).toString)

    testAllApis(
      'f22.sinh(),
      "f22.sinh",
      "SINH(f22)",
      math.sinh(2.0).toString)
  }

  @Test
  def testTan(): Unit = {
    testAllApis(
      'f2.tan(),
      "f2.tan()",
      "TAN(f2)",
      math.tan(42.toByte).toString)

    testAllApis(
      'f3.tan(),
      "f3.tan()",
      "TAN(f3)",
      math.tan(43.toShort).toString)

    testAllApis(
      'f4.tan(),
      "f4.tan()",
      "TAN(f4)",
      math.tan(44.toLong).toString)

    testAllApis(
      'f5.tan(),
      "f5.tan()",
      "TAN(f5)",
      math.tan(4.5.toFloat).toString)

    testAllApis(
      'f6.tan(),
      "f6.tan()",
      "TAN(f6)",
      math.tan(4.6).toString)

    testAllApis(
      'f15.tan(),
      "tan(f15)",
      "TAN(f15)",
      math.tan(-1231.1231231321321321111).toString)
  }

  @Test
  def testTanh(): Unit = {
    testAllApis(
      0.tanh(),
      "0.tanh()",
      "TANH(0)",
      math.tanh(0).toString)

    testAllApis(
      -1.tanh(),
      "-1.tanh()",
      "TANH(-1)",
      math.tanh(-1).toString)

    testAllApis(
      'f4.tanh(),
      "f4.tanh",
      "TANH(f4)",
      math.tanh(44L).toString)

    testAllApis(
      'f6.tanh(),
      "f6.tanh",
      "TANH(f6)",
      math.tanh(4.6D).toString)

    testAllApis(
      'f7.tanh(),
      "f7.tanh",
      "TANH(f7)",
      math.tanh(3).toString)

    testAllApis(
      'f22.tanh(),
      "f22.tanh",
      "TANH(f22)",
      math.tanh(2.0).toString)
  }

  @Test
  def testCot(): Unit = {
    testAllApis(
      'f2.cot(),
      "f2.cot()",
      "COT(f2)",
      (1.0d / math.tan(42.toByte)).toString)

    testAllApis(
      'f3.cot(),
      "f3.cot()",
      "COT(f3)",
      (1.0d / math.tan(43.toShort)).toString)

    testAllApis(
      'f4.cot(),
      "f4.cot()",
      "COT(f4)",
      (1.0d / math.tan(44.toLong)).toString)

    testAllApis(
      'f5.cot(),
      "f5.cot()",
      "COT(f5)",
      (1.0d / math.tan(4.5.toFloat)).toString)

    testAllApis(
      'f6.cot(),
      "f6.cot()",
      "COT(f6)",
      (1.0d / math.tan(4.6)).toString)

    testAllApis(
      'f15.cot(),
      "cot(f15)",
      "COT(f15)",
      (1.0d / math.tan(-1231.1231231321321321111)).toString)
  }

  @Test
  def testAsin(): Unit = {
    testAllApis(
      'f25.asin(),
      "f25.asin()",
      "ASIN(f25)",
      math.asin(0.42.toByte).toString)

    testAllApis(
      'f26.asin(),
      "f26.asin()",
      "ASIN(f26)",
      math.asin(0.toShort).toString)

    testAllApis(
      'f27.asin(),
      "f27.asin()",
      "ASIN(f27)",
      math.asin(0.toLong).toString)

    testAllApis(
      'f28.asin(),
      "f28.asin()",
      "ASIN(f28)",
      math.asin(0.45.toFloat).toString)

    testAllApis(
      'f29.asin(),
      "f29.asin()",
      "ASIN(f29)",
      math.asin(0.46).toString)

    testAllApis(
      'f30.asin(),
      "f30.asin()",
      "ASIN(f30)",
      math.asin(1).toString)

    testAllApis(
      'f31.asin(),
      "f31.asin()",
      "ASIN(f31)",
      math.asin(-0.1231231321321321111).toString)
  }

  @Test
  def testAcos(): Unit = {
    testAllApis(
      'f25.acos(),
      "f25.acos()",
      "ACOS(f25)",
      math.acos(0.42.toByte).toString)

    testAllApis(
      'f26.acos(),
      "f26.acos()",
      "ACOS(f26)",
      math.acos(0.toShort).toString)

    testAllApis(
      'f27.acos(),
      "f27.acos()",
      "ACOS(f27)",
      math.acos(0.toLong).toString)

    testAllApis(
      'f28.acos(),
      "f28.acos()",
      "ACOS(f28)",
      math.acos(0.45.toFloat).toString)

    testAllApis(
      'f29.acos(),
      "f29.acos()",
      "ACOS(f29)",
      math.acos(0.46).toString)

    testAllApis(
      'f30.acos(),
      "f30.acos()",
      "ACOS(f30)",
      math.acos(1).toString)

    testAllApis(
      'f31.acos(),
      "f31.acos()",
      "ACOS(f31)",
      math.acos(-0.1231231321321321111).toString)
  }

  @Test
  def testAtan(): Unit = {
    testAllApis(
      'f25.atan(),
      "f25.atan()",
      "ATAN(f25)",
      math.atan(0.42.toByte).toString)

    testAllApis(
      'f26.atan(),
      "f26.atan()",
      "ATAN(f26)",
      math.atan(0.toShort).toString)

    testAllApis(
      'f27.atan(),
      "f27.atan()",
      "ATAN(f27)",
      math.atan(0.toLong).toString)

    testAllApis(
      'f28.atan(),
      "f28.atan()",
      "ATAN(f28)",
      math.atan(0.45.toFloat).toString)

    testAllApis(
      'f29.atan(),
      "f29.atan()",
      "ATAN(f29)",
      math.atan(0.46).toString)

    testAllApis(
      'f30.atan(),
      "f30.atan()",
      "ATAN(f30)",
      math.atan(1).toString)

    testAllApis(
      'f31.atan(),
      "f31.atan()",
      "ATAN(f31)",
      math.atan(-0.1231231321321321111).toString)
  }

  @Test
  def testAtan2(): Unit = {
    testAllApis(
      atan2('f25, 'f26),
      "atan2(f25, f26)",
      "ATAN2(f25, f26)",
      math.atan2(0.42.toByte, 0.toByte).toString)

    testAllApis(
      atan2('f26, 'f25),
      "atan2(f26, f25)",
      "ATAN2(f26, f25)",
      math.atan2(0.toShort, 0.toShort).toString)

    testAllApis(
      atan2('f27, 'f27),
      "atan2(f27, f27)",
      "ATAN2(f27, f27)",
      math.atan2(0.toLong, 0.toLong).toString)

    testAllApis(
      atan2('f28, 'f28),
      "atan2(f28, f28)",
      "ATAN2(f28, f28)",
      math.atan2(0.45.toFloat, 0.45.toFloat).toString)

    testAllApis(
      atan2('f29, 'f29),
      "atan2(f29, f29)",
      "ATAN2(f29, f29)",
      math.atan2(0.46, 0.46).toString)

    testAllApis(
      atan2('f30, 'f30),
      "atan2(f30, f30)",
      "ATAN2(f30, f30)",
      math.atan2(1, 1).toString)

    testAllApis(
      atan2('f31, 'f31),
      "atan2(f31, f31)",
      "ATAN2(f31, f31)",
      math.atan2(-0.1231231321321321111, -0.1231231321321321111).toString)
  }

  @Test
  def testDegrees(): Unit = {
    testAllApis(
      'f2.degrees(),
      "f2.degrees()",
      "DEGREES(f2)",
      math.toDegrees(42.toByte).toString)

    testAllApis(
      'f3.degrees(),
      "f3.degrees()",
      "DEGREES(f3)",
      math.toDegrees(43.toShort).toString)

    testAllApis(
      'f4.degrees(),
      "f4.degrees()",
      "DEGREES(f4)",
      math.toDegrees(44.toLong).toString)

    testAllApis(
      'f5.degrees(),
      "f5.degrees()",
      "DEGREES(f5)",
      math.toDegrees(4.5.toFloat).toString)

    testAllApis(
      'f6.degrees(),
      "f6.degrees()",
      "DEGREES(f6)",
      math.toDegrees(4.6).toString)

    testAllApis(
      'f15.degrees(),
      "degrees(f15)",
      "DEGREES(f15)",
      math.toDegrees(-1231.1231231321321321111).toString)
  }

  @Test
  def testRadians(): Unit = {
    testAllApis(
      'f2.radians(),
      "f2.radians()",
      "RADIANS(f2)",
      math.toRadians(42.toByte).toString)

    testAllApis(
      'f3.radians(),
      "f3.radians()",
      "RADIANS(f3)",
      math.toRadians(43.toShort).toString)

    testAllApis(
      'f4.radians(),
      "f4.radians()",
      "RADIANS(f4)",
      math.toRadians(44.toLong).toString)

    testAllApis(
      'f5.radians(),
      "f5.radians()",
      "RADIANS(f5)",
      math.toRadians(4.5.toFloat).toString)

    testAllApis(
      'f6.radians(),
      "f6.radians()",
      "RADIANS(f6)",
      math.toRadians(4.6).toString)

    testAllApis(
      'f15.radians(),
      "radians(f15)",
      "RADIANS(f15)",
      math.toRadians(-1231.1231231321321321111).toString)
  }

  @Test
  def testSign(): Unit = {
    testAllApis(
      'f4.sign(),
      "f4.sign()",
      "SIGN(f4)",
      1.toString)

    testAllApis(
      'f6.sign(),
      "f6.sign()",
      "SIGN(f6)",
      1.0.toString)

    testAllApis(
      'f15.sign(),
      "sign(f15)",
      "SIGN(f15)",
      (-1).toString)
  }

  @Test
  def testRound(): Unit = {
    testAllApis(
      'f29.round('f30),
      "f29.round(f30)",
      "ROUND(f29, f30)",
      0.5.toString)

    testAllApis(
      'f31.round('f7),
      "f31.round(f7)",
      "ROUND(f31, f7)",
      (-0.123).toString)

    testAllApis(
      'f4.round('f32),
      "f4.round(f32)",
      "ROUND(f4, f32)",
      40.toString)
  }

  @Test
  def testPi(): Unit = {
    testAllApis(
      pi(),
      "pi()",
      "PI",
      math.Pi.toString)
  }

  @Test
  def testRandAndRandInteger(): Unit = {
    val random1 = new java.util.Random(1)
    testAllApis(
      rand(1),
      "rand(1)",
      "RAND(1)",
      random1.nextDouble().toString)

    val random2 = new java.util.Random(3)
    testAllApis(
      rand('f7),
      "rand(f7)",
      "RAND(f7)",
      random2.nextDouble().toString)

    val random3 = new java.util.Random(1)
    testAllApis(
      randInteger(1, 10),
      "randInteger(1, 10)",
      "RAND_INTEGER(1, 10)",
      random3.nextInt(10).toString)

    val random4 = new java.util.Random(3)
    testAllApis(
      randInteger('f7, 'f4.cast(Types.INT)),
      "randInteger(f7, f4.cast(INT))",
      "RAND_INTEGER(f7, CAST(f4 AS INT))",
      random4.nextInt(44).toString)
  }

  @Test
  def testE(): Unit = {
    testAllApis(
      e(),
      "E()",
      "E()",
      math.E.toString)

    testAllApis(
      e(),
      "e()",
      "e()",
      math.E.toString)
  }

  @Test
  def testLog(): Unit = {
    testAllApis(
      'f6.log(),
      "f6.log",
      "LOG(f6)",
      "1.5260563034950492"
    )

    testTableApi(
      log('f6),
      "log(f6)",
      "1.5260563034950492"
    )

    testAllApis(
      ('f6 - 'f6 + 100).log('f6 - 'f6 + 10),
      "(f6 - f6 + 100).log(f6 - f6 + 10)",
      "LOG(f6 - f6 + 10, f6 - f6 + 100)",
      "2.0"
    )

    testAllApis(
      ('f6 + 20).log(),
      "(f6+20).log",
      "LOG(f6+20)",
      "3.202746442938317"
    )

    testAllApis(
      10.log(),
      "10.log",
      "LOG(10)",
      "2.302585092994046"
    )

    testAllApis(
      100.log(10),
      "100.log(10)",
      "LOG(10, 100)",
      "2.0"
    )

    testTableApi(
      log(10, 100),
      "log(10, 100)",
      "2.0"
    )
  }

  @Test
  def testTruncate(): Unit = {
    testAllApis(
      'f29.truncate('f30),
      "f29.truncate(f30)",
      "truncate(f29, f30)",
      "0.4")

    testAllApis(
      'f31.truncate('f7),
      "f31.truncate(f7)",
      "truncate(f31, f7)",
      "-0.123")

    testAllApis(
      'f4.truncate('f32),
      "f4.truncate(f32)",
      "truncate(f4, f32)",
      "40")

    testAllApis(
      'f28.cast(Types.DOUBLE).truncate(1),
      "f28.cast(DOUBLE).truncate(1)",
      "truncate(cast(f28 as DOUBLE), 1)",
      "0.4")

    testAllApis(
      'f31.cast(Types.DECIMAL).truncate(2),
      "f31.cast(DECIMAL).truncate(2)",
      "truncate(cast(f31 as decimal), 2)",
      "-0.12")

    testAllApis(
      'f36.cast(Types.DECIMAL).truncate(),
      "f36.cast(DECIMAL).truncate()",
      "truncate(42.324)",
      "42")

    testAllApis(
      'f5.cast(Types.FLOAT).truncate(),
      "f5.cast(FLOAT).truncate()",
      "truncate(cast(f5 as float))",
      "4.0")

    testAllApis(
      42.truncate(-1),
      "42.truncate(-1)",
      "truncate(42, -1)",
      "40")

    testAllApis(
      42.truncate(-3),
      "42.truncate(-3)",
      "truncate(42, -3)",
      "0")

    //    The validation parameter is null
    testAllApis(
      'f33.cast(Types.INT).truncate(1),
      "f33.cast(INT).truncate(1)",
      "truncate(cast(null as integer), 1)",
      "null")

    testAllApis(
      43.21.truncate('f33.cast(Types.INT)),
      "43.21.truncate(f33.cast(INT))",
      "truncate(43.21, cast(null as integer))",
      "null")

    testAllApis(
      'f33.cast(Types.DOUBLE).truncate(1),
      "f33.cast(DOUBLE).truncate(1)",
      "truncate(cast(null as double), 1)",
      "null")

    testAllApis(
      'f33.cast(Types.INT).truncate(1),
      "f33.cast(INT).truncate(1)",
      "truncate(cast(null as integer))",
      "null")

    testAllApis(
      'f33.cast(Types.DOUBLE).truncate(),
      "f33.cast(DOUBLE).truncate()",
      "truncate(cast(null as double))",
      "null")
  }

  // ----------------------------------------------------------------------------------------------
  // Temporal functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testExtract(): Unit = {
    testAllApis(
      'f16.extract(TimeIntervalUnit.YEAR),
      "f16.extract(YEAR)",
      "EXTRACT(YEAR FROM f16)",
      "1996")

    testAllApis(
      'f16.extract(TimeIntervalUnit.QUARTER),
      "f16.extract(QUARTER)",
      "EXTRACT(QUARTER FROM f16)",
      "4")

    testAllApis(
      'f16.extract(TimeIntervalUnit.MONTH),
      "extract(f16, MONTH)",
      "EXTRACT(MONTH FROM f16)",
      "11")

    testAllApis(
      'f16.extract(TimeIntervalUnit.WEEK),
      "extract(f16, WEEK)",
      "EXTRACT(WEEK FROM f16)",
      "45")

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
      'f18.extract(TimeIntervalUnit.QUARTER),
      "f18.extract(QUARTER)",
      "EXTRACT(QUARTER FROM f18)",
      "4")

    testAllApis(
      'f16.extract(TimeIntervalUnit.QUARTER),
      "f16.extract(QUARTER)",
      "EXTRACT(QUARTER FROM f16)",
      "4")

    testAllApis(
      'f18.extract(TimeIntervalUnit.MONTH),
      "f18.extract(MONTH)",
      "EXTRACT(MONTH FROM f18)",
      "11")

    testAllApis(
      'f18.extract(TimeIntervalUnit.WEEK),
      "f18.extract(WEEK)",
      "EXTRACT(WEEK FROM f18)",
      "45")

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
      'f20.extract(TimeIntervalUnit.QUARTER),
      "f20.extract(QUARTER)",
      "EXTRACT(QUARTER FROM f20)",
      "1")

    testAllApis(
      'f20.extract(TimeIntervalUnit.YEAR),
      "f20.extract(YEAR)",
      "EXTRACT(YEAR FROM f20)",
      "2")

    // test SQL only time units
    testSqlApi(
      "EXTRACT(MILLENNIUM FROM f18)",
      "2")

    testSqlApi(
      "EXTRACT(MILLENNIUM FROM f16)",
      "2")

    testSqlApi(
      "EXTRACT(CENTURY FROM f18)",
      "20")

    testSqlApi(
      "EXTRACT(CENTURY FROM f16)",
      "20")

    testSqlApi(
      "EXTRACT(DOY FROM f18)",
      "315")

    testSqlApi(
      "EXTRACT(DOY FROM f16)",
      "315")

    testSqlApi(
      "EXTRACT(DOW FROM f18)",
      "1")

    testSqlApi(
      "EXTRACT(DOW FROM f16)",
      "1")

    testSqlApi(
      "EXTRACT(WEEK FROM f18)",
      "45")

    testSqlApi(
      "EXTRACT(WEEK FROM f16)",
      "45")

    testSqlApi(
      "YEAR(f18)",
      "1996")

    testSqlApi(
      "YEAR(f16)",
      "1996")

    testSqlApi(
      "QUARTER(f18)",
      "4")

    testSqlApi(
      "QUARTER(f16)",
      "4")

    testSqlApi(
      "MONTH(f18)",
      "11")

    testSqlApi(
      "MONTH(f16)",
      "11")

    testSqlApi(
      "WEEK(f18)",
      "45")

    testSqlApi(
      "WEEK(f16)",
      "45")

    testSqlApi(
      "DAYOFYEAR(f18)",
      "315")

    testSqlApi(
      "DAYOFYEAR(f16)",
      "315")

    testSqlApi(
      "DAYOFMONTH(f18)",
      "10")

    testSqlApi(
      "DAYOFMONTH(f16)",
      "10")

    testSqlApi(
      "DAYOFWEEK(f18)",
      "1")

    testSqlApi(
      "DAYOFWEEK(f16)",
      "1")

    testSqlApi(
      "HOUR(f17)",
      "6")

    testSqlApi(
      "HOUR(f19)",
      "7")

    testSqlApi(
      "MINUTE(f17)",
      "55")

    testSqlApi(
      "MINUTE(f19)",
      "23")

    testSqlApi(
      "SECOND(f17)",
      "44")

    testSqlApi(
      "SECOND(f19)",
      "33")
  }

  @Test
  def testTemporalFloor(): Unit = {
    testAllApis(
      'f18.floor(TimeIntervalUnit.YEAR),
      "f18.floor(YEAR)",
      "FLOOR(f18 TO YEAR)",
      "1996-01-01 00:00:00.0")

    testAllApis(
      'f18.floor(TimeIntervalUnit.MONTH),
      "f18.floor(MONTH)",
      "FLOOR(f18 TO MONTH)",
      "1996-11-01 00:00:00.0")

    testAllApis(
      'f18.floor(TimeIntervalUnit.DAY),
      "f18.floor(DAY)",
      "FLOOR(f18 TO DAY)",
      "1996-11-10 00:00:00.0")

    testAllApis(
      'f18.floor(TimeIntervalUnit.MINUTE),
      "f18.floor(MINUTE)",
      "FLOOR(f18 TO MINUTE)",
      "1996-11-10 06:55:00.0")

    testAllApis(
      'f18.floor(TimeIntervalUnit.SECOND),
      "f18.floor(SECOND)",
      "FLOOR(f18 TO SECOND)",
      "1996-11-10 06:55:44.0")

    testAllApis(
      'f17.floor(TimeIntervalUnit.HOUR),
      "f17.floor(HOUR)",
      "FLOOR(f17 TO HOUR)",
      "06:00:00")

    testAllApis(
      'f17.floor(TimeIntervalUnit.MINUTE),
      "f17.floor(MINUTE)",
      "FLOOR(f17 TO MINUTE)",
      "06:55:00")

    testAllApis(
      'f17.floor(TimeIntervalUnit.SECOND),
      "f17.floor(SECOND)",
      "FLOOR(f17 TO SECOND)",
      "06:55:44")

    testAllApis(
      'f16.floor(TimeIntervalUnit.YEAR),
      "f16.floor(YEAR)",
      "FLOOR(f16 TO YEAR)",
      "1996-01-01")

    testAllApis(
      'f16.floor(TimeIntervalUnit.MONTH),
      "f16.floor(MONTH)",
      "FLOOR(f16 TO MONTH)",
      "1996-11-01")

    testAllApis(
      'f18.ceil(TimeIntervalUnit.YEAR),
      "f18.ceil(YEAR)",
      "CEIL(f18 TO YEAR)",
      "1997-01-01 00:00:00.0")

    testAllApis(
      'f18.ceil(TimeIntervalUnit.MONTH),
      "f18.ceil(MONTH)",
      "CEIL(f18 TO MONTH)",
      "1996-12-01 00:00:00.0")

    testAllApis(
      'f18.ceil(TimeIntervalUnit.DAY),
      "f18.ceil(DAY)",
      "CEIL(f18 TO DAY)",
      "1996-11-11 00:00:00.0")

    testAllApis(
      'f18.ceil(TimeIntervalUnit.MINUTE),
      "f18.ceil(MINUTE)",
      "CEIL(f18 TO MINUTE)",
      "1996-11-10 06:56:00.0")

    testAllApis(
      'f18.ceil(TimeIntervalUnit.SECOND),
      "f18.ceil(SECOND)",
      "CEIL(f18 TO SECOND)",
      "1996-11-10 06:55:45.0")

    testAllApis(
      'f17.ceil(TimeIntervalUnit.HOUR),
      "f17.ceil(HOUR)",
      "CEIL(f17 TO HOUR)",
      "07:00:00")

    testAllApis(
      'f17.ceil(TimeIntervalUnit.MINUTE),
      "f17.ceil(MINUTE)",
      "CEIL(f17 TO MINUTE)",
      "06:56:00")

    testAllApis(
      'f17.ceil(TimeIntervalUnit.SECOND),
      "f17.ceil(SECOND)",
      "CEIL(f17 TO SECOND)",
      "06:55:44")

    testAllApis(
      'f16.ceil(TimeIntervalUnit.YEAR),
      "f16.ceil(YEAR)",
      "CEIL(f16 TO YEAR)",
      "1997-01-01")

    testAllApis(
      'f16.ceil(TimeIntervalUnit.MONTH),
      "f16.ceil(MONTH)",
      "CEIL(f16 TO MONTH)",
      "1996-12-01")
  }

  @Test
  def testCurrentTimePoint(): Unit = {

    // current time points are non-deterministic
    // we just test the format of the output
    // manual test can be found in NonDeterministicTests

    testAllApis(
      currentDate().cast(Types.STRING).charLength() >= 5,
      "currentDate().cast(STRING).charLength() >= 5",
      "CHAR_LENGTH(CAST(CURRENT_DATE AS VARCHAR)) >= 5",
      "true")

    testAllApis(
      currentTime().cast(Types.STRING).charLength() >= 5,
      "currentTime().cast(STRING).charLength() >= 5",
      "CHAR_LENGTH(CAST(CURRENT_TIME AS VARCHAR)) >= 5",
      "true")

    testAllApis(
      currentTimestamp().cast(Types.STRING).charLength() >= 12,
      "currentTimestamp().cast(STRING).charLength() >= 12",
      "CHAR_LENGTH(CAST(CURRENT_TIMESTAMP AS VARCHAR)) >= 12",
      "true")

    testAllApis(
      localTimestamp().cast(Types.STRING).charLength() >= 12,
      "localTimestamp().cast(STRING).charLength() >= 12",
      "CHAR_LENGTH(CAST(LOCALTIMESTAMP AS VARCHAR)) >= 12",
      "true")

    testAllApis(
      localTime().cast(Types.STRING).charLength() >= 5,
      "localTime().cast(STRING).charLength() >= 5",
      "CHAR_LENGTH(CAST(LOCALTIME AS VARCHAR)) >= 5",
      "true")

    // comparisons are deterministic
    testAllApis(
      localTimestamp() === localTimestamp(),
      "localTimestamp() === localTimestamp()",
      "LOCALTIMESTAMP = LOCALTIMESTAMP",
      "true")
  }

  @Test
  def testOverlaps(): Unit = {
    testAllApis(
      temporalOverlaps("2:55:00".toTime, 1.hour, "3:30:00".toTime, 2.hours),
      "temporalOverlaps('2:55:00'.toTime, 1.hour, '3:30:00'.toTime, 2.hours)",
      "(TIME '2:55:00', INTERVAL '1' HOUR) OVERLAPS (TIME '3:30:00', INTERVAL '2' HOUR)",
      "true")

    testAllApis(
      temporalOverlaps("9:00:00".toTime, "9:30:00".toTime, "9:29:00".toTime, "9:31:00".toTime),
      "temporalOverlaps(toTime('9:00:00'), '9:30:00'.toTime, '9:29:00'.toTime, '9:31:00'.toTime)",
      "(TIME '9:00:00', TIME '9:30:00') OVERLAPS (TIME '9:29:00', TIME '9:31:00')",
      "true")

    testAllApis(
      temporalOverlaps("9:00:00".toTime, "10:00:00".toTime, "10:15:00".toTime, 3.hours),
      "temporalOverlaps('9:00:00'.toTime, '10:00:00'.toTime, '10:15:00'.toTime, 3.hours)",
      "(TIME '9:00:00', TIME '10:00:00') OVERLAPS (TIME '10:15:00', INTERVAL '3' HOUR)",
      "false")

    testAllApis(
      temporalOverlaps("2011-03-10".toDate, 10.days, "2011-03-19".toDate, 10.days),
      "temporalOverlaps(toDate('2011-03-10'), 10.days, '2011-03-19'.toDate, 10.days)",
      "(DATE '2011-03-10', INTERVAL '10' DAY) OVERLAPS (DATE '2011-03-19', INTERVAL '10' DAY)",
      "true")

    testAllApis(
      temporalOverlaps("2011-03-10 05:02:02".toTimestamp, 0.milli,
        "2011-03-10 05:02:02".toTimestamp, "2011-03-10 05:02:01".toTimestamp),
      "temporalOverlaps(toTimestamp('2011-03-10 05:02:02'), 0.milli, " +
        "'2011-03-10 05:02:02'.toTimestamp, '2011-03-10 05:02:01'.toTimestamp)",
      "(TIMESTAMP '2011-03-10 05:02:02', INTERVAL '0' SECOND) OVERLAPS " +
        "(TIMESTAMP '2011-03-10 05:02:02', TIMESTAMP '2011-03-10 05:02:01')",
      "true")

    testAllApis(
      temporalOverlaps("2011-03-10 02:02:02.001".toTimestamp, 0.milli,
        "2011-03-10 02:02:02.002".toTimestamp, "2011-03-10 02:02:02.002".toTimestamp),
      "temporalOverlaps('2011-03-10 02:02:02.001'.toTimestamp, 0.milli, " +
        "'2011-03-10 02:02:02.002'.toTimestamp, '2011-03-10 02:02:02.002'.toTimestamp)",
      "(TIMESTAMP '2011-03-10 02:02:02.001', INTERVAL '0' SECOND) OVERLAPS " +
        "(TIMESTAMP '2011-03-10 02:02:02.002', TIMESTAMP '2011-03-10 02:02:02.002')",
      "false")
  }

  @Test
  def testTimestampDiff(): Unit = {
    val dataMap = Map(
      ("DAY", TimePointUnit.DAY, "SQL_TSI_DAY") -> Seq(
        ("2018-07-03 11:11:11", "2018-07-05 11:11:11", "2"), // timestamp, timestamp
        ("2016-06-15", "2016-06-16 11:11:11", "1"), // date, timestamp
        ("2016-06-15 11:00:00", "2016-06-19", "3"), // timestamp, date
        ("2016-06-15", "2016-06-18", "3") // date, date
      ),
      ("HOUR", TimePointUnit.HOUR, "SQL_TSI_HOUR") -> Seq(
        ("2018-07-03 11:11:11", "2018-07-04 12:12:11", "25"),
        ("2016-06-15", "2016-06-16 11:11:11", "35"),
        ("2016-06-15 11:00:00", "2016-06-19", "85"),
        ("2016-06-15", "2016-06-12", "-72")
      ),
      ("MINUTE", TimePointUnit.MINUTE, "SQL_TSI_MINUTE") -> Seq(
        ("2018-07-03 11:11:11", "2018-07-03 12:10:11", "59"),
        ("2016-06-15", "2016-06-16 11:11:11", "2111"),
        ("2016-06-15 11:00:00", "2016-06-19", "5100"),
        ("2016-06-15", "2016-06-18", "4320")
      ),
      ("SECOND", TimePointUnit.SECOND, "SQL_TSI_SECOND") -> Seq(
        ("2018-07-03 11:11:11", "2018-07-03 11:12:12", "61"),
        ("2016-06-15", "2016-06-16 11:11:11", "126671"),
        ("2016-06-15 11:00:00", "2016-06-19", "306000"),
        ("2016-06-15", "2016-06-18", "259200")
      ),
      ("WEEK", TimePointUnit.WEEK, "SQL_TSI_WEEK") -> Seq(
        ("2018-05-03 11:11:11", "2018-07-03 11:12:12", "8"),
        ("2016-04-15", "2016-07-16 11:11:11", "13"),
        ("2016-04-15 11:00:00", "2016-09-19", "22"),
        ("2016-08-15", "2016-06-18", "-8")
      ),
      ("MONTH", TimePointUnit.MONTH, "SQL_TSI_MONTH") -> Seq(
        ("2018-07-03 11:11:11", "2018-09-05 11:11:11", "2"),
        ("2016-06-15", "2018-06-16 11:11:11", "24"),
        ("2016-06-15 11:00:00", "2018-05-19", "23"),
        ("2016-06-15", "2018-03-18", "21")
      ),
      ("QUARTER", TimePointUnit.QUARTER, "SQL_TSI_QUARTER") -> Seq(
        ("2018-01-03 11:11:11", "2018-09-05 11:11:11", "2"),
        ("2016-06-15", "2018-06-16 11:11:11", "8"),
        ("2016-06-15 11:00:00", "2018-05-19", "7"),
        ("2016-06-15", "2018-03-18", "7")
      )
    )

    for ((unitParts, dataParts) <- dataMap) {
      for ((data,index) <- dataParts.zipWithIndex) {
        index match {
          case 0 => // timestamp, timestamp
            testAllApis(
              timestampDiff(unitParts._2, data._1.toTimestamp, data._2.toTimestamp),
              s"timestampDiff(${unitParts._1}, '${data._1}'.toTimestamp, '${data._2}'.toTimestamp)",
              s"TIMESTAMPDIFF(${unitParts._1}, TIMESTAMP '${data._1}', TIMESTAMP '${data._2}')",
              data._3
            )
            testSqlApi(  // sql tsi
              s"TIMESTAMPDIFF(${unitParts._3}, TIMESTAMP '${data._1}', TIMESTAMP '${data._2}')",
              data._3
            )
          case 1 => // date, timestamp
            testAllApis(
              timestampDiff(unitParts._2, data._1.toDate, data._2.toTimestamp),
              s"timestampDiff(${unitParts._1}, '${data._1}'.toDate, '${data._2}'.toTimestamp)",
              s"TIMESTAMPDIFF(${unitParts._1}, DATE '${data._1}', TIMESTAMP '${data._2}')",
              data._3
            )
            testSqlApi( // sql tsi
              s"TIMESTAMPDIFF(${unitParts._3}, DATE '${data._1}', TIMESTAMP '${data._2}')",
              data._3
            )
          case 2 => // timestamp, date
            testAllApis(
              timestampDiff(unitParts._2, data._1.toTimestamp, data._2.toDate),
              s"timestampDiff(${unitParts._1}, '${data._1}'.toTimestamp, '${data._2}'.toDate)",
              s"TIMESTAMPDIFF(${unitParts._1}, TIMESTAMP '${data._1}', DATE '${data._2}')",
              data._3
            )
            testSqlApi( // sql tsi
              s"TIMESTAMPDIFF(${unitParts._3}, TIMESTAMP '${data._1}', DATE '${data._2}')",
              data._3
            )
          case 3 => // date, date
            testAllApis(
              timestampDiff(unitParts._2, data._1.toDate, data._2.toDate),
              s"timestampDiff(${unitParts._1}, '${data._1}'.toDate, '${data._2}'.toDate)",
              s"TIMESTAMPDIFF(${unitParts._1}, DATE '${data._1}', DATE '${data._2}')",
              data._3
            )
            testSqlApi( // sql tsi
              s"TIMESTAMPDIFF(${unitParts._3}, DATE '${data._1}', DATE '${data._2}')",
              data._3
            )
        }
      }
    }

    testAllApis(
      timestampDiff(TimePointUnit.DAY, nullOf(Types.SQL_TIMESTAMP),
        "2016-02-24 12:42:25".toTimestamp),
      "timestampDiff(DAY, nullOf(SQL_TIMESTAMP), '2016-02-24 12:42:25'.toTimestamp)",
      "TIMESTAMPDIFF(DAY, CAST(NULL AS TIMESTAMP), TIMESTAMP '2016-02-24 12:42:25')",
      "null"
    )

    testAllApis(
      timestampDiff(TimePointUnit.DAY, "2016-02-24 12:42:25".toTimestamp,
        nullOf(Types.SQL_TIMESTAMP)),
      "timestampDiff(DAY, '2016-02-24 12:42:25'.toTimestamp,  nullOf(SQL_TIMESTAMP))",
      "TIMESTAMPDIFF(DAY, TIMESTAMP '2016-02-24 12:42:25',  CAST(NULL AS TIMESTAMP))",
      "null"
    )
  }

  @Test
  def testTimestampAdd(): Unit = {
    val data = Seq(
      (1, "2017-11-29 22:58:58.998"),
      (3, "2017-11-29 22:58:58.998"),
      (-1, "2017-11-29 22:58:58.998"),
      (-61, "2017-11-29 22:58:58.998"),
      (-1000, "2017-11-29 22:58:58.998")
    )

    val YEAR = Seq(
      "2018-11-29 22:58:58.998",
      "2020-11-29 22:58:58.998",
      "2016-11-29 22:58:58.998",
      "1956-11-29 22:58:58.998",
      "1017-11-29 22:58:58.998")

    val QUARTER = Seq(
      "2018-03-01 22:58:58.998",
      "2018-08-31 22:58:58.998",
      "2017-08-29 22:58:58.998",
      "2002-08-29 22:58:58.998",
      "1767-11-29 22:58:58.998")

    val MONTH = Seq(
      "2017-12-29 22:58:58.998",
      "2018-03-01 22:58:58.998",
      "2017-10-29 22:58:58.998",
      "2012-10-29 22:58:58.998",
      "1934-07-29 22:58:58.998")

    val WEEK = Seq(
      "2017-12-06 22:58:58.998",
      "2017-12-20 22:58:58.998",
      "2017-11-22 22:58:58.998",
      "2016-09-28 22:58:58.998",
      "1998-09-30 22:58:58.998")

    val DAY = Seq(
      "2017-11-30 22:58:58.998",
      "2017-12-02 22:58:58.998",
      "2017-11-28 22:58:58.998",
      "2017-09-29 22:58:58.998",
      "2015-03-05 22:58:58.998")

    val HOUR = Seq(
      "2017-11-29 23:58:58.998",
      "2017-11-30 01:58:58.998",
      "2017-11-29 21:58:58.998",
      "2017-11-27 09:58:58.998",
      "2017-10-19 06:58:58.998")

    val MINUTE = Seq(
      "2017-11-29 22:59:58.998",
      "2017-11-29 23:01:58.998",
      "2017-11-29 22:57:58.998",
      "2017-11-29 21:57:58.998",
      "2017-11-29 06:18:58.998")

    val SECOND = Seq(
      "2017-11-29 22:58:59.998",
      "2017-11-29 22:59:01.998",
      "2017-11-29 22:58:57.998",
      "2017-11-29 22:57:57.998",
      "2017-11-29 22:42:18.998")

    // we do not supported FRAC_SECOND, MICROSECOND, SQL_TSI_FRAC_SECOND, SQL_TSI_MICROSECOND
    val intervalMapResults = Map(
      "YEAR" -> YEAR,
      "SQL_TSI_YEAR" -> YEAR,
      "QUARTER" -> QUARTER,
      "SQL_TSI_QUARTER" -> QUARTER,
      "MONTH" -> MONTH,
      "SQL_TSI_MONTH" -> MONTH,
      "WEEK" -> WEEK,
      "SQL_TSI_WEEK" -> WEEK,
      "DAY" -> DAY,
      "SQL_TSI_DAY" -> DAY,
      "HOUR" -> HOUR,
      "SQL_TSI_HOUR" -> HOUR,
      "MINUTE" -> MINUTE,
      "SQL_TSI_MINUTE" -> MINUTE,
      "SECOND" -> SECOND,
      "SQL_TSI_SECOND" -> SECOND
    )

    def intervalCount(interval: String, count: Int): (Expression, String) = interval match {
      case "YEAR" => (count.years, s"$count.years")
      case "SQL_TSI_YEAR" => (count.years, s"$count.years")
      case "QUARTER" => (count.quarters, s"$count.quarters")
      case "SQL_TSI_QUARTER" => (count.quarters, s"$count.quarters")
      case "MONTH" => (count.months, s"$count.months")
      case "SQL_TSI_MONTH" => (count.months, s"$count.months")
      case "WEEK" => (count.weeks, s"$count.weeks")
      case "SQL_TSI_WEEK" => (count.weeks, s"$count.weeks")
      case "DAY" => (count.days, s"$count.days")
      case "SQL_TSI_DAY" => (count.days, s"$count.days")
      case "HOUR" => (count.hours, s"$count.hours")
      case "SQL_TSI_HOUR" => (count.hours, s"$count.hours")
      case "MINUTE" => (count.minutes, s"$count.minutes")
      case "SQL_TSI_MINUTE" => (count.minutes, s"$count.minutes")
      case "SECOND" => (count.seconds, s"$count.seconds")
      case "SQL_TSI_SECOND" => (count.seconds, s"$count.seconds")
    }

    for ((interval, result) <- intervalMapResults) {
        for (i <- 0 to 4) {
          val (offset, ts) = data(i)
          val timeInterval = intervalCount(interval, offset)
          testAllApis(
            timeInterval._1 + ts.toTimestamp,
            s"${timeInterval._2} + '$ts'.toTimestamp",
            s"TIMESTAMPADD($interval, $offset, TIMESTAMP '$ts')",
            result(i))
        }
    }

    testAllApis(
      "2016-02-24 12:42:25".toTimestamp + nullOf(Types.INTERVAL_MILLIS),
      "'2016-02-24 12:42:25'.toTimestamp + nullOf(INTERVAL_MILLIS)",
      "TIMESTAMPADD(HOUR, CAST(NULL AS INTEGER), TIMESTAMP '2016-02-24 12:42:25')",
      "null")

    testAllApis(
      nullOf(Types.SQL_TIMESTAMP) + -200.hours,
      "nullOf(SQL_TIMESTAMP) + -200.hours",
      "TIMESTAMPADD(HOUR, -200, CAST(NULL AS TIMESTAMP))",
      "null")

    testAllApis(
      nullOf(Types.SQL_TIMESTAMP) + 3.months,
      "nullOf(SQL_TIMESTAMP) + 3.months",
      "TIMESTAMPADD(MONTH, 3, CAST(NULL AS TIMESTAMP))",
      "null")

    // TIMESTAMPADD with DATE returns a TIMESTAMP value for sub-day intervals.
    testAllApis("2016-06-15".toDate + 1.month,
      "'2016-06-15'.toDate + 1.month",
      "timestampadd(MONTH, 1, date '2016-06-15')",
      "2016-07-15")

    testAllApis("2016-06-15".toDate + 1.day,
      "'2016-06-15'.toDate + 1.day",
      "timestampadd(DAY, 1, date '2016-06-15')",
      "2016-06-16")

    testAllApis("2016-06-15".toTimestamp - 1.hour,
      "'2016-06-15'.toTimestamp - 1.hour",
      "timestampadd(HOUR, -1, date '2016-06-15')",
      "2016-06-14 23:00:00.0")

    testAllApis("2016-06-15".toTimestamp + 1.minute,
      "'2016-06-15'.toTimestamp + 1.minute",
      "timestampadd(MINUTE, 1, date '2016-06-15')",
      "2016-06-15 00:01:00.0")

    testAllApis("2016-06-15".toTimestamp - 1.second,
      "'2016-06-15'.toTimestamp - 1.second",
      "timestampadd(SQL_TSI_SECOND, -1, date '2016-06-15')",
      "2016-06-14 23:59:59.0")

    testAllApis("2016-06-15".toTimestamp + 1.second,
      "'2016-06-15'.toTimestamp + 1.second",
      "timestampadd(SECOND, 1, date '2016-06-15')",
      "2016-06-15 00:00:01.0")

    testAllApis(nullOf(Types.SQL_TIMESTAMP) + 1.second,
      "nullOf(SQL_TIMESTAMP) + 1.second",
      "timestampadd(SECOND, 1, cast(null as date))",
      "null")

    testAllApis(nullOf(Types.SQL_TIMESTAMP) + 1.day,
      "nullOf(SQL_TIMESTAMP) + 1.day",
      "timestampadd(DAY, 1, cast(null as date))",
      "null")

    // Round to the last day of previous month
    testAllApis("2016-05-31".toDate + 1.month,
      "'2016-05-31'.toDate + 1.month",
      "timestampadd(MONTH, 1, date '2016-05-31')",
      "2016-06-30")

    testAllApis("2016-01-31".toDate + 5.month,
      "'2016-01-31'.toDate + 5.month",
      "timestampadd(MONTH, 5, date '2016-01-31')",
      "2016-06-30")

    testAllApis("2016-03-31".toDate - 1.month,
      "'2016-03-31'.toDate - 1.month",
      "timestampadd(MONTH, -1, date '2016-03-31')",
      "2016-02-29")

    testAllApis("2016-03-31".toDate - 1.week,
      "'2016-03-31'.toDate - 1.week",
      "timestampadd(WEEK, -1, date '2016-03-31')",
      "2016-03-24")

    // test TIMESTAMPADD with positive time interval in various granularity.
    testSqlApi("TIMESTAMPADD(SECOND, 1, time '23:59:59')", "00:00:00")
    testSqlApi("TIMESTAMPADD(MINUTE, 1, time '00:00:00')", "00:01:00")
    testSqlApi("TIMESTAMPADD(MINUTE, 1, time '23:59:59')", "00:00:59")
    testSqlApi("TIMESTAMPADD(HOUR, 1, time '23:59:59')", "00:59:59")
    testSqlApi("TIMESTAMPADD(DAY, 15, time '23:59:59')", "23:59:59")
    testSqlApi("TIMESTAMPADD(WEEK, 3, time '23:59:59')", "23:59:59")
    testSqlApi("TIMESTAMPADD(MONTH, 6, time '23:59:59')", "23:59:59")
    testSqlApi("TIMESTAMPADD(QUARTER, 1, time '23:59:59')", "23:59:59")
    testSqlApi("TIMESTAMPADD(YEAR, 10, time '23:59:59')", "23:59:59")

    // test TIMESTAMPADD with negative time interval in various granularity.
    testSqlApi("TIMESTAMPADD(SECOND, -1, time '00:00:00')", "23:59:59")
    testSqlApi("TIMESTAMPADD(MINUTE, -1, time '00:00:00')", "23:59:00")
    testSqlApi("TIMESTAMPADD(MINUTE, -1, time '00:00:59')", "23:59:59")
    testSqlApi("TIMESTAMPADD(HOUR, -1, time '00:00:00')", "23:00:00")
    testSqlApi("TIMESTAMPADD(DAY, -1, time '23:59:59')", "23:59:59")
    testSqlApi("TIMESTAMPADD(WEEK, -1, time '23:59:59')", "23:59:59")
    testSqlApi("TIMESTAMPADD(MONTH, -1, time '23:59:59')", "23:59:59")
    testSqlApi("TIMESTAMPADD(QUARTER, -1, time '23:59:59')", "23:59:59")
    testSqlApi("TIMESTAMPADD(YEAR, -1, time '23:59:59')", "23:59:59")
  }

  // ----------------------------------------------------------------------------------------------
  // Hash functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testHashFunctions(): Unit = {
    val expectedMd5 = "098f6bcd4621d373cade4e832627b4f6"
    val expectedSha1 = "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"
    val expectedSha224 = "90a3ed9e32b2aaf4c61c410eb925426119e1a9dc53d4286ade99a809"
    val expectedSha256 = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
    val expectedSha384 = "768412320f7b0aa5812fce428dc4706b3cae50e02a64caa16a7" +
      "82249bfe8efc4b7ef1ccb126255d196047dfedf17a0a9"
    val expectedSha512 = "ee26b0dd4af7e749aa1a8ee3c10ae9923f618980772e473f8819a" +
      "5d4940e0db27ac185f8a0e1d5f84f88bc887fd67b143732c304cc5fa9ad8e6f57f50028a8ff"

    testAllApis(
      "test".md5(),
      "md5('test')",
      "MD5('test')",
      expectedMd5)

    testAllApis(
      "test".sha1(),
      "sha1('test')",
      "SHA1('test')",
      expectedSha1)

    // sha224
    testAllApis(
      "test".sha224(),
      "sha224('test')",
      "SHA224('test')",
      expectedSha224)

    // sha-2 224
    testAllApis(
      "test".sha2(224),
      "sha2('test', 224)",
      "SHA2('test', 224)",
      expectedSha224)

    // sha256
    testAllApis(
      "test".sha256(),
      "sha256('test')",
      "SHA256('test')",
      expectedSha256)

    // sha-2 256
    testAllApis(
      "test".sha2(256),
      "sha2('test', 256)",
      "SHA2('test', 256)",
      expectedSha256)

    // sha384
    testAllApis(
      "test".sha384(),
      "sha384('test')",
      "SHA384('test')",
      expectedSha384)

    // sha-2 384
    testAllApis(
      "test".sha2(384),
      "sha2('test', 384)",
      "SHA2('test', 384)",
      expectedSha384)

    // sha512
    testAllApis(
      "test".sha512(),
      "sha512('test')",
      "SHA512('test')",
      expectedSha512)

    // sha-2 512
    testAllApis(
      "test".sha2(512),
      "sha2('test', 512)",
      "SHA2('test', 512)",
      expectedSha512)

    // null tests
    testAllApis(
      'f33.md5(),
      "md5(f33)",
      "MD5(f33)",
      "null")

    testAllApis(
      'f33.sha1(),
      "sha1(f33)",
      "SHA1(f33)",
      "null")

    testAllApis(
      'f33.sha224(),
      "sha224(f33)",
      "SHA2(f33, 224)",
      "null")

    testAllApis(
      'f33.sha2(224),
      "sha2(f33, 224)",
      "SHA2(f33, 224)",
      "null")

    testAllApis(
      'f33.sha256(),
      "sha256(f33)",
      "SHA2(f33, 256)",
      "null")

    testAllApis(
      'f33.sha384(),
      "sha384(f33)",
      "SHA2(f33, 384)",
      "null")

    testAllApis(
      'f33.sha512(),
      "sha512(f33)",
      "SHA2(f33, 512)",
      "null")

    testAllApis(
      "test".sha2(nullOf(Types.INT)),
      "sha2('test', nullOf(INT))",
      "SHA2('test', CAST(NULL AS INT))",
      "null")

    // non-constant bit length
    testAllApis(
      "test".sha2('f34),
      "sha2('test', f34)",
      "SHA2('test', f34)",
      expectedSha256)
  }

  // ----------------------------------------------------------------------------------------------
  // Other functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testIsTrueIsFalse(): Unit = {
    testAllApis(
      'f1.isTrue,
      "f1.isTrue",
      "f1 IS TRUE",
      "true")

    testAllApis(
      'f21.isTrue,
      "f21.isTrue",
      "f21 IS TRUE",
      "false")

    testAllApis(
      false.isFalse,
      "false.isFalse",
      "FALSE IS FALSE",
      "true")

    testAllApis(
      'f21.isFalse,
      "f21.isFalse",
      "f21 IS FALSE",
      "false")

    testAllApis(
      'f1.isNotTrue,
      "f1.isNotTrue",
      "f1 IS NOT TRUE",
      "false")

    testAllApis(
      'f21.isNotTrue,
      "f21.isNotTrue",
      "f21 IS NOT TRUE",
      "true")

    testAllApis(
      false.isNotFalse,
      "false.isNotFalse",
      "FALSE IS NOT FALSE",
      "false")

    testAllApis(
      'f21.isNotFalse,
      "f21.isNotFalse",
      "f21 IS NOT FALSE",
      "true")
  }
}
