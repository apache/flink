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

import org.apache.flink.table.expressions.utils.ScalarTypesTestBase
import org.junit.Test

import java.io.{ByteArrayOutputStream, PrintStream}

class ScalarFunctionsTest extends ScalarTypesTestBase {

  // ----------------------------------------------------------------------------------------------
  // String functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testOverlay(): Unit = {
    // (tableApiString, sqlApiString, expectedResult)
    val cases = Seq(
      // constants, no length argument
      ("'xxxxxtest'.overlay('xxxx', 6)",
        "OVERLAY('xxxxxtest' PLACING 'xxxx' FROM 6)",
        "xxxxxxxxx"),

      // constants
      ("'xxxxxtest'.overlay('xxxx', 6, 2)",
        "OVERLAY('xxxxxtest' PLACING 'xxxx' FROM 6 FOR 2)",
        "xxxxxxxxxst"),

      // invalid position on the constants
      ("'123456789'.overlay('abc', 100, 2)",
        "OVERLAY('123456789' PLACING 'It' FROM -1 FOR 4)",
        "123456789"),

      // invalid position on the constants
      ("'123456789'.overlay('abc', -1, 2)",
        "OVERLAY('123456789' PLACING 'It' FROM -1 FOR 2)",
        "123456789"),

      // invalid len on the constants
      ("'123456789'.overlay('abc', 2, 100)",
        "OVERLAY('123456789' PLACING 'abc' FROM 2 FOR 100)",
        "1abc"),

      // invalid len on the constants
      ("'123456789'.overlay('abc', 2, -1)",
        "OVERLAY('123456789' PLACING 'abc' FROM 2 FOR -1)",
        "1abc"),

      // invalid start & len on the constants
      ("'123456789'.overlay('abc', 100, -1)",
        "OVERLAY('123456789' PLACING 'abc' FROM 100 FOR -1)",
        "123456789"),

      // field
      ("f0.overlay('It', 1, 4)",
        "OVERLAY(f0 PLACING 'It' FROM 1 FOR 4)",
        "It is a test String."),

      // invalid position
      ("f0.overlay('It', -1, 4)",
        "OVERLAY(f0 PLACING 'It' FROM -1 FOR 4)",
        "This is a test String."),

      // invalid position
      ("f0.overlay('It', 100, 4)",
        "OVERLAY(f0 PLACING 'It' FROM 100 FOR 4)",
        "This is a test String."),

      // invalid position
      ("f0.overlay('It', -1, 2)",
        "OVERLAY(f0 PLACING 'It' FROM -1 FOR 2)",
        "This is a test String."),

      // invalid position
      ("f0.overlay('It', 100, 2)",
        "OVERLAY(f0 PLACING 'It' FROM 100 FOR 2)",
        "This is a test String."),

      // invalid length
      ("f0.overlay('IS', 6, 100)",
        "OVERLAY(f0 PLACING 'IS' FROM 6 FOR 100)",
        "This IS"),

      // invalid length
      ("f0.overlay('IS', 6, -1)",
        "OVERLAY(f0 PLACING 'IS' FROM 6 FOR -1)",
        "This IS"),

      // null field. f40 is NULL.
      ("f40.overlay('It', 1, 4)",
        "OVERLAY(f40 PLACING 'It' FROM 1 FOR 2)",
        "null")
    )

    cases.foreach(x => {
      // TODO: ignore Table API currently
      testSqlApi(
        x._2,
        x._3
      )
    })

  }

  @Test
  def testPosition(): Unit = {
    testSqlApi(
      "POSITION('test' IN 'xxxtest')",
      "4")

    testSqlApi(
      "POSITION('testx' IN 'xxxtest')",
      "0")

    testSqlApi(
      "POSITION('aa' IN 'aaads')",
      "1")
  }

  @Test
  def testLocate(): Unit = {
    testSqlApi(
      "locate('test', 'xxxtest')",
      "4")

    testSqlApi(
      "locate('testx', 'xxxtest')",
      "0")

    testSqlApi("locate('aa',  'aaads')", "1")

    testSqlApi("locate('aa', 'aaads', 2)", "2")
  }

  @Test
  def testLeft(): Unit = {
    testSqlApi(
      "`LEFT`(f0, 2)",
      "Th")

    testSqlApi(
      "`LEFT`(f0, 100)",
      "This is a test String.")

    testSqlApi(
      "`LEFT`(f0, -2)",
      "")

    testSqlApi(
      "`LEFT`(f0, 0)",
      "")

    testSqlApi(
      "`LEFT`(f0, CAST(null as Integer))",
      "null")

    testSqlApi(
      "`LEFT`(CAST(null as VARCHAR), -2)",
      "null")

    testSqlApi(
      "`LEFT`(CAST(null as VARCHAR), 2)",
      "null")
  }

  @Test
  def testRight(): Unit = {
    testSqlApi(
      "`right`(f0, 2)",
      "g.")

    testSqlApi(
      "`right`(f0, 100)",
      "This is a test String.")

    testSqlApi(
      "`right`(f0, -2)",
      "")

    testSqlApi(
      "`right`(f0, 0)",
      "")

    testSqlApi(
      "`right`(f0, CAST(null as Integer))",
      "null")

    testSqlApi(
      "`right`(CAST(null as VARCHAR), -2)",
      "null")

    testSqlApi(
      "`right`(CAST(null as VARCHAR), 2)",
      "null")
  }

  @Test
  def testAscii(): Unit = {
    testSqlApi(
      "ascii('efg')",
      "101")

    testSqlApi(
      "ascii('abcdef')",
      "97")

    testSqlApi(
      "ascii('')",
      "0")

    testSqlApi(
      "ascii(cast (null AS VARCHAR))",
      "null"
    )

    testSqlApi(
      "ascii('val_238') = ascii('val_239')",
      "true"
    )
  }

  @Test
  def testInstr(): Unit = {
    testSqlApi(
      "instr('Corporate Floor', 'or', 3, 2)",
      "14")

    testSqlApi(
      "instr('Corporate Floor', 'or', -3, 2)",
      "2")

    testSqlApi(
      "instr('Tech on the net', 'e')",
      "2")

    testSqlApi(
      "instr('Tech on the net', 'e', 1, 2)",
      "11")

    testSqlApi(
      "instr('Tech on the net', 'e', 1, 3)",
      "14")

    testSqlApi(
      "instr('Tech on the net', 'e', -3, 2)",
      "2")

    testSqlApi(
      "instr('myteststring', 'st')",
      "5")

    testSqlApi(
      "instr(cast (null AS VARCHAR), 'e')",
      "null"
    )

    testSqlApi(
      "instr('e', cast (null AS VARCHAR))",
      "null"
    )

    testSqlApi(
      "instr('val_238', '_') = instr('val_239', '_')",
      "true"
    )

      testSqlApi(
      "instr('val_239', '_')",
      "4"
    )
  }

  @Test
  def testSubstring(): Unit = {
    testSqlApi(
      "SUBSTRING(f0, 2)",
      "his is a test String.")

    testSqlApi(
      "SUBSTRING(f0, 2, 5)",
      "his i")

    testSqlApi(
      "SUBSTRING(f0, 1, f7)",
      "Thi")

    testSqlApi(
      "SUBSTRING(f0, CAST(1 AS TINYINT), f7)",
      "Thi")

    testSqlApi(
      "SUBSTRING(f0 FROM 2 FOR 1)",
      "h")

    testSqlApi(
      "SUBSTRING(f0 FROM 2)",
      "his is a test String.")

    testSqlApi(
      "SUBSTRING(f0 FROM -2)",
      "g.")

    testSqlApi(
      "SUBSTRING(f0 FROM -2 FOR 1)",
      "g")

    testSqlApi(
      "SUBSTRING(f0 FROM -2 FOR 0)",
      "")
  }

  @Test
  def testTrim(): Unit = {
    testSqlApi(
      "TRIM(f8)",
      "This is a test String.")

    testSqlApi(
      "TRIM(f8)",
      "This is a test String.")

    testSqlApi(
      "TRIM(TRAILING FROM f8)",
      " This is a test String.")

    testSqlApi(
      "TRIM(BOTH '.' FROM f0)",
      "This is a test String")

    testSqlApi(
      "trim(BOTH 'abc' FROM 'abcddcba')",
      "dd")

    testSqlApi(
      "trim(BOTH 'abd' FROM 'abcddcba')",
      "cddc")

    testSqlApi(
      "trim(BOTH '开心' FROM '心情开开心心')",
      "情")

    testSqlApi(
      "trim(BOTH '开心' FROM '心情开开心心')",
      "情")

    testSqlApi("trim(LEADING  from '  example  ')", "example  ")
    testSqlApi("trim(TRAILING from '  example  ')", "  example")
    testSqlApi("trim(BOTH     from '  example  ')", "example")

    testSqlApi("trim(LEADING  'e' from 'example')", "xample")
    testSqlApi("trim(TRAILING 'e' from 'example')", "exampl")
    testSqlApi("trim(BOTH     'e' from 'example')", "xampl")

    testSqlApi("trim(BOTH     'xyz' from 'example')", "example")
  }

  @Test
  def testLTrim(): Unit = {
    testSqlApi(
      "LTRIM(f8)",
      "This is a test String. ")

    testSqlApi(
      "LTRIM(f8)",
      "This is a test String. ")

    testSqlApi(
      "LTRIM(f0, 'This ')",
      "a test String.")

    testSqlApi(
      "ltrim('abcddcba', 'abc')",
      "ddcba")

    testSqlApi(
      "LTRIM('abcddcba', 'abd')",
      "cddcba")

    testSqlApi(
      "ltrim('心情开开心心', '开心')",
      "情开开心心")

    testSqlApi(
      "LTRIM('abcddcba', CAST(null as VARCHAR))",
      "null")

    testSqlApi(
      "LTRIM(CAST(null as VARCHAR), 'abcddcba')",
      "null")
  }

  @Test
  def testRTrim(): Unit = {
    testSqlApi(
      "rtrim(f8)",
      " This is a test String.")

    testSqlApi(
      "rtrim(f8)",
      " This is a test String.")

    testSqlApi(
      "rtrim(f0, 'String. ')",
      "This is a tes")

    testSqlApi(
      "rtrim('abcddcba', 'abc')",
      "abcdd")

    testSqlApi(
      "rtrim('abcddcba', 'abd')",
      "abcddc")

    testSqlApi(
      "rtrim('心情开开心心', '开心')",
      "心情")

    testSqlApi(
      "rtrim('abcddcba', CAST(null as VARCHAR))",
      "null")

    testSqlApi(
      "rtrim(CAST(null as VARCHAR), 'abcddcba')",
      "null")
  }

  @Test
  def testCharLength(): Unit = {
    testSqlApi(
      "CHAR_LENGTH(f0)",
      "22")

    testSqlApi(
      "CHARACTER_LENGTH(f0)",
      "22")
  }

  @Test
  def testLength(): Unit = {
    testSqlApi(
      "LENGTH(f0)",
      "22")

    testSqlApi(
      "LENGTH(f0)",
      "22")

    testSqlApi(
      "length(uuid())",
      "36")
  }

  @Test
  def testUpperCase(): Unit = {
    testSqlApi(
      "UPPER(f0)",
      "THIS IS A TEST STRING.")
  }

  @Test
  def testLowerCase(): Unit = {
    testSqlApi(
      "LOWER(f0)",
      "this is a test string.")
  }

  @Test
  def testInitCap(): Unit = {
    testSqlApi(
      "INITCAP(f0)",
      "This Is A Test String.")

    testSqlApi("INITCAP('ab')", "Ab")
    testSqlApi("INITCAP('a B')", "A B")
    testSqlApi("INITCAP('fLinK')", "Flink")
  }

  @Test
  def testConcat(): Unit = {
    testSqlApi(
      "f0||f0",
      "This is a test String.This is a test String.")
  }

  @Test
  def testLike(): Unit = {
    testSqlApi(
      "f0 LIKE 'Th_s%'",
      "true")

    testSqlApi(
      "f0 LIKE '%is a%'",
      "true")

    testSqlApi("'abcxxxdef' LIKE 'abcx%'", "true")
    testSqlApi("'abcxxxdef' LIKE '%%def'", "true")
    testSqlApi("'abcxxxdef' LIKE 'abcxxxdef'", "true")
    testSqlApi("'abcxxxdef' LIKE '%xdef'", "true")
    testSqlApi("'abcxxxdef' LIKE 'abc%def%'", "true")
    testSqlApi("'abcxxxdef' LIKE '%abc%def'", "true")
    testSqlApi("'abcxxxdef' LIKE '%abc%def%'", "true")
    testSqlApi("'abcxxxdef' LIKE 'abc%def'", "true")

    // false
    testSqlApi("'abcxxxdef' LIKE 'abdxxxdef'", "false")
    testSqlApi("'abcxxxdef' LIKE '%xqef'", "false")
    testSqlApi("'abcxxxdef' LIKE 'abc%qef%'", "false")
    testSqlApi("'abcxxxdef' LIKE '%abc%qef'", "false")
    testSqlApi("'abcxxxdef' LIKE '%abc%qef%'", "false")
    testSqlApi("'abcxxxdef' LIKE 'abc%qef'", "false")
  }

  @Test
  def testNotLike(): Unit = {
    testSqlApi(
      "f0 NOT LIKE 'Th_s%'",
      "false")

    testSqlApi(
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
    testSqlApi(
      "f0 SIMILAR TO '_*'",
      "true")

    testSqlApi(
      "f0 SIMILAR TO 'This (is)? a (test)+ Strin_*'",
      "true")
  }

  @Test
  def testNotSimilar(): Unit = {
    testSqlApi(
      "f0 NOT SIMILAR TO '_*'",
      "false")

    testSqlApi(
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
    testSqlApi(
      "CONCAT('xx', f33)",
      "xx")
    testSqlApi(
      "CONCAT('AA','BB','CC','---')",
      "AABBCC---")
    testSqlApi(
      "CONCAT('x~x','b~b','c~~~~c','---')",
      "x~xb~bc~~~~c---")

    testSqlApi("concat(f35)", "a")
    testSqlApi("concat(f35,f36)", "ab")
    testSqlApi("concat(f35,f36,f33)", "ab")
  }

  @Test
  def testConcatWs(): Unit = {
    testSqlApi(
      "CONCAT_WS(f33, 'AA')",
      "AA")
    testSqlApi(
      "concat_ws('~~~~','AA')",
      "AA")
    testSqlApi(
      "concat_ws('~','AA','BB')",
      "AA~BB")
    testSqlApi(
      "concat_ws('~',f33, 'AA','BB','',f33, 'CC')",
      "AA~BB~~CC")
    testSqlApi(
      "CONCAT_WS('~~~~','Flink', f33, 'xx', f33, f33)",
      "Flink~~~~xx")

    testSqlApi("concat_ws('||', f35, f36, f33)", "a||b")
  }

  def testRegexpReplace(): Unit = {

    testSqlApi(
      "regexp_replace('foobar', 'oo|ar', 'abc')",
      "fabcbabc")

    testSqlApi(
      "regexp_replace('foofar', '^f', '')",
      "oofar")

    testSqlApi(
      "regexp_replace('foobar', '^f*.*r$', '')",
      "")

    testSqlApi(
      "regexp_replace('foobar', '\\d', '')",
      "foobar")

    testSqlApi(
      "regexp_replace('foobar', '\\w', '')",
      "")

    testSqlApi(
      "regexp_replace('fooobar', 'oo', '$')",
      "f$obar")

    testSqlApi(
      "regexp_replace('foobar', 'oo', '\\')",
      "f\\bar")

    testSqlApi(
      "REGEXP_REPLACE(f33, 'oo|ar', '')",
      "null")

    testSqlApi(
      "REGEXP_REPLACE('foobar', f33, '')",
      "null")

    testSqlApi(
      "REGEXP_REPLACE('foobar', 'oo|ar', f33)",
      "null")

    // This test was added for the null literal problem in string expression parsing (FLINK-10463).
    testSqlApi(
      "REGEXP_REPLACE(CAST(NULL AS VARCHAR), 'oo|ar', f33)",
      "null")

    testSqlApi("regexp_replace('100-200', '(\\d+)', 'num')", "num-num")
    testSqlApi("regexp_replace('100-200', '(\\d+)-(\\d+)', '400')", "400")
    testSqlApi("regexp_replace('100-200', '(\\d+)', '400')", "400-400")
    testSqlApi("regexp_replace('100-200', '', '400')", "100-200")
    testSqlApi("regexp_replace(f40, '(\\d+)', '400')", "null")
    testSqlApi("regexp_replace(CAST(null as VARCHAR), '(\\d+)', 'num')", "null")
    testSqlApi("regexp_replace('100-200', CAST(null as VARCHAR), '400')", "null")
    testSqlApi("regexp_replace('100-200', '(\\d+)', CAST(null as VARCHAR))", "null")
  }

  @Test
  def testRegexpExtract(): Unit = {
    testSqlApi(
      "REGEXP_EXTRACT('foothebar', 'foo(.*?)(bar)', 2)",
      "bar")

    testSqlApi(
      "REGEXP_EXTRACT('foothebar', 'foo(.*?)(bar)', 0)",
      "foothebar")

    testSqlApi(
      "REGEXP_EXTRACT('foothebar', 'foo(.*?)(bar)', 1)",
      "the")

    testSqlApi(
      "REGEXP_EXTRACT('foothebar', 'foo([\\w]+)', 1)",
      "thebar")

    testSqlApi(
      "REGEXP_EXTRACT('foothebar', 'foo([\\d]+)', 1)",
      "null")

    testSqlApi(
      "REGEXP_EXTRACT(f33, 'foo(.*?)(bar)', 2)",
      "null")

    testSqlApi(
      "REGEXP_EXTRACT('foothebar', f33, 2)",
      "null")

    //test for optional group index
    testSqlApi(
      "REGEXP_EXTRACT('foothebar', 'foo(.*?)(bar)')",
      "foothebar")

    testSqlApi("regexp_extract('100-200', '(\\d+)-(\\d+)', 1)", "100")
    testSqlApi("regexp_extract('100-200', '', 1)", "null")
    testSqlApi("regexp_extract('100-200', '(\\d+)-(\\d+)', -1)", "null")
    testSqlApi("regexp_extract(f40, '(\\d+)-(\\d+)', 1)", "null")
    testSqlApi("regexp_extract(CAST(null as VARCHAR), '(\\d+)-(\\d+)', 1)", "null")
    testSqlApi("regexp_extract('100-200', CAST(null as VARCHAR), 1)", "null")
    testSqlApi("regexp_extract('100-200', '(\\d+)-(\\d+)', CAST(null as BIGINT))", "null")
  }

  @Test
  def testFromBase64(): Unit = {
    testSqlApi(
      "FROM_BASE64('aGVsbG8gd29ybGQ=')",
      "hello world")

    //null test
    testSqlApi(
      "FROM_BASE64(f33)",
      "null")

    testSqlApi(
      "FROM_BASE64('5L2g5aW9')",
      "你好"
    )
  }

  @Test
  def testToBase64(): Unit = {
    testSqlApi(
      "TO_BASE64(f0)",
      "VGhpcyBpcyBhIHRlc3QgU3RyaW5nLg==")

    testSqlApi(
      "TO_BASE64(f8)",
      "IFRoaXMgaXMgYSB0ZXN0IFN0cmluZy4g")

    testSqlApi(
      "TO_BASE64('')",
      "")

    //null test
    testSqlApi(
      "TO_BASE64(f33)",
      "null")

    testSqlApi(
      "TO_BASE64('你好')",
      "5L2g5aW9"
    )

    testSqlApi(
      "to_base64(f37)",
      "AQIDBA==")
    testSqlApi(
      "to_base64(from_base64(f38))",
      "AQIDBA==")
  }

  @Test
  def testUUID(): Unit = {
    testSqlApi(
      "CHARACTER_LENGTH(UUID())",
      "36")

    testSqlApi(
      "SUBSTRING(UUID(), 9, 1)",
      "-")

    testSqlApi(
      "SUBSTRING(UUID(), 14, 1)",
      "-")

    testSqlApi(
      "SUBSTRING(UUID(), 19, 1)",
      "-")

    testSqlApi(
      "SUBSTRING(UUID(), 24, 1)",
      "-")

    // test uuid with bytes
    testSqlApi(
      "UUID(f53)",
      "5eb63bbb-e01e-3ed0-93cb-22bb8f5acdc3"
    )
  }

  @Test
  def testSubString(): Unit = {
    Array("substring", "substr").foreach {
      substr =>
        testSqlApi(s"$substr(f0, 2, 3)", "his")
        testSqlApi(s"$substr(f0, 2, 100)", "his is a test String.")
        testSqlApi(s"$substr(f0, 100, 10)", "")
        testSqlApi(s"$substr(f0, 2, -1)", "null")
        testSqlApi(s"$substr(f40, 2, 3)", "null")
        testSqlApi(s"$substr(CAST(null AS VARCHAR), 2, 3)", "null")
        testSqlApi(s"$substr(f0, 2, f14)", "null")
        testSqlApi(s"$substr(f0, f30, f7)", "Thi")
        testSqlApi(s"$substr(f39, 1, 2)", "1世")
    }
  }

  @Test
  def testLPad(): Unit = {
    testSqlApi("lpad(f33,1,'??')", "null")
    testSqlApi("lpad(f35, 1, '??')", "a")
    testSqlApi("lpad(f35, 2, '??')", "?a")
    testSqlApi("lpad(f35, 5, '??')", "????a")
    testSqlApi("lpad(f35, CAST(null as INT), '??')", "null")
    testSqlApi("lpad(f35, 5, CAST(null as VARCHAR))", "null")
    testSqlApi("lpad(f40, 1, '??')", "null")
    testSqlApi("lpad('hi', 1, '??')", "h")
    testSqlApi("lpad('hi', 5, '??')", "???hi")
    testSqlApi("lpad(CAST(null as VARCHAR), 5, '??')", "null")
    testSqlApi("lpad('hi', CAST(null as INT), '??')", "null")
    testSqlApi("lpad('hi', 5, CAST(null as VARCHAR))", "null")
    testSqlApi("lpad('',1,'??')", "?")
    testSqlApi("lpad('',30,'??')", "??????????????????????????????")
    testSqlApi("lpad('111',-2,'??')", "null")
    testSqlApi("lpad('\u0061\u0062',1,'??')", "a") // the unicode of ab is \u0061\u0062
    testSqlApi("lpad('⎨⎨',1,'??')", "⎨")
    testSqlApi("lpad('äääääääää',2,'??')", "ää")
    testSqlApi("lpad('äääääääää',10,'??')", "?äääääääää")
    testSqlApi("lpad('Hello', -1, 'x') IS NULL", "true")
    testSqlApi("lpad('Hello', -1, 'x') IS NOT NULL", "false")
    testSqlApi("lpad('ab', 5, '')", "null")

    testSqlApi(
      "lpad('äää',13,'12345')",
      "1234512345äää")
  }

  @Test
  def testRPad(): Unit = {
    testSqlApi("rpad(f33,1,'??')", "null")
    testSqlApi("rpad(f35, 1, '??')", "a")
    testSqlApi("rpad(f35, 2, '??')", "a?")
    testSqlApi("rpad(f35, 5, '??')", "a????")
    testSqlApi("rpad(f35, CAST(null as INT), '??')", "null")
    testSqlApi("rpad(f35, 5, CAST(null as VARCHAR))", "null")
    testSqlApi("rpad(f40, 1, '??')", "null")
    testSqlApi("rpad('hi', 1, '??')", "h")
    testSqlApi("rpad('hi', 5, '??')", "hi???")
    testSqlApi("rpad(CAST(null as VARCHAR), 5, '??')", "null")
    testSqlApi("rpad('hi', CAST(null as INT), '??')", "null")
    testSqlApi("rpad('hi', 5, CAST(null as VARCHAR))", "null")
    testSqlApi("rpad('',1,'??')", "?")
    testSqlApi("rpad('111',-2,'??')", "null")
    testSqlApi("rpad('\u0061\u0062',1,'??')", "a") // the unicode of ab is \u0061\u0062
    testSqlApi("rpad('üö',1,'??')", "ü")
    testSqlApi("rpad('abcd', 5, '')", "null")
    testSqlApi(
      "rpad('äää',13,'12345')",
      "äää1234512345")
  }

  @Test
  def testParseUrl(): Unit = {

    // NOTE: parse_url() requires HOST PATH etc. all capitalized
    def testUrl(
      url: String,
      host: String,
      path: String,
      query: String,
      ref: String,
      protocol: String,
      file: String,
      authority: String,
      userInfo: String,
      qv: String)
    : Unit = {

      val parts =
        Map(
          "HOST" -> host,
          "PATH" -> path,
          "QUERY" -> query,
          "REF" -> ref,
          "PROTOCOL" -> protocol,
          "FILE" -> file,
          "AUTHORITY" -> authority,
          "USERINFO" -> userInfo)

      for ((n, v) <- parts) {
        testSqlApi(s"parse_url('$url', '$n')", v)
      }

      testSqlApi(s"parse_url('$url', 'QUERY', 'query')", qv)
    }

    testUrl(
      "http://userinfo@flink.apache.org/path?query=1#Ref",
      "flink.apache.org", "/path", "query=1", "Ref",
      "http", "/path?query=1", "userinfo@flink.apache.org", "userinfo", "1")

    testUrl(
      "https://use%20r:pas%20s@example.com/dir%20/pa%20th.HTML?query=x%20y&q2=2#Ref%20two",
      "example.com", "/dir%20/pa%20th.HTML", "query=x%20y&q2=2", "Ref%20two",
      "https", "/dir%20/pa%20th.HTML?query=x%20y&q2=2", "use%20r:pas%20s@example.com",
      "use%20r:pas%20s", "x%20y")

    testUrl(
      "http://user:pass@host",
      "host", "", "null", "null", "http", "", "user:pass@host", "user:pass", "null")

    testUrl(
      "http://user:pass@host/",
      "host", "/", "null", "null", "http", "/", "user:pass@host", "user:pass", "null")

    testUrl(
      "http://user:pass@host/?#",
      "host", "/", "", "", "http", "/?", "user:pass@host", "user:pass", "null")

    testUrl(
      "http://user:pass@host/file;param?query;p2",
      "host", "/file;param", "query;p2", "null", "http", "/file;param?query;p2",
      "user:pass@host", "user:pass", "null")

    testUrl(
      "invalid://user:pass@host/file;param?query;p2",
      "null", "null", "null", "null", "null", "null", "null", "null", "null")
  }

  @Test
  def testRepeat(): Unit = {
    testSqlApi("repeat(f35, 2)", "aa")
    testSqlApi("repeat(f35, 0)", "")
    testSqlApi("repeat(f40, 2)", "null")
    testSqlApi("repeat('hi', 2)", "hihi")
    testSqlApi("repeat('hi', 0)", "")
    testSqlApi("repeat('hi', CAST(null as INT))", "null")
    testSqlApi("repeat(CAST(null as VARCHAR), 2)", "null")
  }

  @Test
  def testReverse(): Unit = {
    testSqlApi("reverse(f38)", "==ABDIQA")
    testSqlApi("reverse(f40)", "null")
    testSqlApi("reverse('hi')", "ih")
    testSqlApi("reverse('hhhi')", "ihhh")
    testSqlApi("reverse(CAST(null as VARCHAR))", "null")
  }

  @Test
  def testReplace(): Unit = {
    testSqlApi("replace(f38, 'A', 'a')", "aQIDBa==")
    testSqlApi("replace(f38, 'Z', 'a')", "AQIDBA==")
    testSqlApi("replace(f38, CAST(null as VARCHAR), 'a')", "null")
    testSqlApi("replace(f38, 'A', CAST(null as VARCHAR))", "null")
    testSqlApi("replace(f40, 'A', 'a')", "null")
    testSqlApi("replace('Test', 'T', 't')", "test")
    testSqlApi("replace(CAST(null as VARCHAR), 'T', 't')", "null")
    testSqlApi("replace('Test', CAST(null as VARCHAR), 't')", "null")
    testSqlApi("replace('Test', 'T', CAST(null as VARCHAR))", "null")
  }

  @Test
  def testSplitIndex(): Unit = {
    testSqlApi("split_index(f38, 'I', 0)", "AQ")
    testSqlApi("split_index(f38, 'I', 2)", "null")
    testSqlApi("split_index(f38, 'I', -1)", "null")
    testSqlApi("split_index(f38, CAST(null as VARCHAR), 0)", "null")
    testSqlApi("split_index(f38, 'I', CAST(null as INT))", "null")
    testSqlApi("split_index(f38, 'I', -1)", "null")
    testSqlApi("split_index(f40, 'I', 0)", "null")
    testSqlApi("split_index(f38, 73, 0)", "AQ")
    testSqlApi("split_index(f38, 256, 0)", "null")
    testSqlApi("split_index(f38, 0, 0)", "null")
    testSqlApi("split_index('Test', 'e', 1)", "st")
    testSqlApi("split_index(CAST(null as VARCHAR), 'e', 1)", "null")
    testSqlApi("split_index('test', CAST(null as VARCHAR), 1)", "null")
    testSqlApi("split_index('test', 'e', -1)", "null")
  }

  @Test
  def testKeyValue(): Unit = {
    // NOTE: Spark has str_to_map
    testSqlApi("keyValue('a=1,b=2,c=3', ',', '=', 'a')", "1")
    testSqlApi("keyValue('a=1,b=2,c=3', ',', '=', 'b')", "2")
    testSqlApi("keyValue('a=1,b=2,c=3', ',', '=', 'c')", "3")
    testSqlApi("keyValue('', ',', '=', 'c')", "null")
    testSqlApi("keyValue(f40, ',', '=', 'c')", "null")
    testSqlApi("keyValue(CAST(null as VARCHAR), ',', '=', 'c')", "null")
    testSqlApi("keyValue('a=1,b=2,c=3', ',', '=', 'd')", "null")
    testSqlApi("keyValue('a=1,b=2,c=3', CAST(null as VARCHAR), '=', 'a')", "null")
    testSqlApi("keyValue('a=1,b=2,c=3', ',', CAST(null as VARCHAR), 'a')", "null")
    testSqlApi("keyValue('a=1,b=2,c=3', ',', '=', CAST(null as VARCHAR))", "null")
  }

  @Test
  def testHashCode(): Unit = {
    testSqlApi("hash_code('abc')", "96354")
    testSqlApi("hash_code(f35)", "97")
    testSqlApi("hash_code(f40)", "null")
    testSqlApi("hash_code(CAST(null as VARCHAR))", "null")
  }

  @Test
  def testMD5(): Unit = {
    testSqlApi("md5('abc')", "900150983cd24fb0d6963f7d28e17f72")
    testSqlApi("md5('')", "d41d8cd98f00b204e9800998ecf8427e")
    testSqlApi("md5(f35)", "0cc175b9c0f1b6a831c399e269772661")
    testSqlApi("md5(f40)", "null")
    testSqlApi("md5(CAST(null as VARCHAR))", "null")
  }

  @Test
  def testRegexp(): Unit = {
    testSqlApi("regexp('100-200', '(\\d+)')", "true")
    testSqlApi("regexp('abc-def', '(\\d+)')", "false")
    testSqlApi("regexp(f35, 'a')", "true")
    testSqlApi("regexp(f40, '(\\d+)')", "null")
    testSqlApi("regexp(CAST(null as VARCHAR), '(\\d+)')", "null")
    testSqlApi("regexp('100-200', CAST(null as VARCHAR))", "null")
  }

  @Test
  def testJsonValue(): Unit = {
    testSqlApi("jsonValue('[10, 20, [30, 40]]', '$[2][*]')", "[30,40]")
    testSqlApi("jsonValue('[10, 20, [30, [40, 50, 60]]]', '$[2][*][1][*]')", "[30,[40,50,60]]")
    testSqlApi("jsonValue(f40, '$[2][*][1][*]')", "null")
    testSqlApi("jsonValue('[10, 20, [30, [40, 50, 60]]]', '')", "null")
    testSqlApi("jsonValue('', '$[2][*][1][*]')", "null")
    testSqlApi("jsonValue(CAST(null as VARCHAR), '$[2][*][1][*]')", "null")
    testSqlApi("jsonValue('[10, 20, [30, [40, 50, 60]]]', CAST(null as VARCHAR))", "null")
  }

  @Test
  def testBin(): Unit = {
    testSqlApi(
      "BIN((CAST(NULL AS TINYINT)))",
      "null")

    testSqlApi(
      "BIN(f2)",
      "101010")

    testSqlApi(
      "BIN(f3)",
      "101011")

    testSqlApi(
      "BIN(f4)",
      "101100")

    testSqlApi(
      "BIN(f7)",
      "11")

    testSqlApi(
      "BIN(12)",
      "1100")

    testSqlApi(
      "BIN(10)",
      "1010")

    testSqlApi(
      "BIN(0)",
      "0")

    testSqlApi(
      "BIN(-7)",
      "1111111111111111111111111111111111111111111111111111111111111001")

    testSqlApi(
      "BIN(-1)",
      "1111111111111111111111111111111111111111111111111111111111111111")

    testSqlApi(
      "BIN(f32)",
      "1111111111111111111111111111111111111111111111111111111111111111")
  }

  // ----------------------------------------------------------------------------------------------
  // Math functions
  // ----------------------------------------------------------------------------------------------
  @Test
  def testAdd(): Unit = {

    testSqlApi(
      "1514356320000 + 6000",
      "1514356326000")

    testSqlApi(
      "f34 + 6",
      "1514356320006")

    testSqlApi(
      "f34 + f34",
      "3028712640000")
  }

  @Test
  def testSubtract(): Unit = {

    testSqlApi(
      "1514356320000 - 6000",
      "1514356314000")

    testSqlApi(
      "f34 - 6",
      "1514356319994")

    testSqlApi(
      "f34 - f34",
      "0")
  }

  @Test
  def testMultiply(): Unit = {

    testSqlApi(
      "1514356320000 * 60000",
      "90861379200000000")

    testSqlApi(
      "f34 * 6",
      "9086137920000")


    testSqlApi(
      "f34 * f34",
      "2293275063923942400000000")

  }

  @Test
  def testDivide(): Unit = {

    testSqlApi(
      "1514356320000 / 60000",
      "2.5239272E7")

    // DIV return decimal
    testSqlApi(
      "DIV(1514356320000, 60000)",
      "25239272")

    testSqlApi(
      "f7 / 2",
      "1.5")

    // f34 => Decimal(19,0)
    // 6 => Integer => Decimal(10,0)
    // Decimal(19,0) / Decimal(10,0) => Decimal(30,11)
    testSqlApi(
      "f34 / 6",
      "252392720000.00000000000")

    // Decimal(19,0) / Decimal(19,0) => Decimal(39,20) => Decimal(38,19)
    testSqlApi(
      "f34 / f34",
      "1.0000000000000000000")
  }

  @Test
  def testMod(): Unit = {

    testSqlApi(
      "mod(1514356320000,60000)",
      "0")

    testSqlApi(
      "mod(f34,f34)",
      "0")

    testSqlApi(
      "mod(f34,6)",
      "0")

    testSqlApi(
      "MOD(f4, f7)",
      "2")

    testSqlApi(
      "MOD(f4, 3)",
      "2")

    testSqlApi(
      "MOD(44, 3)",
      "2")
  }

  @Test
  def testExp(): Unit = {
    testSqlApi(
      "EXP(f2)",
      math.exp(42.toByte).toString)

    testSqlApi(
      "EXP(f3)",
      math.exp(43.toShort).toString)

    testSqlApi(
      "EXP(f4)",
      math.exp(44.toLong).toString)

    testSqlApi(
      "EXP(f5)",
      math.exp(4.5.toFloat).toString)

    testSqlApi(
      "EXP(f6)",
      math.exp(4.6).toString)

    testSqlApi(
      "EXP(f7)",
      math.exp(3).toString)

    testSqlApi(
      "EXP(3)",
      math.exp(3).toString)
  }

  @Test
  def testLog10(): Unit = {
    testSqlApi(
      "LOG10(f2)",
      math.log10(42.toByte).toString)

    testSqlApi(
      "LOG10(f3)",
      math.log10(43.toShort).toString)

    testSqlApi(
      "LOG10(f4)",
      math.log10(44.toLong).toString)

    testSqlApi(
      "LOG10(f5)",
      math.log10(4.5.toFloat).toString)

    testSqlApi(
      "LOG10(f6)",
      math.log10(4.6).toString)
  }

  @Test
  def testPower(): Unit = {
    // f7: int , f4: long, f6: double
    testSqlApi(
      "POWER(f2, f7)",
      math.pow(42.toByte, 3).toString)

    testSqlApi(
      "POWER(f3, f6)",
      math.pow(43.toShort, 4.6D).toString)

    testSqlApi(
      "POWER(f4, f5)",
      math.pow(44.toLong, 4.5.toFloat).toString)

    testSqlApi(
      "POWER(f4, f5)",
      math.pow(44.toLong, 4.5.toFloat).toString)

    // f5: float
    testSqlApi(
      "power(f5, f5)",
      math.pow(4.5F, 4.5F).toString)

    testSqlApi(
      "power(f5, f6)",
      math.pow(4.5F, 4.6D).toString)

    testSqlApi(
      "power(f5, f7)",
      math.pow(4.5F, 3).toString)

    testSqlApi(
      "power(f5, f4)",
      math.pow(4.5F, 44L).toString)

    // f22: bigDecimal
    // TODO delete casting in SQL when CALCITE-1467 is fixed
    testSqlApi(
      "power(CAST(f22 AS DOUBLE), f5)",
      math.pow(2, 4.5F).toString)

    testSqlApi(
      "power(CAST(f22 AS DOUBLE), f6)",
      math.pow(2, 4.6D).toString)

    testSqlApi(
      "power(CAST(f22 AS DOUBLE), f7)",
      math.pow(2, 3).toString)

    testSqlApi(
      "power(CAST(f22 AS DOUBLE), f4)",
      math.pow(2, 44L).toString)

    testSqlApi(
      "power(f6, f22)",
      math.pow(4.6D, 2).toString)
  }

  @Test
  def testSqrt(): Unit = {
    testSqlApi(
      "SQRT(f6)",
      math.sqrt(4.6D).toString)

    testSqlApi(
      "SQRT(f7)",
      math.sqrt(3).toString)

    testSqlApi(
      "SQRT(f4)",
      math.sqrt(44L).toString)

    testSqlApi(
      "SQRT(CAST(f22 AS DOUBLE))",
      math.sqrt(2.0).toString)

    testSqlApi(
      "SQRT(f5)",
      math.pow(4.5F, 0.5).toString)

    testSqlApi(
      "SQRT(25)",
      "5.0")

    testSqlApi(
      // TODO fix FLINK-4621
      "POWER(CAST(2.2 AS DOUBLE), CAST(0.5 AS DOUBLE))",
      math.sqrt(2.2).toString)
  }

  @Test
  def testLn(): Unit = {
    testSqlApi(
      "LN(f2)",
      math.log(42.toByte).toString)

    testSqlApi(
      "LN(f3)",
      math.log(43.toShort).toString)

    testSqlApi(
      "LN(f4)",
      math.log(44.toLong).toString)

    testSqlApi(
      "LN(f5)",
      math.log(4.5.toFloat).toString)

    testSqlApi(
      "LN(f6)",
      math.log(4.6).toString)
  }

  @Test
  def testAbs(): Unit = {
    testSqlApi(
      "ABS(f2)",
      "42")

    testSqlApi(
      "ABS(f3)",
      "43")

    testSqlApi(
      "ABS(f4)",
      "44")

    testSqlApi(
      "ABS(f5)",
      "4.5")

    testSqlApi(
      "ABS(f6)",
      "4.6")

    testSqlApi(
      "ABS(f9)",
      "42")

    testSqlApi(
      "ABS(f10)",
      "43")

    testSqlApi(
      "ABS(f11)",
      "44")

    testSqlApi(
      "ABS(f12)",
      "4.5")

    testSqlApi(
      "ABS(f13)",
      "4.6")

    testSqlApi(
      "ABS(f15)",
      "1231.1231231321321321111")
  }

  @Test
  def testArithmeticFloorCeil(): Unit = {
    testSqlApi(
      "FLOOR(f5)",
      "4.0")

    testSqlApi(
      "CEIL(f5)",
      "5.0")

    testSqlApi(
      "FLOOR(f3)",
      "43")

    testSqlApi(
      "CEIL(f3)",
      "43")

    testSqlApi(
      "FLOOR(f15)",
      "-1232")

    testSqlApi(
      "CEIL(f15)",
      "-1231")
  }

  @Test
  def testSin(): Unit = {
    testSqlApi(
      "SIN(f2)",
      math.sin(42.toByte).toString)

    testSqlApi(
      "SIN(f3)",
      math.sin(43.toShort).toString)

    testSqlApi(
      "SIN(f4)",
      math.sin(44.toLong).toString)

    testSqlApi(
      "SIN(f5)",
      math.sin(4.5.toFloat).toString)

    testSqlApi(
      "SIN(f6)",
      math.sin(4.6).toString)

    testSqlApi(
      "SIN(f15)",
      math.sin(-1231.1231231321321321111).toString)
  }

  @Test
  def testCos(): Unit = {
    testSqlApi(
      "COS(f2)",
      math.cos(42.toByte).toString)

    testSqlApi(
      "COS(f3)",
      math.cos(43.toShort).toString)

    testSqlApi(
      "COS(f4)",
      math.cos(44.toLong).toString)

    testSqlApi(
      "COS(f5)",
      math.cos(4.5.toFloat).toString)

    testSqlApi(
      "COS(f6)",
      math.cos(4.6).toString)

    testSqlApi(
      "COS(f15)",
      math.cos(-1231.1231231321321321111).toString)
  }

  @Test
  def testTan(): Unit = {
    testSqlApi(
      "TAN(f2)",
      math.tan(42.toByte).toString)

    testSqlApi(
      "TAN(f3)",
      math.tan(43.toShort).toString)

    testSqlApi(
      "TAN(f4)",
      math.tan(44.toLong).toString)

    testSqlApi(
      "TAN(f5)",
      math.tan(4.5.toFloat).toString)

    testSqlApi(
      "TAN(f6)",
      math.tan(4.6).toString)

    testSqlApi(
      "TAN(f15)",
      math.tan(-1231.1231231321321321111).toString)
  }

  @Test
  def testCot(): Unit = {
    testSqlApi(
      "COT(f2)",
      (1.0d / math.tan(42.toByte)).toString)

    testSqlApi(
      "COT(f3)",
      (1.0d / math.tan(43.toShort)).toString)

    testSqlApi(
      "COT(f4)",
      (1.0d / math.tan(44.toLong)).toString)

    testSqlApi(
      "COT(f5)",
      (1.0d / math.tan(4.5.toFloat)).toString)

    testSqlApi(
      "COT(f6)",
      (1.0d / math.tan(4.6)).toString)

    testSqlApi(
      "COT(f15)",
      (1.0d / math.tan(-1231.1231231321321321111)).toString)
  }

  @Test
  def testAsin(): Unit = {
    testSqlApi(
      "ASIN(f25)",
      math.asin(0.42.toByte).toString)

    testSqlApi(
      "ASIN(f26)",
      math.asin(0.toShort).toString)

    testSqlApi(
      "ASIN(f27)",
      math.asin(0.toLong).toString)

    testSqlApi(
      "ASIN(f28)",
      math.asin(0.45.toFloat).toString)

    testSqlApi(
      "ASIN(f29)",
      math.asin(0.46).toString)

    testSqlApi(
      "ASIN(f30)",
      math.asin(1).toString)

    testSqlApi(
      "ASIN(f31)",
      math.asin(-0.1231231321321321111).toString)
  }

  @Test
  def testAcos(): Unit = {
    testSqlApi(
      "ACOS(f25)",
      math.acos(0.42.toByte).toString)

    testSqlApi(
      "ACOS(f26)",
      math.acos(0.toShort).toString)

    testSqlApi(
      "ACOS(f27)",
      math.acos(0.toLong).toString)

    testSqlApi(
      "ACOS(f28)",
      math.acos(0.45.toFloat).toString)

    testSqlApi(
      "ACOS(f29)",
      math.acos(0.46).toString)

    testSqlApi(
      "ACOS(f30)",
      math.acos(1).toString)

    testSqlApi(
      "ACOS(f31)",
      math.acos(-0.1231231321321321111).toString)
  }

  @Test
  def testAtan(): Unit = {
    testSqlApi(
      "ATAN(f25)",
      math.atan(0.42.toByte).toString)

    testSqlApi(
      "ATAN(f26)",
      math.atan(0.toShort).toString)

    testSqlApi(
      "ATAN(f27)",
      math.atan(0.toLong).toString)

    testSqlApi(
      "ATAN(f28)",
      math.atan(0.45.toFloat).toString)

    testSqlApi(
      "ATAN(f29)",
      math.atan(0.46).toString)

    testSqlApi(
      "ATAN(f30)",
      math.atan(1).toString)

    testSqlApi(
      "ATAN(f31)",
      math.atan(-0.1231231321321321111).toString)
  }

  @Test
  def testDegrees(): Unit = {
    testSqlApi(
      "DEGREES(f2)",
      math.toDegrees(42.toByte).toString)

    testSqlApi(
      "DEGREES(f3)",
      math.toDegrees(43.toShort).toString)

    testSqlApi(
      "DEGREES(f4)",
      math.toDegrees(44.toLong).toString)

    testSqlApi(
      "DEGREES(f5)",
      math.toDegrees(4.5.toFloat).toString)

    testSqlApi(
      "DEGREES(f6)",
      math.toDegrees(4.6).toString)

    testSqlApi(
      "DEGREES(f15)",
      math.toDegrees(-1231.1231231321321321111).toString)
  }

  @Test
  def testRadians(): Unit = {
    testSqlApi(
      "RADIANS(f2)",
      math.toRadians(42.toByte).toString)

    testSqlApi(
      "RADIANS(f3)",
      math.toRadians(43.toShort).toString)

    testSqlApi(
      "RADIANS(f4)",
      math.toRadians(44.toLong).toString)

    testSqlApi(
      "RADIANS(f5)",
      math.toRadians(4.5.toFloat).toString)

    testSqlApi(
      "RADIANS(f6)",
      math.toRadians(4.6).toString)

    testSqlApi(
      "RADIANS(f15)",
      math.toRadians(-1231.1231231321321321111).toString)
  }

  @Test
  def testSign(): Unit = {
    testSqlApi(
      "SIGN(f4)",
      1.toString)

    testSqlApi(
      "SIGN(f6)",
      1.0.toString)

    testSqlApi(
      "SIGN(f15)",
      "-1.0000000000000000000") // calcite: SIGN(Decimal(p,s)) => Decimal(p,s)
  }

  @Test
  def testRound(): Unit = {
    testSqlApi(
      "ROUND(f29, f30)",
      0.5.toString)

    testSqlApi(
      "ROUND(f31, f7)",
      "-0.123")

    testSqlApi(
      "ROUND(f4, f32)",
      40.toString)

    testSqlApi(
      "ROUND(125.315)",
      "125")

    testSqlApi(
      "ROUND(-125.315, 2)",
      "-125.32")

    testSqlApi(
      "ROUND(125.315, 0)",
      "125")

    testSqlApi(
      "ROUND(1.4, 1)",
      "1.4")
  }

  @Test
  def testPi(): Unit = {
    // PI function
    testSqlApi(
      "PI()",
      math.Pi.toString)

    // PI operator
    testSqlApi(
      "PI",
      math.Pi.toString)
  }

  @Test
  def testRandAndRandInteger(): Unit = {
    val random1 = new java.util.Random(1)
    testSqlApi(
      "RAND(1)",
      random1.nextDouble().toString)

    val random2 = new java.util.Random(3)
    testSqlApi(
      "RAND(f7)",
      random2.nextDouble().toString)

    val random3 = new java.util.Random(1)
    testSqlApi(
      "RAND_INTEGER(1, 10)",
      random3.nextInt(10).toString)

    val random4 = new java.util.Random(3)
    testSqlApi(
      "RAND_INTEGER(f7, CAST(f4 AS INT))",
      random4.nextInt(44).toString)
  }

  @Test
  def testE(): Unit = {
    testSqlApi(
      "E()",
      math.E.toString)

    testSqlApi(
      "e()",
      math.E.toString)
  }

  @Test
  def testLog(): Unit = {
    testSqlApi(
      "LOG(f6)",
      "1.5260563034950492"
    )

    testSqlApi(
      "LOG(f6 - f6 + 10, f6 - f6 + 100)",
      "2.0"
    )

    testSqlApi(
      "LOG(f6+20)",
      "3.202746442938317"
    )

    testSqlApi(
      "LOG(10)",
      "2.302585092994046"
    )

    testSqlApi(
      "LOG(10, 100)",
      "2.0"
    )

    testSqlApi(
      "LOG(cast (null AS DOUBLE))",
      "null"
    )

    testSqlApi(
      "LOG(cast (null AS DOUBLE), 1)",
      "null"
    )

    testSqlApi(
      "LOG(1, cast (null AS DOUBLE))",
      "null"
    )
    testSqlApi(
      "LOG(cast (null AS DOUBLE), cast (null AS DOUBLE))",
      "null"
    )
  }

  @Test
  def testLog2(): Unit = {
    testSqlApi(
      "LOG2(f6)",
      "2.2016338611696504"
    )

    testSqlApi(
      "LOG2(f6+20)",
      "4.620586410451877"
    )

    testSqlApi(
      "LOG2(10)",
      "3.3219280948873626"
    )

    testSqlApi(
      "LOG2(cast (null AS DOUBLE))",
      "null"
    )
  }

  @Test
  def testChr(): Unit = {
    testSqlApi(
      "CHR(f4)",
      ","
    )

    testSqlApi(
      "CHR(f43)",
      ""
    )

    testSqlApi(
      "CHR(f42)",
      Character.MIN_VALUE.toString
    )

    testSqlApi(
      "CHR(65)",
      "A"
    )

    testSqlApi(
      "CHR(CAST (-10 AS BIGINT))",
      ""
    )

    testSqlApi(
      "CHR(300)",
      ","
    )

    testSqlApi(
      "CHR(97)",
      "a"
    )

    testSqlApi(
      "CHR(97 + 256)",
      "a"
    )

    testSqlApi(
      "CHR(-9)",
      ""
    )

    testSqlApi(
      "CHR(0)",
      Character.MIN_VALUE.toString
    )

    testSqlApi(
      "CHR(149)",
      149.toChar.toString
    )

    testSqlApi(
      "CHR(cast (null AS BIGINT))",
      "null"
    )
  }

  // ----------------------------------------------------------------------------------------------
  // Temporal functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testExtract(): Unit = {
    testSqlApi(
      "EXTRACT(YEAR FROM f16)",
      "1996")

    testSqlApi(
      "EXTRACT(MONTH FROM f16)",
      "11")

    testSqlApi(
      "EXTRACT(DAY FROM f16)",
      "10")

    testSqlApi(
      "EXTRACT(YEAR FROM f18)",
      "1996")

    testSqlApi(
      "EXTRACT(MONTH FROM f18)",
      "11")

    testSqlApi(
      "EXTRACT(DAY FROM f18)",
      "10")

    testSqlApi(
      "EXTRACT(HOUR FROM f18)",
      "6")

    testSqlApi(
      "EXTRACT(HOUR FROM f17)",
      "6")

    testSqlApi(
      "EXTRACT(MINUTE FROM f18)",
      "55")

    testSqlApi(
      "EXTRACT(MINUTE FROM f17)",
      "55")

    testSqlApi(
      "EXTRACT(SECOND FROM f18)",
      "44")

    testSqlApi(
      "EXTRACT(SECOND FROM f17)",
      "44")

    testSqlApi(
      "EXTRACT(DAY FROM f19)",
      "16979")

    testSqlApi(
      "EXTRACT(HOUR FROM f19)",
      "7")

    testSqlApi(
      "EXTRACT(MINUTE FROM f19)",
      "23")

    testSqlApi(
      "EXTRACT(SECOND FROM f19)",
      "33")

    testSqlApi(
      "EXTRACT(MONTH FROM f20)",
      "1")

    testSqlApi(
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
      "EXTRACT(QUARTER FROM f18)",
      "4")

    testSqlApi(
      "EXTRACT(QUARTER FROM f16)",
      "4")

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
    testSqlApi(
      "FLOOR(f18 TO YEAR)",
      "1996-01-01 00:00:00.000")

    testSqlApi(
      "FLOOR(f18 TO MONTH)",
      "1996-11-01 00:00:00.000")

    testSqlApi(
      "FLOOR(f18 TO DAY)",
      "1996-11-10 00:00:00.000")

    testSqlApi(
      "FLOOR(f18 TO MINUTE)",
      "1996-11-10 06:55:00.000")

    testSqlApi(
      "FLOOR(f18 TO SECOND)",
      "1996-11-10 06:55:44.000")

    testSqlApi(
      "FLOOR(f17 TO HOUR)",
      "06:00:00")

    testSqlApi(
      "FLOOR(f17 TO MINUTE)",
      "06:55:00")

    testSqlApi(
      "FLOOR(f17 TO SECOND)",
      "06:55:44")

    testSqlApi(
      "FLOOR(f16 TO YEAR)",
      "1996-01-01")

    testSqlApi(
      "FLOOR(f16 TO MONTH)",
      "1996-11-01")

    testSqlApi(
      "CEIL(f18 TO YEAR)",
      "1997-01-01 00:00:00.000")

    testSqlApi(
      "CEIL(f18 TO MONTH)",
      "1996-12-01 00:00:00.000")

    testSqlApi(
      "CEIL(f18 TO DAY)",
      "1996-11-11 00:00:00.000")

    testSqlApi(
      "CEIL(f18 TO MINUTE)",
      "1996-11-10 06:56:00.000")

    testSqlApi(
      "CEIL(f18 TO SECOND)",
      "1996-11-10 06:55:45.000")

    testSqlApi(
      "CEIL(f17 TO HOUR)",
      "07:00:00")

    testSqlApi(
      "CEIL(f17 TO MINUTE)",
      "06:56:00")

    testSqlApi(
      "CEIL(f17 TO SECOND)",
      "06:55:44")

    testSqlApi(
      "CEIL(f16 TO YEAR)",
      "1997-01-01")

    testSqlApi(
      "CEIL(f16 TO MONTH)",
      "1996-12-01")
  }

  @Test
  def testCurrentTimePoint(): Unit = {

    // current time points are non-deterministic
    // we just test the format of the output
    // manual test can be found in NonDeterministicTests

    testSqlApi(
      "CHAR_LENGTH(CAST(CURRENT_DATE AS VARCHAR)) >= 5",
      "true")

    testSqlApi(
      "CHAR_LENGTH(CAST(CURRENT_TIME AS VARCHAR)) >= 5",
      "true")

    testSqlApi(
      "CHAR_LENGTH(CAST(CURRENT_TIMESTAMP AS VARCHAR)) >= 12",
      "true")

    testSqlApi(
      "CHAR_LENGTH(CAST(LOCALTIMESTAMP AS VARCHAR)) >= 12",
      "true")

    testSqlApi(
      "CHAR_LENGTH(CAST(LOCALTIME AS VARCHAR)) >= 5",
      "true")

    // comparisons are deterministic
    testSqlApi(
      "LOCALTIMESTAMP = LOCALTIMESTAMP",
      "true")
  }

  @Test
  def testOverlaps(): Unit = {
    testSqlApi(
      "(TIME '2:55:00', INTERVAL '1' HOUR) OVERLAPS (TIME '3:30:00', INTERVAL '2' HOUR)",
      "true")

    testSqlApi(
      "(TIME '9:00:00', TIME '9:30:00') OVERLAPS (TIME '9:29:00', TIME '9:31:00')",
      "true")

    testSqlApi(
      "(TIME '9:00:00', TIME '10:00:00') OVERLAPS (TIME '10:15:00', INTERVAL '3' HOUR)",
      "false")

    testSqlApi(
      "(DATE '2011-03-10', INTERVAL '10' DAY) OVERLAPS (DATE '2011-03-19', INTERVAL '10' DAY)",
      "true")

    testSqlApi(
      "(TIMESTAMP '2011-03-10 05:02:02', INTERVAL '0' SECOND) OVERLAPS " +
        "(TIMESTAMP '2011-03-10 05:02:02', TIMESTAMP '2011-03-10 05:02:01')",
      "true")

    testSqlApi(
      "(TIMESTAMP '2011-03-10 02:02:02.001', INTERVAL '0' SECOND) OVERLAPS " +
        "(TIMESTAMP '2011-03-10 02:02:02.002', TIMESTAMP '2011-03-10 02:02:02.002')",
      "false")
  }

  @Test
  def testQuarter(): Unit = {
    testSqlApi(
      "QUARTER(DATE '1997-01-27')",
      "1")

    testSqlApi(
      "QUARTER(DATE '1997-04-27')",
      "2")

    testSqlApi(
      "QUARTER(DATE '1997-12-31')",
      "4")
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
            testSqlApi(
              s"TIMESTAMPDIFF(${unitParts._1}, TIMESTAMP '${data._1}', TIMESTAMP '${data._2}')",
              data._3
            )
            testSqlApi(  // sql tsi
              s"TIMESTAMPDIFF(${unitParts._3}, TIMESTAMP '${data._1}', TIMESTAMP '${data._2}')",
              data._3
            )
          case 1 => // date, timestamp
            testSqlApi(
              s"TIMESTAMPDIFF(${unitParts._1}, DATE '${data._1}', TIMESTAMP '${data._2}')",
              data._3
            )
            testSqlApi( // sql tsi
              s"TIMESTAMPDIFF(${unitParts._3}, DATE '${data._1}', TIMESTAMP '${data._2}')",
              data._3
            )
          case 2 => // timestamp, date
            testSqlApi(
              s"TIMESTAMPDIFF(${unitParts._1}, TIMESTAMP '${data._1}', DATE '${data._2}')",
              data._3
            )
            testSqlApi( // sql tsi
              s"TIMESTAMPDIFF(${unitParts._3}, TIMESTAMP '${data._1}', DATE '${data._2}')",
              data._3
            )
          case 3 => // date, date
            testSqlApi(
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

    testSqlApi(
      "TIMESTAMPDIFF(DAY, CAST(NULL AS TIMESTAMP), TIMESTAMP '2016-02-24 12:42:25')",
      "null"
    )

    testSqlApi(
      "TIMESTAMPDIFF(DAY, TIMESTAMP '2016-02-24 12:42:25',  CAST(NULL AS TIMESTAMP))",
      "null"
    )
  }

  @Test
  def testTimestampAdd(): Unit = {

    val data = Seq(
      (1, "TIMESTAMP '2017-11-29 22:58:58.998'"),
      (3, "TIMESTAMP '2017-11-29 22:58:58.998'"),
      (-1, "TIMESTAMP '2017-11-29 22:58:58.998'"),
      (-61, "TIMESTAMP '2017-11-29 22:58:58.998'"),
      (-1000, "TIMESTAMP '2017-11-29 22:58:58.998'")
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

    for ((interval, result) <- intervalMapResults) {
      testSqlApi(
        s"TIMESTAMPADD($interval, ${data.head._1}, ${data.head._2})", result.head)
      testSqlApi(
        s"TIMESTAMPADD($interval, ${data(1)._1}, ${data(1)._2})", result(1))
      testSqlApi(
        s"TIMESTAMPADD($interval, ${data(2)._1}, ${data(2)._2})", result(2))
      testSqlApi(
        s"TIMESTAMPADD($interval, ${data(3)._1}, ${data(3)._2})", result(3))
      testSqlApi(
        s"TIMESTAMPADD($interval, ${data(4)._1}, ${data(4)._2})", result(4))
    }

    testSqlApi("TIMESTAMPADD(HOUR, CAST(NULL AS INTEGER), TIMESTAMP '2016-02-24 12:42:25')", "null")

    testSqlApi("TIMESTAMPADD(HOUR, -200, CAST(NULL AS TIMESTAMP))", "null")

    testSqlApi("TIMESTAMPADD(DAY, 1, DATE '2016-06-15')", "2016-06-16")

    testSqlApi("TIMESTAMPADD(MONTH, 3, CAST(NULL AS TIMESTAMP))", "null")

  }

  @Test
  def testToTimestamp(): Unit = {
    testSqlApi("to_timestamp(1513135677000)", "2017-12-13 03:27:57.000")
    testSqlApi("to_timestamp('2017-09-15 00:00:00')", "2017-09-15 00:00:00.000")
    testSqlApi("to_timestamp('20170915000000', 'yyyyMMddHHmmss')", "2017-09-15 00:00:00.000")
  }

  @Test
  def testToDate(): Unit = {
    testSqlApi("to_date('2017-09-15 00:00:00')", "2017-09-15")
  }

  @Test
  def testDateSub(): Unit = {
    testSqlApi("date_sub(f18, 10)", "1996-10-31")
    testSqlApi("date_sub(f18, -10)", "1996-11-20")
    testSqlApi("date_sub(TIMESTAMP '2017-10-15 23:00:00', 30)", "2017-09-15")
    testSqlApi("date_sub(f40, 30)", "null")
    testSqlApi("date_sub(CAST(NULL AS TIMESTAMP), 30)", "null")
    testSqlApi("date_sub(CAST(NULL AS VARCHAR), 30)", "null")
    testSqlApi("date_sub('2017-10--11', 30)", "null")
    testSqlApi("date_sub('2017--10-11', 30)", "null")
  }

  @Test
  def testDateAdd(): Unit = {
    testSqlApi("date_add(f18, 10)", "1996-11-20")
    testSqlApi("date_add(f18, -10)", "1996-10-31")
    testSqlApi("date_add(TIMESTAMP '2017-10-15 23:00:00', 30)", "2017-11-14")
    testSqlApi("date_add(f40, 30)", "null")
    testSqlApi("date_add(CAST(NULL AS TIMESTAMP), 30)", "null")
    testSqlApi("date_add(CAST(NULL AS VARCHAR), 30)", "null")
    testSqlApi("date_add('2017-10--11', 30)", "null")
    testSqlApi("date_add('2017--10-11', 30)", "null")
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

    testSqlApi(
      "MD5('test')",
      expectedMd5)

    testSqlApi(
      "SHA1('test')",
      expectedSha1)

    // sha224
    testSqlApi(
      "SHA224('test')",
      expectedSha224)

    // sha-2 224
    testSqlApi(
      "SHA2('test', 224)",
      expectedSha224)

    // sha256
    testSqlApi(
      "SHA256('test')",
      expectedSha256)

    // sha-2 256
    testSqlApi(
      "SHA2('test', 256)",
      expectedSha256)

    // sha384
    testSqlApi(
      "SHA384('test')",
      expectedSha384)

    // sha-2 384
    testSqlApi(
      "SHA2('test', 384)",
      expectedSha384)

    // sha512
    testSqlApi(
      "SHA512('test')",
      expectedSha512)

    // sha-2 512
    testSqlApi(
      "SHA2('test', 512)",
      expectedSha512)

    // null tests
    testSqlApi(
      "MD5(f33)",
      "null")

    testSqlApi(
      "SHA1(f33)",
      "null")

    testSqlApi(
      "SHA2(f33, 224)",
      "null")

    testSqlApi(
      "SHA2(f33, 224)",
      "null")

    testSqlApi(
      "SHA2(f33, 256)",
      "null")

    testSqlApi(
      "SHA2(f33, 384)",
      "null")

    testSqlApi(
      "SHA2(f33, 512)",
      "null")

    testSqlApi(
      "SHA2('test', CAST(NULL AS INT))",
      "null")

    // non-constant bit length
    testSqlApi(
      "SHA2('test', f44)",
      expectedSha256)
  }

  // ----------------------------------------------------------------------------------------------
  // Other functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testIsTrueIsFalse(): Unit = {
    testSqlApi(
      "f1 IS TRUE",
      "true")

    testSqlApi(
      "f21 IS TRUE",
      "false")

    testSqlApi(
      "FALSE IS FALSE",
      "true")

    testSqlApi(
      "f21 IS FALSE",
      "false")

    testSqlApi(
      "f1 IS NOT TRUE",
      "false")

    testSqlApi(
      "f21 IS NOT TRUE",
      "true")

    testSqlApi(
      "FALSE IS NOT FALSE",
      "false")

    testSqlApi(
      "f21 IS NOT FALSE",
      "true")
  }

  @Test
  def testStringFunctionsWithNull(): Unit = {
    val functions = List(
      ("%s.subString(2)",     "SUBSTRING(%s, 2)"),
      ("%s.trim()",           "TRIM(%s)"),
      ("%s.like('%%link')",   "%s LIKE '%%link'"),
      ("%s.charLength()",     "CHAR_LENGTH(%s)"),
      ("%s.lowerCase()",      "LOWER(%s)"),
      ("%s.upperCase()",      "UPPER(%s)"),
      ("%s.initCap()",        "INITCAP(%s)"),
      ("%s.position('aa')",   "POSITION('aa' IN %s)"),
      ("%s.overlay('aa', 2)", "OVERLAY(%s PLACING 'aa' FROM 2 FOR 2)")
    )

    val field = "f40"
    functions.foreach ( x => {
      val tableApiString = x._1.format(field)
      val sqlApiString = x._2.format(field)
      testSqlApi(
        // TODO: ignore Table API currently
        sqlApiString,
        "null"
      )
    })
  }

  @Test
  def testCodeGenNPE(): Unit = {
    // case 1: non-null field argument, null result,
    // case 2: null field argument, null result
    // case 3: constant argument, null result
    val fields = Seq ("f0", "f40", "''")

    fields.foreach(x => {
      val tableApiString =
        """
          |%s.subString(1, -1)
          |.upperCase()
        """.stripMargin.format(x)
      val sqlApiString = "UPPER(%s)"
        .format("SUBSTRING(%s, 1, -1)")
        .format(x)

      testSqlApi(
        // TODO: ignore Table API currently
        sqlApiString,
        "null"
      )
    })
  }

  @Test
  def testNullBigDecimal(): Unit = {
    testSqlApi(
      "SIGN(f41)",
      "null")
    testSqlApi("SIGN(f41)", "null")
  }

  @Test
  def testEncodeAndDecode(): Unit = {
    testSqlApi(
      "decode(encode('aabbef', 'UTF-16LE'), 'UTF-16LE')",
      "aabbef")

    testSqlApi(
      "decode(encode('aabbef', 'utf-8'), 'utf-8')",
      "aabbef")

    testSqlApi(
      "decode(encode('', 'utf-8'), 'utf-8')",
      "")

    testSqlApi(
      "encode(cast (null AS VARCHAR), 'utf-8')",
      "null"
    )

    testSqlApi(
      "encode(cast (null AS VARCHAR), cast (null AS VARCHAR))",
      "null"
    )

    testSqlApi(
      "encode('aabbef', cast (null AS VARCHAR))",
      "null"
    )

    testSqlApi(
      "decode(cast (null AS BINARY), 'utf-8')",
      "null"
    )

    testSqlApi(
      "decode(cast (null AS BINARY), cast (null AS VARCHAR))",
      "null"
    )

    testSqlApi(
      "decode(encode('aabbef', 'utf-8'), cast (null AS VARCHAR))",
      "null"
    )

    testSqlApi(
      "decode(encode('中国', 'UTF-16LE'), 'UTF-16LE')",
      "中国")

    testSqlApi(
      "decode(encode('val_238', 'US-ASCII'), 'US-ASCII') =" +
          " decode(encode('val_238', 'utf-8'), 'utf-8')",
      "true")
  }


  @Test
  def testStringToMap(): Unit = {
    testSqlApi(
      "STR_TO_MAP('k1=v1,k2=v2')",
      "{k1=v1, k2=v2}")
    testSqlApi(
      "STR_TO_MAP('k1:v1;k2: v2', ';', ':')",
      "{k1=v1, k2= v2}")

    // test empty
    testSqlApi(
      "STR_TO_MAP('')",
      "{}")

    // test key access
    testSqlApi(
      "STR_TO_MAP('k1=v1,k2=v2')['k1']",
      "v1")
    testSqlApi(
      "STR_TO_MAP('k1:v1;k2:v2', ';', ':')['k2']",
      "v2")

    // test non-exist key access
    testSqlApi(
      "STR_TO_MAP('k1=v1,k2=v2')['k3']",
      "null")

    testSqlApi(
      "STR_TO_MAP(f46)",
      "{test1=1, test2=2, test3=3}")

    testSqlApi(
      "STR_TO_MAP(f47)",
      "null")
  }

  @Test
  def testIf(): Unit = {
    // test IF(BOOL, INT, BIGINT), will do implicit type coercion.
    testSqlApi(
      "IF(f7 > 5, f14, f4)",
      "44")
    // test input with null
    testSqlApi(
      "IF(f7 < 5, cast(null as int), f4)",
      "null")

    // f0 is a STRING, cast(f0 as double) should never be ran
    testSqlApi(
      "IF(1 = 1, f6, cast(f0 as double))",
      "4.6")

    // test STRING, STRING
    testSqlApi(
      "IF(f7 > 5, f0, f8)",
      " This is a test String. ")

    // test BYTE, BYTE
    testSqlApi(
      "IF(f7 < 5, f2, f9)",
      "42")

    // test INT, INT
    testSqlApi(
      "IF(f7 < 5, f14, f7)",
      "-3")

    // test SHORT, SHORT
    testSqlApi(
      "IF(f7 < 5, f3, f10)",
      "43")

    // test Long, Long
    testSqlApi(
      "IF(f7 < 5, f4, f11)",
      "44")

    // test Double, Double
    testSqlApi(
      "IF(f7 < 5, f6, f13)",
      "4.6")

    // test BOOL, BOOL
    testSqlApi(
      "IF(f7 < 5, f1, f48)",
      "true")

    // test DECIMAL, DECIMAL
    testSqlApi(
      "IF(f7 < 5, f15, f49)",
      "-1231.1231231321321321111")

    // test BINARY, BINARY
    // the answer BINARY will cast to STRING in ExpressionTestBase.scala
    testSqlApi(
      "IF(f7 < 5, f53, f54)",
      "hello world") // hello world

    // test DATE, DATE
    testSqlApi(
      "IF(f7 < 5, f16, f50)",
      "1996-11-10")

    // test TIME, TIME
    testSqlApi(
      "IF(f7 < 5, f17, f51)",
      "06:55:44")

    // test TIMESTAMP, TIMESTAMP
    testSqlApi(
      "IF(f7 < 5, f18, f52)",
      "1996-11-10 06:55:44.333")
  }

  @Test
  def testToTimestampWithNumeric(): Unit = {
    // Test integral and fractional numeric to timestamp.
    testSqlApi(
      "to_timestamp(f2)",
      "1970-01-01 00:00:00.042")
    testSqlApi(
      "to_timestamp(f3)",
      "1970-01-01 00:00:00.043")
    testSqlApi(
      "to_timestamp(f4)",
      "1970-01-01 00:00:00.044")
    testSqlApi(
      "to_timestamp(f5)",
      "1970-01-01 00:00:00.004")
    testSqlApi(
      "to_timestamp(f6)",
      "1970-01-01 00:00:00.004")
    testSqlApi(
      "to_timestamp(f7)",
      "1970-01-01 00:00:00.003")
    // Test decimal to timestamp.
    testSqlApi(
      "to_timestamp(f15)",
      "1969-12-31 23:59:58.769")
    // test with null input
    testSqlApi(
      "to_timestamp(cast(null as varchar))",
      "null")
  }

  @Test
  def testFromUnixTimeWithNumeric(): Unit = {
    // Test integral and fractional numeric from_unixtime.
    testSqlApi(
      "from_unixtime(f2)",
      "1970-01-01 00:00:42")
    testSqlApi(
      "from_unixtime(f3)",
      "1970-01-01 00:00:43")
    testSqlApi(
      "from_unixtime(f4)",
      "1970-01-01 00:00:44")
    testSqlApi(
      "from_unixtime(f5)",
      "1970-01-01 00:00:04")
    testSqlApi(
      "from_unixtime(f6)",
      "1970-01-01 00:00:04")
    testSqlApi(
      "from_unixtime(f7)",
      "1970-01-01 00:00:03")
    // Test decimal to from_unixtime.
    testSqlApi(
      "from_unixtime(f15)",
      "1969-12-31 23:39:29")
    // test with null input
    testSqlApi(
      "from_unixtime(cast(null as int))",
      "null")
  }

  @Test
  def testIsDecimal(): Unit = {
    testSqlApi(
      "IS_DECIMAL('1')",
      "true")

    testSqlApi(
      "IS_DECIMAL('123')",
      "true")

    testSqlApi(
      "IS_DECIMAL('2')",
      "true")

    testSqlApi(
      "IS_DECIMAL('11.4445')",
      "true")

    testSqlApi(
      "IS_DECIMAL('3')",
      "true")

    testSqlApi(
      "IS_DECIMAL('abc')",
      "false")

    // test null string field
    testSqlApi(
      "IS_DECIMAL(f33)",
      "false")
  }

  @Test
  def testIsDigit(): Unit = {
    testSqlApi(
      "IS_DIGIT('1')",
      "true")

    testSqlApi(
      "IS_DIGIT('123')",
      "true")

    testSqlApi(
      "IS_DIGIT('2')",
      "true")

    testSqlApi(
      "IS_DIGIT('11.4445')",
      "false")

    testSqlApi(
      "IS_DIGIT('3')",
      "true")

    testSqlApi(
      "IS_DIGIT('abc')",
      "false")

    // test null string field
    testSqlApi(
      "IS_DIGIT(f33)",
      "false")
  }


  @Test
  def testIsAlpha(): Unit = {
    testSqlApi(
      "IS_ALPHA('1')",
      "false")

    testSqlApi(
      "IS_ALPHA('123')",
      "false")

    testSqlApi(
      "IS_ALPHA('2')",
      "false")

    testSqlApi(
      "IS_ALPHA('11.4445')",
      "false")

    testSqlApi(
      "IS_ALPHA('3')",
      "false")

    testSqlApi(
      "IS_ALPHA('abc')",
      "true")

    // test null string field
    testSqlApi(
      "IS_ALPHA(f33)",
      "false")
  }

  @Test
  def testBit(): Unit = {
    testSqlApi(
      "BITAND(1, 1)",
      s"${1 & 1}")

    testSqlApi(
      "BITAND(2, 2)",
      s"${2 & 2}")

    testSqlApi(
      "BITAND(-3, -2)",
      "-4")

    testSqlApi(
      "BITOR(1, 2)",
      s"${1 | 2}")

    testSqlApi(
      "BITOR(3, 0)",
      s"${3 | 0}")

    testSqlApi(
      "BITOR(3, 4)",
      "7")

    testSqlApi(
      "BITOR(-3, 2)",
      "-1")

    testSqlApi(
      "BITXOR(1, 1)",
      s"${1 ^ 1}")

    testSqlApi(
      "BITXOR(3, 1)",
      s"${3 ^ 1}")

    testSqlApi(
      "BITXOR(-3, 2)",
      "-1")

    testSqlApi(
      "BITNOT(1)",
      s"${~1}")

    testSqlApi(
      "BITNOT(2)",
      s"${~2}")

    testSqlApi(
      "BITNOT(3)",
      s"${~3}")

    testSqlApi(
      "BITNOT(-3)",
      "2")
  }

}
