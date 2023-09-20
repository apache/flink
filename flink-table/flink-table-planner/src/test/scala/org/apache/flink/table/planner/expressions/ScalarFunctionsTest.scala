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
package org.apache.flink.table.planner.expressions

import org.apache.flink.table.api._
import org.apache.flink.table.expressions.{Expression, TimeIntervalUnit, TimePointUnit}
import org.apache.flink.table.planner.expressions.utils.ScalarTypesTestBase

import org.junit.jupiter.api.Test

class ScalarFunctionsTest extends ScalarTypesTestBase {

  // ----------------------------------------------------------------------------------------------
  // String functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testOverlay(): Unit = {
    // (tableApiString, sqlApiString, expectedResult)
    val cases = Seq(
      // constants, no length argument
      ("xxxxxtest".overlay("xxxx", 6), "OVERLAY('xxxxxtest' PLACING 'xxxx' FROM 6)", "xxxxxxxxx"),

      // constants
      (
        "xxxxxtest".overlay("xxxx", 6, 2),
        "OVERLAY('xxxxxtest' PLACING 'xxxx' FROM 6 FOR 2)",
        "xxxxxxxxxst"),

      // invalid position on the constants
      (
        "123456789".overlay("abc", 100, 2),
        "OVERLAY('123456789' PLACING 'It' FROM -1 FOR 4)",
        "123456789"),

      // invalid position on the constants
      (
        "123456789".overlay("abc", -1, 2),
        "OVERLAY('123456789' PLACING 'It' FROM -1 FOR 2)",
        "123456789"),

      // invalid len on the constants
      (
        "123456789".overlay("abc", 2, 100),
        "OVERLAY('123456789' PLACING 'abc' FROM 2 FOR 100)",
        "1abc"),

      // invalid len on the constants
      (
        "123456789".overlay("abc", 2, -1),
        "OVERLAY('123456789' PLACING 'abc' FROM 2 FOR -1)",
        "1abc"),

      // invalid start & len on the constants
      (
        "123456789".overlay("abc", 100, -1),
        "OVERLAY('123456789' PLACING 'abc' FROM 100 FOR -1)",
        "123456789"),

      // field
      ($"f0".overlay("It", 1, 4), "OVERLAY(f0 PLACING 'It' FROM 1 FOR 4)", "It is a test String."),

      // invalid position
      (
        $"f0".overlay("It", -1, 4),
        "OVERLAY(f0 PLACING 'It' FROM -1 FOR 4)",
        "This is a test String."),

      // invalid position
      (
        $"f0".overlay("It", 100, 4),
        "OVERLAY(f0 PLACING 'It' FROM 100 FOR 4)",
        "This is a test String."),

      // invalid position
      (
        $"f0".overlay("It", -1, 2),
        "OVERLAY(f0 PLACING 'It' FROM -1 FOR 2)",
        "This is a test String."),

      // invalid position
      (
        $"f0".overlay("It", 100, 2),
        "OVERLAY(f0 PLACING 'It' FROM 100 FOR 2)",
        "This is a test String."),

      // invalid length
      ($"f0".overlay("IS", 6, 100), "OVERLAY(f0 PLACING 'IS' FROM 6 FOR 100)", "This IS"),

      // invalid length
      ($"f0".overlay("IS", 6, -1), "OVERLAY(f0 PLACING 'IS' FROM 6 FOR -1)", "This IS"),

      // null field. f40 is NULL.
      ($"f40".overlay("It", 1, 4), "OVERLAY(f40 PLACING 'It' FROM 1 FOR 2)", "NULL")
    )

    cases.foreach(
      x => {
        testAllApis(
          x._1,
          x._2,
          x._3
        )
      })

  }

  @Test
  def testPosition(): Unit = {
    testAllApis("test".position("xxxtest"), "POSITION('test' IN 'xxxtest')", "4")

    testAllApis("testx".position("xxxtest"), "POSITION('testx' IN 'xxxtest')", "0")

    testSqlApi("POSITION('aa' IN 'aaads')", "1")

    testSqlApi("position('aa' in 'aaads')", "1")
  }

  @Test
  def testLocate(): Unit = {
    testSqlApi("locate('test', 'xxxtest')", "4")

    testSqlApi("locate('testx', 'xxxtest')", "0")

    testAllApis("aa".locate("aaads"), "LOCATE('aa', 'aaads')", "1")

    testAllApis("aa".locate("aaads", 2), "LOCATE('aa', 'aaads', 2)", "2")
  }

  @Test
  def testLeft(): Unit = {
    testSqlApi("`LEFT`(f0, 2)", "Th")

    testSqlApi("`LEFT`(f0, 100)", "This is a test String.")

    testSqlApi("`LEFT`(f0, -2)", "")

    testSqlApi("`LEFT`(f0, 0)", "")

    testSqlApi("`LEFT`(f0, CAST(null as Integer))", "NULL")

    testSqlApi("`LEFT`(CAST(null as VARCHAR), -2)", "NULL")

    testSqlApi("`LEFT`(CAST(null as VARCHAR), 2)", "NULL")
  }

  @Test
  def testRight(): Unit = {
    testSqlApi("`right`(f0, 2)", "g.")

    testSqlApi("`right`(f0, 100)", "This is a test String.")

    testSqlApi("`right`(f0, -2)", "")

    testSqlApi("`right`(f0, 0)", "")

    testSqlApi("`right`(f0, CAST(null as Integer))", "NULL")

    testSqlApi("`right`(CAST(null as VARCHAR), -2)", "NULL")

    testSqlApi("`right`(CAST(null as VARCHAR), 2)", "NULL")
  }

  @Test
  def testAscii(): Unit = {
    testAllApis("efg".ascii(), "ASCII('efg')", "101")

    testSqlApi("ascii('abcdef')", "97")

    testSqlApi("ascii('')", "0")

    testSqlApi(
      "ascii(cast (null AS VARCHAR))",
      "NULL"
    )

    testSqlApi(
      "ascii('val_238') = ascii('val_239')",
      "TRUE"
    )
  }

  @Test
  def testLeftAndRight(): Unit = {
    val str = "Hello"
    testAllApis(str.left(3), s"LEFT('$str', 3)", "Hel")
    testAllApis(str.right(3), s"RIGHT('$str', 3)", "llo")
  }

  @Test
  def testInstr(): Unit = {
    testSqlApi("instr('Corporate Floor', 'or', 3, 2)", "14")

    testSqlApi("instr('Corporate Floor', 'or', -3, 2)", "2")

    testSqlApi("instr('Tech on the net', 'e')", "2")

    testSqlApi("instr('Tech on the net', 'e', 1, 2)", "11")

    testSqlApi("instr('Tech on the net', 'e', 1, 3)", "14")

    testSqlApi("instr('Tech on the net', 'e', -3, 2)", "2")

    testAllApis("myteststring".instr("st"), "instr('myteststring', 'st')", "5")

    testSqlApi(
      "instr(cast (null AS VARCHAR), 'e')",
      "NULL"
    )

    testSqlApi(
      "instr('e', cast (null AS VARCHAR))",
      "NULL"
    )

    testSqlApi(
      "instr('val_238', '_') = instr('val_239', '_')",
      "TRUE"
    )

    testSqlApi(
      "instr('val_239', '_')",
      "4"
    )
  }

  @Test
  def testSubstring(): Unit = {
    testAllApis('f0.substring(2), "SUBSTRING(f0, 2)", "his is a test String.")

    testAllApis('f0.substring(2, 5), "SUBSTRING(f0, 2, 5)", "his i")

    testAllApis('f0.substring(1, 'f7), "SUBSTRING(f0, 1, f7)", "Thi")

    testAllApis(
      'f0.substring(1.cast(DataTypes.TINYINT), 'f7),
      "SUBSTRING(f0, CAST(1 AS TINYINT), f7)",
      "Thi")

    testSqlApi("SUBSTRING(f0 FROM 2 FOR 1)", "h")

    testSqlApi("SUBSTRING(f0 FROM 2)", "his is a test String.")

    testSqlApi("SUBSTRING(f0 FROM -2)", "g.")

    testSqlApi("SUBSTRING(f0 FROM -2 FOR 1)", "g")

    testSqlApi("SUBSTRING(f0 FROM -2 FOR 0)", "")
  }

  @Test
  def testReplace(): Unit = {
    testAllApis('f0.replace(" ", "_"), "REPLACE(f0, ' ', '_')", "This_is_a_test_String.")

    testAllApis('f0.replace("i", ""), "REPLACE(f0, 'i', '')", "Ths s a test Strng.")

    testAllApis('f33.replace("i", ""), "REPLACE(f33, 'i', '')", "NULL")

    testAllApis(
      'f0.replace(nullOf(DataTypes.STRING), ""),
      "REPLACE(f0, NULLIF('', ''), '')",
      "NULL")

    testAllApis(
      'f0.replace(" ", nullOf(DataTypes.STRING)),
      "REPLACE(f0, ' ', NULLIF('', ''))",
      "NULL")
  }

  @Test
  def testTrim(): Unit = {
    testAllApis('f8.trim(), "TRIM(f8)", "This is a test String.")

    testAllApis(
      'f8.trim(removeLeading = true, removeTrailing = true, " "),
      "TRIM(f8)",
      "This is a test String.")

    testAllApis(
      'f8.trim(removeLeading = false, removeTrailing = true, " "),
      "TRIM(TRAILING FROM f8)",
      " This is a test String.")

    testAllApis(
      'f0.trim(removeLeading = true, removeTrailing = true, "."),
      "TRIM(BOTH '.' FROM f0)",
      "This is a test String")

    testSqlApi("trim(BOTH 'abc' FROM 'abcddcba')", "dd")

    testSqlApi("trim(BOTH 'abd' FROM 'abcddcba')", "cddc")

    testSqlApi("trim(BOTH '开心' FROM '心情开开心心')", "情")

    testSqlApi("trim(BOTH '开心' FROM '心情开开心心')", "情")

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
    testAllApis('f8.ltrim(), "LTRIM(f8)", "This is a test String. ")

    testSqlApi("LTRIM(f8)", "This is a test String. ")

    testSqlApi("LTRIM(f0, 'This ')", "a test String.")

    testSqlApi("ltrim('abcddcba', 'abc')", "ddcba")

    testSqlApi("LTRIM('abcddcba', 'abd')", "cddcba")

    testSqlApi("ltrim('心情开开心心', '开心')", "情开开心心")

    testSqlApi("LTRIM('abcddcba', CAST(null as VARCHAR))", "NULL")

    testSqlApi("LTRIM(CAST(null as VARCHAR), 'abcddcba')", "NULL")
  }

  @Test
  def testRTrim(): Unit = {
    testAllApis('f8.rtrim(), "rtrim(f8)", " This is a test String.")

    testSqlApi("rtrim(f8)", " This is a test String.")

    testSqlApi("rtrim(f0, 'String. ')", "This is a tes")

    testSqlApi("rtrim('abcddcba', 'abc')", "abcdd")

    testSqlApi("rtrim('abcddcba', 'abd')", "abcddc")

    testSqlApi("rtrim('心情开开心心', '开心')", "心情")

    testSqlApi("rtrim('abcddcba', CAST(null as VARCHAR))", "NULL")

    testSqlApi("rtrim(CAST(null as VARCHAR), 'abcddcba')", "NULL")
  }

  @Test
  def testCharLength(): Unit = {
    testAllApis('f0.charLength(), "CHAR_LENGTH(f0)", "22")

    testAllApis('f0.charLength(), "CHARACTER_LENGTH(f0)", "22")
  }

  @Test
  def testUpperCase(): Unit = {
    testAllApis('f0.upperCase(), "UPPER(f0)", "THIS IS A TEST STRING.")
  }

  @Test
  def testLowerCase(): Unit = {
    testAllApis('f0.lowerCase(), "LOWER(f0)", "this is a test string.")
  }

  @Test
  def testInitCap(): Unit = {
    testAllApis('f0.initCap(), "INITCAP(f0)", "This Is A Test String.")

    testSqlApi("INITCAP('ab')", "Ab")
    testSqlApi("INITCAP('a B')", "A B")
    testSqlApi("INITCAP('fLinK')", "Flink")
  }

  @Test
  def testConcat(): Unit = {
    testAllApis('f0 + 'f0, "f0||f0", "This is a test String.This is a test String.")
  }

  @Test
  def testLike(): Unit = {
    testAllApis('f0.like("Th_s%"), "f0 LIKE 'Th_s%'", "TRUE")

    testAllApis('f0.like("%is a%"), "f0 LIKE '%is a%'", "TRUE")

    testSqlApi("'abcxxxdef' LIKE 'abcx%'", "TRUE")
    testSqlApi("'abcxxxdef' LIKE '%%def'", "TRUE")
    testSqlApi("'abcxxxdef' LIKE 'abcxxxdef'", "TRUE")
    testSqlApi("'abcxxxdef' LIKE '%xdef'", "TRUE")
    testSqlApi("'abcxxxdef' LIKE 'abc%def%'", "TRUE")
    testSqlApi("'abcxxxdef' LIKE '%abc%def'", "TRUE")
    testSqlApi("'abcxxxdef' LIKE '%abc%def%'", "TRUE")
    testSqlApi("'abcxxxdef' LIKE 'abc%def'", "TRUE")

    // false
    testSqlApi("'abcxxxdef' LIKE 'abdxxxdef'", "FALSE")
    testSqlApi("'abcxxxdef' LIKE '%xqef'", "FALSE")
    testSqlApi("'abcxxxdef' LIKE 'abc%qef%'", "FALSE")
    testSqlApi("'abcxxxdef' LIKE '%abc%qef'", "FALSE")
    testSqlApi("'abcxxxdef' LIKE '%abc%qef%'", "FALSE")
    testSqlApi("'abcxxxdef' LIKE 'abc%qef'", "FALSE")
  }

  @Test
  def testNotLike(): Unit = {
    testAllApis(!'f0.like("Th_s%"), "f0 NOT LIKE 'Th_s%'", "FALSE")

    testAllApis(!'f0.like("%is a%"), "f0 NOT LIKE '%is a%'", "FALSE")
  }

  @Test
  def testLikeWithEscape(): Unit = {
    testSqlApi("f23 LIKE '&%Th_s%' ESCAPE '&'", "TRUE")

    testSqlApi("f23 LIKE '&%%is a%' ESCAPE '&'", "TRUE")

    testSqlApi("f0 LIKE 'Th_s%' ESCAPE '&'", "TRUE")

    testSqlApi("f0 LIKE '%is a%' ESCAPE '&'", "TRUE")
  }

  @Test
  def testNotLikeWithEscape(): Unit = {
    testSqlApi("f23 NOT LIKE '&%Th_s%' ESCAPE '&'", "FALSE")

    testSqlApi("f23 NOT LIKE '&%%is a%' ESCAPE '&'", "FALSE")

    testSqlApi("f0 NOT LIKE 'Th_s%' ESCAPE '&'", "FALSE")

    testSqlApi("f0 NOT LIKE '%is a%' ESCAPE '&'", "FALSE")
  }

  @Test
  def testSimilar(): Unit = {
    testAllApis('f0.similar("_*"), "f0 SIMILAR TO '_*'", "TRUE")

    testAllApis(
      'f0.similar("This (is)? a (test)+ Strin_*"),
      "f0 SIMILAR TO 'This (is)? a (test)+ Strin_*'",
      "TRUE")
  }

  @Test
  def testNotSimilar(): Unit = {
    testAllApis(!'f0.similar("_*"), "f0 NOT SIMILAR TO '_*'", "FALSE")

    testAllApis(
      !'f0.similar("This (is)? a (test)+ Strin_*"),
      "f0 NOT SIMILAR TO 'This (is)? a (test)+ Strin_*'",
      "FALSE")
  }

  @Test
  def testSimilarWithEscape(): Unit = {
    testSqlApi("f24 SIMILAR TO '&*&__*' ESCAPE '&'", "TRUE")

    testSqlApi("f0 SIMILAR TO '_*' ESCAPE '&'", "TRUE")

    testSqlApi("f24 SIMILAR TO '&*&_This (is)? a (test)+ Strin_*' ESCAPE '&'", "TRUE")

    testSqlApi("f0 SIMILAR TO 'This (is)? a (test)+ Strin_*' ESCAPE '&'", "TRUE")
  }

  @Test
  def testNotSimilarWithEscape(): Unit = {
    testSqlApi("f24 NOT SIMILAR TO '&*&__*' ESCAPE '&'", "FALSE")

    testSqlApi("f0 NOT SIMILAR TO '_*' ESCAPE '&'", "FALSE")

    testSqlApi("f24 NOT SIMILAR TO '&*&_This (is)? a (test)+ Strin_*' ESCAPE '&'", "FALSE")

    testSqlApi("f0 NOT SIMILAR TO 'This (is)? a (test)+ Strin_*' ESCAPE '&'", "FALSE")
  }

  @Test
  def testMultiConcat(): Unit = {
    testAllApis(concat("xx", 'f33), "CONCAT('xx', f33)", "NULL")
    testAllApis(concat("AA", "BB", "CC", "---"), "CONCAT('AA','BB','CC','---')", "AABBCC---")
    testAllApis(
      concat("x~x", "b~b", "c~~~~c", "---"),
      "CONCAT('x~x','b~b','c~~~~c','---')",
      "x~xb~bc~~~~c---")

    testSqlApi("concat(f35)", "a")
    testSqlApi("concat(f35,f36)", "ab")
    testSqlApi("concat(f35,f36,f33)", "NULL")
  }

  @Test
  def testConcatWs(): Unit = {
    testAllApis(concat_ws('f33, "AA"), "CONCAT_WS(f33, 'AA')", "NULL")
    testAllApis(concat_ws("~~~~", "AA"), "concat_ws('~~~~','AA')", "AA")
    testAllApis(concat_ws("~", "AA", "BB"), "concat_ws('~','AA','BB')", "AA~BB")
    testAllApis(
      concat_ws("~", 'f33, "AA", "BB", "", 'f33, "CC"),
      "concat_ws('~',f33, 'AA','BB','',f33, 'CC')",
      "AA~BB~~CC")
    testAllApis(
      concat_ws("~~~~", "Flink", 'f33, "xx", 'f33, 'f33),
      "CONCAT_WS('~~~~','Flink', f33, 'xx', f33, f33)",
      "Flink~~~~xx")

    testSqlApi("concat_ws('||', f35, f36, f33)", "a||b")
  }

  @Test
  def testRegexpReplace(): Unit = {

    testAllApis(
      "foobar".regexpReplace("oo|ar", "abc"),
      "regexp_replace('foobar', 'oo|ar', 'abc')",
      "fabcbabc")

    testAllApis("foofar".regexpReplace("^f", ""), "regexp_replace('foofar', '^f', '')", "oofar")

    testAllApis(
      "foobar".regexpReplace("^f*.*r$", ""),
      "regexp_replace('foobar', '^f*.*r$', '')",
      "")

    testAllApis(
      "foo1bar2".regexpReplace("\\d", ""),
      "regexp_replace('foobar', '\\d', '')",
      "foobar")

    testAllApis("foobar".regexpReplace("\\w", ""), "regexp_replace('foobar', '\\w', '')", "")

    testAllApis(
      "fooobar".regexpReplace("oo", "$"),
      "regexp_replace('fooobar', 'oo', '$')",
      "f$obar")

    testAllApis(
      "foobar".regexpReplace("oo", "\\"),
      "regexp_replace('foobar', 'oo', '\\')",
      "f\\bar")

    testAllApis('f33.regexpReplace("oo|ar", ""), "REGEXP_REPLACE(f33, 'oo|ar', '')", "NULL")

    testAllApis("foobar".regexpReplace('f33, ""), "REGEXP_REPLACE('foobar', f33, '')", "NULL")

    testAllApis(
      "foobar".regexpReplace("oo|ar", 'f33),
      "REGEXP_REPLACE('foobar', 'oo|ar', f33)",
      "NULL")

    // This test was added for the null literal problem in string expression parsing (FLINK-10463).
    testAllApis(
      nullOf(Types.STRING).regexpReplace("oo|ar", 'f33),
      "REGEXP_REPLACE(CAST(NULL AS VARCHAR), 'oo|ar', f33)",
      "NULL")
  }

  @Test
  def testRegexpExtract(): Unit = {
    testAllApis(
      "foothebar".regexpExtract("foo(.*?)(bar)", 2),
      "REGEXP_EXTRACT('foothebar', 'foo(.*?)(bar)', 2)",
      "bar")

    testAllApis(
      "foothebar".regexpExtract("foo(.*?)(bar)", 0),
      "REGEXP_EXTRACT('foothebar', 'foo(.*?)(bar)', 0)",
      "foothebar")

    testAllApis(
      "foothebar".regexpExtract("foo(.*?)(bar)", 1),
      "REGEXP_EXTRACT('foothebar', 'foo(.*?)(bar)', 1)",
      "the")

    testAllApis(
      "foothebar".regexpExtract("foo([\\w]+)", 1),
      "REGEXP_EXTRACT('foothebar', 'foo([\\w]+)', 1)",
      "thebar")

    testAllApis(
      "foothebar".regexpExtract("foo([\\d]+)", 1),
      "REGEXP_EXTRACT('foothebar', 'foo([\\d]+)', 1)",
      "NULL")

    testAllApis(
      'f33.regexpExtract("foo(.*?)(bar)", 2),
      "REGEXP_EXTRACT(f33, 'foo(.*?)(bar)', 2)",
      "NULL")

    testAllApis("foothebar".regexpExtract('f33, 2), "REGEXP_EXTRACT('foothebar', f33, 2)", "NULL")

    // test for optional group index
    testAllApis(
      "foothebar".regexpExtract("foo(.*?)(bar)"),
      "REGEXP_EXTRACT('foothebar', 'foo(.*?)(bar)')",
      "foothebar")
  }

  @Test
  def testFromBase64(): Unit = {
    testSqlApi("FROM_BASE64('aGVsbG8gd29ybGQ=')", "hello world")

    testSqlApi("CONCAT(FROM_BASE64('5L2g5aW9'), ' flink')", "你好 flink")

    // null test
    testSqlApi("FROM_BASE64(f33)", "NULL")

    testSqlApi(
      "FROM_BASE64('5L2g5aW9')",
      "你好"
    )

    testSqlApi("FROM_BASE64(CAST(x'6147567362473867643239796247513D' AS VARBINARY))", "hello world")

    testSqlApi("FROM_BASE64(x'6147567362473867643239796247513D')", "hello world")

    testSqlApi("FROM_BASE64(f58)", "你好")
  }

  @Test
  def testToBase64(): Unit = {
    testSqlApi("TO_BASE64(f0)", "VGhpcyBpcyBhIHRlc3QgU3RyaW5nLg==")

    testSqlApi("TO_BASE64(f8)", "IFRoaXMgaXMgYSB0ZXN0IFN0cmluZy4g")

    testSqlApi("TO_BASE64('')", "")

    // null test
    testSqlApi("TO_BASE64(f33)", "NULL")

    testSqlApi(
      "TO_BASE64('你好')",
      "5L2g5aW9"
    )

    testSqlApi("to_base64(f37)", "AQIDBA==")
    testSqlApi("to_base64(from_base64(f38))", "AQIDBA==")
  }

  @Test
  def testSubString(): Unit = {
    Array("substring", "substr").foreach {
      substr =>
        testAllApis('f0.substr(2, 3), s"$substr(f0, 2, 3)", "his")
        testAllApis('f0.substr(2), s"$substr(f0, 2)", "his is a test String.")
        testSqlApi(s"$substr(f0, 2, 100)", "his is a test String.")
        testSqlApi(s"$substr(f0, 100, 10)", "")
        testSqlApi(s"$substr(f0, 2, -1)", "NULL")
        testSqlApi(s"$substr(f40, 2, 3)", "NULL")
        testSqlApi(s"$substr(CAST(null AS VARCHAR), 2, 3)", "NULL")
        testSqlApi(s"$substr(f0, 2, f14)", "NULL")
        testSqlApi(s"$substr(f0, f30, f7)", "Thi")
        testSqlApi(s"$substr(f39, 1, 2)", "1世")
    }
  }

  @Test
  def testLPad(): Unit = {
    testSqlApi("lpad(f33,1,'??')", "NULL")
    testSqlApi("lpad(f35, 1, '??')", "a")
    testSqlApi("lpad(f35, 2, '??')", "?a")
    testSqlApi("lpad(f35, 5, '??')", "????a")
    testSqlApi("lpad(f35, CAST(null as INT), '??')", "NULL")
    testSqlApi("lpad(f35, 5, CAST(null as VARCHAR))", "NULL")
    testSqlApi("lpad(f40, 1, '??')", "NULL")
    testSqlApi("lpad('hi', 1, '??')", "h")
    testSqlApi("lpad('hi', 5, '??')", "???hi")
    testSqlApi("lpad(CAST(null as VARCHAR), 5, '??')", "NULL")
    testSqlApi("lpad('hi', CAST(null as INT), '??')", "NULL")
    testSqlApi("lpad('hi', 5, CAST(null as VARCHAR))", "NULL")
    testSqlApi("lpad('',1,'??')", "?")
    testSqlApi("lpad('',30,'??')", "??????????????????????????????")
    testSqlApi("lpad('111',-2,'??')", "NULL")
    testSqlApi("lpad('\u0061\u0062',1,'??')", "a") // the unicode of ab is \u0061\u0062
    testSqlApi("lpad('⎨⎨',1,'??')", "⎨")
    testSqlApi("lpad('äääääääää',2,'??')", "ää")
    testSqlApi("lpad('äääääääää',10,'??')", "?äääääääää")
    testSqlApi("lpad('Hello', -1, 'x') IS NULL", "TRUE")
    testSqlApi("lpad('Hello', -1, 'x') IS NOT NULL", "FALSE")
    testSqlApi("lpad('ab', 5, '')", "NULL")

    testAllApis("äää".lpad(13, "12345"), "lpad('äää',13,'12345')", "1234512345äää")
  }

  @Test
  def testRPad(): Unit = {
    testSqlApi("rpad(f33,1,'??')", "NULL")
    testSqlApi("rpad(f35, 1, '??')", "a")
    testSqlApi("rpad(f35, 2, '??')", "a?")
    testSqlApi("rpad(f35, 5, '??')", "a????")
    testSqlApi("rpad(f35, CAST(null as INT), '??')", "NULL")
    testSqlApi("rpad(f35, 5, CAST(null as VARCHAR))", "NULL")
    testSqlApi("rpad(f40, 1, '??')", "NULL")
    testSqlApi("rpad('hi', 1, '??')", "h")
    testSqlApi("rpad('hi', 5, '??')", "hi???")
    testSqlApi("rpad(CAST(null as VARCHAR), 5, '??')", "NULL")
    testSqlApi("rpad('hi', CAST(null as INT), '??')", "NULL")
    testSqlApi("rpad('hi', 5, CAST(null as VARCHAR))", "NULL")
    testSqlApi("rpad('',1,'??')", "?")
    testSqlApi("rpad('111',-2,'??')", "NULL")
    testSqlApi("rpad('\u0061\u0062',1,'??')", "a") // the unicode of ab is \u0061\u0062
    testSqlApi("rpad('üö',1,'??')", "ü")
    testSqlApi("rpad('abcd', 5, '')", "NULL")
    testAllApis("äää".rpad(13, "12345"), "rpad('äää',13,'12345')", "äää1234512345")
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
        qv: String): Unit = {

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
        testAllApis(url.parseUrl(s"$n"), s"parse_url('$url', '$n')", v)
      }

      testAllApis(url.parseUrl("QUERY", "query"), s"parse_url('$url', 'QUERY', 'query')", qv)
    }

    testUrl(
      "http://userinfo@flink.apache.org/path?query=1#Ref",
      "flink.apache.org",
      "/path",
      "query=1",
      "Ref",
      "http",
      "/path?query=1",
      "userinfo@flink.apache.org",
      "userinfo",
      "1"
    )

    testUrl(
      "https://use%20r:pas%20s@example.com/dir%20/pa%20th.HTML?query=x%20y&q2=2#Ref%20two",
      "example.com",
      "/dir%20/pa%20th.HTML",
      "query=x%20y&q2=2",
      "Ref%20two",
      "https",
      "/dir%20/pa%20th.HTML?query=x%20y&q2=2",
      "use%20r:pas%20s@example.com",
      "use%20r:pas%20s",
      "x%20y"
    )

    testUrl(
      "http://user:pass@host",
      "host",
      "",
      "NULL",
      "NULL",
      "http",
      "",
      "user:pass@host",
      "user:pass",
      "NULL")

    testUrl(
      "http://user:pass@host/",
      "host",
      "/",
      "NULL",
      "NULL",
      "http",
      "/",
      "user:pass@host",
      "user:pass",
      "NULL")

    testUrl(
      "http://user:pass@host/?#",
      "host",
      "/",
      "",
      "",
      "http",
      "/?",
      "user:pass@host",
      "user:pass",
      "NULL")

    testUrl(
      "http://user:pass@host/file;param?query;p2",
      "host",
      "/file;param",
      "query;p2",
      "NULL",
      "http",
      "/file;param?query;p2",
      "user:pass@host",
      "user:pass",
      "NULL")

    testUrl(
      "invalid://user:pass@host/file;param?query;p2",
      "NULL",
      "NULL",
      "NULL",
      "NULL",
      "NULL",
      "NULL",
      "NULL",
      "NULL",
      "NULL")
  }

  @Test
  def testRepeat(): Unit = {
    testAllApis('f0.repeat(1), "REPEAT(f0, 1)", "This is a test String.")

    testAllApis('f0.repeat(2), "REPEAT(f0, 2)", "This is a test String.This is a test String.")

    testAllApis('f0.repeat(0), "REPEAT(f0, 0)", "")

    testAllApis('f0.repeat(-1), "REPEAT(f0, -1)", "")

    testAllApis('f33.repeat(2), "REPEAT(f33, 2)", "NULL")

    testAllApis("".repeat(1), "REPEAT('', 2)", "")
  }

  @Test
  def testReverse(): Unit = {
    testAllApis('f38.reverse(), "reverse(f38)", "==ABDIQA")
    testAllApis('f40.reverse(), "reverse(f40)", "NULL")
    testSqlApi("reverse('hi')", "ih")
    testSqlApi("reverse('hhhi')", "ihhh")
    testSqlApi("reverse(CAST(null as VARCHAR))", "NULL")
  }

  @Test
  def testSplitIndex(): Unit = {
    testAllApis('f38.splitIndex("I", 0), "split_index(f38, 'I', 0)", "AQ")
    testSqlApi("split_index(f38, 'I', 2)", "NULL")
    testSqlApi("split_index(f38, 'I', -1)", "NULL")
    testSqlApi("split_index(f38, CAST(null as VARCHAR), 0)", "NULL")
    testSqlApi("split_index(f38, 'I', CAST(null as INT))", "NULL")
    testSqlApi("split_index(f38, 'I', -1)", "NULL")
    testSqlApi("split_index(f40, 'I', 0)", "NULL")
    testSqlApi("split_index(f38, 73, 0)", "AQ")
    testSqlApi("split_index(f38, 256, 0)", "NULL")
    testSqlApi("split_index(f38, 0, 0)", "NULL")
    testSqlApi("split_index('Test', 'e', 1)", "st")
    testSqlApi("split_index(CAST(null as VARCHAR), 'e', 1)", "NULL")
    testSqlApi("split_index('test', CAST(null as VARCHAR), 1)", "NULL")
    testSqlApi("split_index('test', 'e', -1)", "NULL")
  }

  @Test
  def testHashCode(): Unit = {
    testSqlApi("hash_code('abc')", "96354")
    testSqlApi("hash_code(f35)", "97")
    testSqlApi("hash_code(f40)", "NULL")
    testSqlApi("hash_code(CAST(null as VARCHAR))", "NULL")
  }

  @Test
  def testMD5(): Unit = {
    testSqlApi("md5('abc')", "900150983cd24fb0d6963f7d28e17f72")
    testSqlApi("md5('')", "d41d8cd98f00b204e9800998ecf8427e")
    testSqlApi("md5(f35)", "0cc175b9c0f1b6a831c399e269772661")
    testSqlApi("md5(f40)", "NULL")
    testSqlApi("md5(CAST(null as VARCHAR))", "NULL")
  }

  @Test
  def testRegexp(): Unit = {
    testAllApis("100-200".regexp("(\\d+)"), "regexp('100-200', '(\\d+)')", "TRUE")
    testSqlApi("regexp('abc-def', '(\\d+)')", "FALSE")
    testSqlApi("regexp(f35, 'a')", "TRUE")
    testSqlApi("regexp(f40, '(\\d+)')", "NULL")
    testSqlApi("regexp(CAST(null as VARCHAR), '(\\d+)')", "NULL")
    testSqlApi("regexp('100-200', CAST(null as VARCHAR))", "NULL")
  }

  @Test
  def testHex(): Unit = {
    testAllApis(100.hex(), "HEX(100)", "64")

    testAllApis('f2.hex(), "HEX(f2)", "2A")

    testAllApis(nullOf(DataTypes.TINYINT).hex(), "HEX(CAST(NULL AS TINYINT))", "NULL")

    testAllApis('f3.hex(), "HEX(f3)", "2B")

    testAllApis('f4.hex(), "HEX(f4)", "2C")

    testAllApis('f7.hex(), "HEX(f7)", "3")

    testAllApis(12.hex(), "HEX(12)", "C")

    testAllApis(10.hex(), "HEX(10)", "A")

    testAllApis(0.hex(), "HEX(0)", "0")

    testAllApis("ö".hex(), "HEX('ö')", "C3B6")

    testAllApis('f32.hex(), "HEX(f32)", "FFFFFFFFFFFFFFFF")

    testAllApis('f0.hex(), "HEX(f0)", "546869732069732061207465737420537472696E672E")

    testAllApis('f8.hex(), "HEX(f8)", "20546869732069732061207465737420537472696E672E20")

    testAllApis('f23.hex(), "HEX(f23)", "25546869732069732061207465737420537472696E672E")

    testAllApis('f24.hex(), "HEX(f24)", "2A5F546869732069732061207465737420537472696E672E")
  }

  @Test
  def testBin(): Unit = {
    testAllApis(nullOf(DataTypes.TINYINT).bin(), "BIN((CAST(NULL AS TINYINT)))", "NULL")

    testAllApis('f2.bin(), "BIN(f2)", "101010")

    testAllApis('f3.bin(), "BIN(f3)", "101011")

    testAllApis('f4.bin(), "BIN(f4)", "101100")

    testAllApis('f7.bin(), "BIN(f7)", "11")

    testAllApis(12.bin(), "BIN(12)", "1100")

    testAllApis(10.bin(), "BIN(10)", "1010")

    testAllApis(0.bin(), "BIN(0)", "0")

    testAllApis(
      'f32.bin(),
      "BIN(f32)",
      "1111111111111111111111111111111111111111111111111111111111111111")
  }

  @Test
  def testUUID(): Unit = {
    testAllApis(uuid().charLength(), "CHARACTER_LENGTH(UUID())", "36")

    testAllApis(uuid().substring(9, 1), "SUBSTRING(UUID(), 9, 1)", "-")

    testAllApis(uuid().substring(14, 1), "SUBSTRING(UUID(), 14, 1)", "-")

    testAllApis(uuid().substring(19, 1), "SUBSTRING(UUID(), 19, 1)", "-")

    testAllApis(uuid().substring(24, 1), "SUBSTRING(UUID(), 24, 1)", "-")
  }

  @Test
  def testTruncate(): Unit = {
    testAllApis('f29.truncate('f30), "truncate(f29, f30)", "0.4")

    testAllApis('f29.truncate(), "truncate(f29)", "0.0")

    testAllApis('f31.truncate('f7), "truncate(f31, f7)", "-0.123")

    testAllApis('f31.truncate(), "truncate(f31)", "0")

    testAllApis('f4.truncate('f32), "truncate(f4, f32)", "40")

    testAllApis('f4.truncate(), "truncate(f4)", "44")

    testAllApis('f28.cast(DataTypes.DOUBLE).truncate(1), "truncate(cast(f28 as DOUBLE), 1)", "0.4")

    testAllApis('f28.cast(DataTypes.DOUBLE).truncate(), "truncate(cast(f28 as DOUBLE))", "0.0")

    // TODO: ignore TableApiTest for cast to DECIMAL(p, s) is not support now.
    //  see https://issues.apache.org/jira/browse/FLINK-13651
    //    testAllApis(
    //      'f31.cast(DataTypes.DECIMAL(38, 18)).truncate(2),
    //      "f31.cast(DECIMAL(10, 10)).truncate(2)",
    //      "truncate(cast(f31 as decimal(38, 18)), 2)",
    //      "-0.12")
    //
    //    testAllApis(
    //      'f36.cast(DataTypes.DECIMAL(38, 18)).truncate(),
    //      "f36.cast(DECIMAL(10, 10)).truncate()",
    //      "truncate(42.324)",
    //      "42")

    testSqlApi("truncate(cast(f31 as decimal(38, 18)), 2)", "-0.12")

    testSqlApi("truncate(cast(f31 as decimal(38, 18)))", "0")

    testAllApis('f28.cast(DataTypes.FLOAT).truncate(1), "truncate(cast(f28 as float), 1)", "0.4")

    testAllApis('f5.cast(DataTypes.FLOAT).truncate(), "truncate(cast(f5 as float))", "4.0")

    testAllApis(42.truncate(-1), "truncate(42, -1)", "40")

    testAllApis(42.truncate(-3), "truncate(42, -3)", "0")

    testAllApis(42.truncate(), "truncate(42)", "42")

    //    The validation parameter is null
    testAllApis('f33.cast(DataTypes.INT).truncate(1), "truncate(cast(null as integer), 1)", "NULL")

    testAllApis(
      43.21.truncate('f33.cast(DataTypes.INT)),
      "truncate(43.21, cast(null as integer))",
      "NULL")

    testAllApis(
      'f33.cast(DataTypes.DOUBLE).truncate(1),
      "truncate(cast(null as double), 1)",
      "NULL")

    testAllApis('f33.cast(DataTypes.INT).truncate(1), "truncate(cast(null as integer))", "NULL")

    testAllApis('f33.cast(DataTypes.DOUBLE).truncate(), "truncate(cast(null as double))", "NULL")

    // TODO: ignore TableApiTest for cast to DECIMAL(p, s) is not support now.
    //  see https://issues.apache.org/jira/browse/FLINK-13651
    //    testAllApis(
    //      'f33.cast(DataTypes.DECIMAL(10, 5)).truncate(),
    //      "f33.cast(DECIMAL(10, 5)).truncate()",
    //      "truncate(cast(null as decimal(10, 5)))",
    //      "NULL")
  }

  // ----------------------------------------------------------------------------------------------
  // Math functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testExp(): Unit = {
    testAllApis('f2.exp(), "EXP(f2)", math.exp(42.toByte).toString)

    testAllApis('f3.exp(), "EXP(f3)", math.exp(43.toShort).toString)

    testAllApis('f4.exp(), "EXP(f4)", math.exp(44.toLong).toString)

    testAllApis('f5.exp(), "EXP(f5)", math.exp(4.5.toFloat).toString)

    testAllApis('f6.exp(), "EXP(f6)", math.exp(4.6).toString)

    testAllApis('f7.exp(), "EXP(f7)", math.exp(3).toString)

    testAllApis(3.exp(), "EXP(3)", math.exp(3).toString)
  }

  @Test
  def testLog10(): Unit = {
    testAllApis('f2.log10(), "LOG10(f2)", math.log10(42.toByte).toString)

    testAllApis('f3.log10(), "LOG10(f3)", math.log10(43.toShort).toString)

    testAllApis('f4.log10(), "LOG10(f4)", math.log10(44.toLong).toString)

    testAllApis('f5.log10(), "LOG10(f5)", math.log10(4.5.toFloat).toString)

    testAllApis('f6.log10(), "LOG10(f6)", math.log10(4.6).toString)
  }

  @Test
  def testPower(): Unit = {
    // f7: int , f4: long, f6: double
    testAllApis('f2.power('f7), "POWER(f2, f7)", math.pow(42.toByte, 3).toString)

    testAllApis('f3.power('f6), "POWER(f3, f6)", math.pow(43.toShort, 4.6d).toString)

    testAllApis('f4.power('f5), "POWER(f4, f5)", math.pow(44.toLong, 4.5.toFloat).toString)

    testAllApis('f4.power('f5), "POWER(f4, f5)", math.pow(44.toLong, 4.5.toFloat).toString)

    // f5: float
    testAllApis('f5.power('f5), "power(f5, f5)", math.pow(4.5f, 4.5f).toString)

    testAllApis('f5.power('f6), "power(f5, f6)", math.pow(4.5f, 4.6d).toString)

    testAllApis('f5.power('f7), "power(f5, f7)", math.pow(4.5f, 3).toString)

    testAllApis('f5.power('f4), "power(f5, f4)", math.pow(4.5f, 44L).toString)

    // f22: bigDecimal
    // TODO delete casting in SQL when CALCITE-1467 is fixed
    testAllApis(
      'f22.cast(DataTypes.DOUBLE).power('f5),
      "power(CAST(f22 AS DOUBLE), f5)",
      math.pow(2, 4.5f).toString)

    testAllApis(
      'f22.cast(DataTypes.DOUBLE).power('f6),
      "power(CAST(f22 AS DOUBLE), f6)",
      math.pow(2, 4.6d).toString)

    testAllApis(
      'f22.cast(DataTypes.DOUBLE).power('f7),
      "power(CAST(f22 AS DOUBLE), f7)",
      math.pow(2, 3).toString)

    testAllApis(
      'f22.cast(DataTypes.DOUBLE).power('f4),
      "power(CAST(f22 AS DOUBLE), f4)",
      math.pow(2, 44L).toString)

    testAllApis(
      'f6.power('f22.cast(DataTypes.DOUBLE)),
      "power(f6, f22)",
      math.pow(4.6d, 2).toString)
  }

  @Test
  def testSqrt(): Unit = {
    testAllApis('f6.sqrt(), "SQRT(f6)", math.sqrt(4.6d).toString)

    testAllApis('f7.sqrt(), "SQRT(f7)", math.sqrt(3).toString)

    testAllApis('f4.sqrt(), "SQRT(f4)", math.sqrt(44L).toString)

    testAllApis(
      'f22.cast(DataTypes.DOUBLE).sqrt(),
      "SQRT(CAST(f22 AS DOUBLE))",
      math.sqrt(2.0).toString)

    testAllApis('f5.sqrt(), "SQRT(f5)", math.pow(4.5f, 0.5).toString)

    testAllApis(25.sqrt(), "SQRT(25)", "5.0")

    testAllApis(
      2.2.sqrt(),
      "POWER(CAST(2.2 AS DOUBLE), CAST(0.5 AS DOUBLE))",
      math.sqrt(2.2).toString)
  }

  @Test
  def testCosh(): Unit = {
    testAllApis(0.cosh(), "COSH(0)", math.cosh(0).toString)

    testAllApis(-1.cosh(), "COSH(-1)", math.cosh(-1).toString)

    testAllApis('f4.cosh(), "COSH(f4)", math.cosh(44L).toString)

    testAllApis('f6.cosh(), "COSH(f6)", math.cosh(4.6d).toString)

    testAllApis('f7.cosh(), "COSH(f7)", math.cosh(3).toString)

    testAllApis('f22.cosh(), "COSH(f22)", math.cosh(2.0).toString)
  }

  @Test
  def testLn(): Unit = {
    testAllApis('f2.ln(), "LN(f2)", math.log(42.toByte).toString)

    testAllApis('f3.ln(), "LN(f3)", math.log(43.toShort).toString)

    testAllApis('f4.ln(), "LN(f4)", math.log(44.toLong).toString)

    testAllApis('f5.ln(), "LN(f5)", math.log(4.5.toFloat).toString)

    testAllApis('f6.ln(), "LN(f6)", math.log(4.6).toString)
  }

  @Test
  def testAbs(): Unit = {
    testAllApis('f2.abs(), "ABS(f2)", "42")

    testAllApis('f3.abs(), "ABS(f3)", "43")

    testAllApis('f4.abs(), "ABS(f4)", "44")

    testAllApis('f5.abs(), "ABS(f5)", "4.5")

    testAllApis('f6.abs(), "ABS(f6)", "4.6")

    testAllApis('f9.abs(), "ABS(f9)", "42")

    testAllApis('f10.abs(), "ABS(f10)", "43")

    testAllApis('f11.abs(), "ABS(f11)", "44")

    testAllApis('f12.abs(), "ABS(f12)", "4.5")

    testAllApis('f13.abs(), "ABS(f13)", "4.6")

    testAllApis('f15.abs(), "ABS(f15)", "1231.1231231321321321111")
  }

  @Test
  def testArithmeticFloorCeil(): Unit = {
    testAllApis('f5.floor(), "FLOOR(f5)", "4.0")

    testAllApis('f5.ceil(), "CEIL(f5)", "5.0")

    testAllApis('f3.floor(), "FLOOR(f3)", "43")

    testAllApis('f3.ceil(), "CEIL(f3)", "43")

    testAllApis('f15.floor(), "FLOOR(f15)", "-1232")

    testAllApis('f15.ceil(), "CEIL(f15)", "-1231")
  }

  @Test
  def testSin(): Unit = {
    testAllApis('f2.sin(), "SIN(f2)", math.sin(42.toByte).toString)

    testAllApis('f3.sin(), "SIN(f3)", math.sin(43.toShort).toString)

    testAllApis('f4.sin(), "SIN(f4)", math.sin(44.toLong).toString)

    testAllApis('f5.sin(), "SIN(f5)", math.sin(4.5.toFloat).toString)

    testAllApis('f6.sin(), "SIN(f6)", math.sin(4.6).toString)

    testAllApis('f15.sin(), "SIN(f15)", math.sin(-1231.1231231321321321111).toString)
  }

  @Test
  def testCos(): Unit = {
    testAllApis('f2.cos(), "COS(f2)", math.cos(42.toByte).toString)

    testAllApis('f3.cos(), "COS(f3)", math.cos(43.toShort).toString)

    testAllApis('f4.cos(), "COS(f4)", math.cos(44.toLong).toString)

    testAllApis('f5.cos(), "COS(f5)", math.cos(4.5.toFloat).toString)

    testAllApis('f6.cos(), "COS(f6)", math.cos(4.6).toString)

    testAllApis('f15.cos(), "COS(f15)", math.cos(-1231.1231231321321321111).toString)
  }

  @Test
  def testSinh(): Unit = {
    testAllApis(0.sinh(), "SINH(0)", math.sinh(0).toString)

    testAllApis(-1.sinh(), "SINH(-1)", math.sinh(-1).toString)

    testAllApis('f4.sinh(), "SINH(f4)", math.sinh(44L).toString)

    testAllApis('f6.sinh(), "SINH(f6)", math.sinh(4.6d).toString)

    testAllApis('f7.sinh(), "SINH(f7)", math.sinh(3).toString)

    testAllApis('f22.sinh(), "SINH(f22)", math.sinh(2.0).toString)
  }

  @Test
  def testTan(): Unit = {
    testAllApis('f2.tan(), "TAN(f2)", math.tan(42.toByte).toString)

    testAllApis('f3.tan(), "TAN(f3)", math.tan(43.toShort).toString)

    testAllApis('f4.tan(), "TAN(f4)", math.tan(44.toLong).toString)

    testAllApis('f5.tan(), "TAN(f5)", math.tan(4.5.toFloat).toString)

    testAllApis('f6.tan(), "TAN(f6)", math.tan(4.6).toString)

    testAllApis('f15.tan(), "TAN(f15)", math.tan(-1231.1231231321321321111).toString)
  }

  @Test
  def testTanh(): Unit = {
    testAllApis(0.tanh(), "TANH(0)", math.tanh(0).toString)

    testAllApis(-1.tanh(), "TANH(-1)", math.tanh(-1).toString)

    testAllApis('f4.tanh(), "TANH(f4)", math.tanh(44L).toString)

    testAllApis('f6.tanh(), "TANH(f6)", math.tanh(4.6d).toString)

    testAllApis('f7.tanh(), "TANH(f7)", math.tanh(3).toString)

    testAllApis('f22.tanh(), "TANH(f22)", math.tanh(2.0).toString)
  }

  @Test
  def testCot(): Unit = {
    testAllApis('f2.cot(), "COT(f2)", (1.0d / math.tan(42.toByte)).toString)

    testAllApis('f3.cot(), "COT(f3)", (1.0d / math.tan(43.toShort)).toString)

    testAllApis('f4.cot(), "COT(f4)", (1.0d / math.tan(44.toLong)).toString)

    testAllApis('f5.cot(), "COT(f5)", (1.0d / math.tan(4.5.toFloat)).toString)

    testAllApis('f6.cot(), "COT(f6)", (1.0d / math.tan(4.6)).toString)

    testAllApis('f15.cot(), "COT(f15)", (1.0d / math.tan(-1231.1231231321321321111)).toString)
  }

  @Test
  def testAsin(): Unit = {
    testAllApis('f25.asin(), "ASIN(f25)", math.asin(0.42.toByte).toString)

    testAllApis('f26.asin(), "ASIN(f26)", math.asin(0.toShort).toString)

    testAllApis('f27.asin(), "ASIN(f27)", math.asin(0.toLong).toString)

    testAllApis('f28.asin(), "ASIN(f28)", math.asin(0.45.toFloat).toString)

    testAllApis('f29.asin(), "ASIN(f29)", math.asin(0.46).toString)

    testAllApis('f30.asin(), "ASIN(f30)", math.asin(1).toString)

    testAllApis('f31.asin(), "ASIN(f31)", math.asin(-0.1231231321321321111).toString)
  }

  @Test
  def testAcos(): Unit = {
    testAllApis('f25.acos(), "ACOS(f25)", math.acos(0.42.toByte).toString)

    testAllApis('f26.acos(), "ACOS(f26)", math.acos(0.toShort).toString)

    testAllApis('f27.acos(), "ACOS(f27)", math.acos(0.toLong).toString)

    testAllApis('f28.acos(), "ACOS(f28)", math.acos(0.45.toFloat).toString)

    testAllApis('f29.acos(), "ACOS(f29)", math.acos(0.46).toString)

    testAllApis('f30.acos(), "ACOS(f30)", math.acos(1).toString)

    testAllApis('f31.acos(), "ACOS(f31)", math.acos(-0.1231231321321321111).toString)
  }

  @Test
  def testAtan(): Unit = {
    testAllApis('f25.atan(), "ATAN(f25)", math.atan(0.42.toByte).toString)

    testAllApis('f26.atan(), "ATAN(f26)", math.atan(0.toShort).toString)

    testAllApis('f27.atan(), "ATAN(f27)", math.atan(0.toLong).toString)

    testAllApis('f28.atan(), "ATAN(f28)", math.atan(0.45.toFloat).toString)

    testAllApis('f29.atan(), "ATAN(f29)", math.atan(0.46).toString)

    testAllApis('f30.atan(), "ATAN(f30)", math.atan(1).toString)

    testAllApis('f31.atan(), "ATAN(f31)", math.atan(-0.1231231321321321111).toString)
  }

  @Test
  def testAtan2(): Unit = {
    testAllApis(atan2('f25, 'f26), "ATAN2(f25, f26)", math.atan2(0.42.toByte, 0.toByte).toString)

    testAllApis(atan2('f26, 'f25), "ATAN2(f26, f25)", math.atan2(0.toShort, 0.toShort).toString)

    testAllApis(atan2('f27, 'f27), "ATAN2(f27, f27)", math.atan2(0.toLong, 0.toLong).toString)

    testAllApis(
      atan2('f28, 'f28),
      "ATAN2(f28, f28)",
      math.atan2(0.45.toFloat, 0.45.toFloat).toString)

    testAllApis(atan2('f29, 'f29), "ATAN2(f29, f29)", math.atan2(0.46, 0.46).toString)

    testAllApis(atan2('f30, 'f30), "ATAN2(f30, f30)", math.atan2(1, 1).toString)

    testAllApis(
      atan2('f31, 'f31),
      "ATAN2(f31, f31)",
      math.atan2(-0.1231231321321321111, -0.1231231321321321111).toString)
  }

  @Test
  def testDegrees(): Unit = {
    testAllApis('f2.degrees(), "DEGREES(f2)", math.toDegrees(42.toByte).toString)

    testAllApis('f3.degrees(), "DEGREES(f3)", math.toDegrees(43.toShort).toString)

    testAllApis('f4.degrees(), "DEGREES(f4)", math.toDegrees(44.toLong).toString)

    testAllApis('f5.degrees(), "DEGREES(f5)", math.toDegrees(4.5.toFloat).toString)

    testAllApis('f6.degrees(), "DEGREES(f6)", math.toDegrees(4.6).toString)

    testAllApis('f15.degrees(), "DEGREES(f15)", math.toDegrees(-1231.1231231321321321111).toString)
  }

  @Test
  def testRadians(): Unit = {
    testAllApis('f2.radians(), "RADIANS(f2)", math.toRadians(42.toByte).toString)

    testAllApis('f3.radians(), "RADIANS(f3)", math.toRadians(43.toShort).toString)

    testAllApis('f4.radians(), "RADIANS(f4)", math.toRadians(44.toLong).toString)

    testAllApis('f5.radians(), "RADIANS(f5)", math.toRadians(4.5.toFloat).toString)

    testAllApis('f6.radians(), "RADIANS(f6)", math.toRadians(4.6).toString)

    testAllApis('f15.radians(), "RADIANS(f15)", math.toRadians(-1231.1231231321321321111).toString)
  }

  @Test
  def testSign(): Unit = {
    testAllApis('f4.sign(), "SIGN(f4)", 1.toString)

    testAllApis('f6.sign(), "SIGN(f6)", 1.0.toString)

    testAllApis(
      'f15.sign(),
      "SIGN(f15)",
      "-1.0000000000000000000"
    ) // calcite: SIGN(Decimal(p,s)) => Decimal(p,s)
  }

  @Test
  def testRound(): Unit = {
    // behavior test
    testAllApis('f29.round('f30), "ROUND(f29, f30)", 0.5.toString)

    testAllApis('f4.round('f32), "ROUND(f4, f32)", 40.toString)

    testSqlApi("ROUND(125.315)", "125")

    testSqlApi("ROUND(-125.315, 2)", "-125.32")

    testSqlApi("ROUND(125.315, 0)", "125")

    testSqlApi("ROUND(1.4, 1)", "1.4")

    // type test
    testAllApis(123.cast(DataTypes.TINYINT).round(-2), "ROUND(CAST(123 AS TINYINT), -2)", "100")
    testSqlApi("ROUND(CAST(123 AS TINYINT))", "123")
    testAllApis(nullOf(DataTypes.TINYINT).round(-2), "ROUND(CAST(NULL AS TINYINT), -2)", "NULL")
    testAllApis(
      123.cast(DataTypes.TINYINT).round(nullOf(DataTypes.INT)),
      "ROUND(CAST(123 AS TINYINT), CAST(NULL AS INT))",
      "NULL")
    testSqlApi("ROUND(CAST(NULL AS TINYINT))", "NULL")

    testAllApis(123.cast(DataTypes.SMALLINT).round(-2), "ROUND(CAST(123 AS SMALLINT), -2)", "100")
    testSqlApi("ROUND(CAST(123 AS SMALLINT))", "123")
    testAllApis(nullOf(DataTypes.SMALLINT).round(-2), "ROUND(CAST(NULL AS SMALLINT), -2)", "NULL")
    testAllApis(
      123.cast(DataTypes.SMALLINT).round(nullOf(DataTypes.INT)),
      "ROUND(CAST(123 AS SMALLINT), CAST(NULL AS INT))",
      "NULL")
    testSqlApi("ROUND(CAST(NULL AS SMALLINT))", "NULL")

    testAllApis(123.cast(DataTypes.INT).round(-2), "ROUND(CAST(123 AS INT), -2)", "100")
    testSqlApi("ROUND(CAST(123 AS INT))", "123")
    testAllApis(nullOf(DataTypes.INT).round(-2), "ROUND(CAST(NULL AS INT), -2)", "NULL")
    testAllApis(
      123.cast(DataTypes.INT).round(nullOf(DataTypes.INT)),
      "ROUND(CAST(123 AS INT), CAST(NULL AS INT))",
      "NULL")
    testSqlApi("ROUND(CAST(NULL AS INT))", "NULL")

    testAllApis(123.cast(DataTypes.BIGINT).round(-2), "ROUND(CAST(123 AS BIGINT), -2)", "100")
    testSqlApi("ROUND(CAST(123 AS BIGINT))", "123")
    testAllApis(nullOf(DataTypes.BIGINT).round(-2), "ROUND(CAST(NULL AS BIGINT), -2)", "NULL")
    testAllApis(
      123.cast(DataTypes.BIGINT).round(nullOf(DataTypes.INT)),
      "ROUND(CAST(123 AS BIGINT), CAST(NULL AS INT))",
      "NULL")
    testSqlApi("ROUND(CAST(NULL AS BIGINT))", "NULL")

    testAllApis(1.2345.cast(DataTypes.FLOAT).round(3), "ROUND(CAST(1.2345 AS FLOAT), 3)", "1.235")
    testSqlApi("ROUND(CAST(1.2345 AS FLOAT))", "1.0")
    testAllApis(nullOf(DataTypes.FLOAT).round(3), "ROUND(CAST(NULL AS FLOAT), 3)", "NULL")
    testAllApis(
      1.2345.cast(DataTypes.FLOAT).round(nullOf(DataTypes.INT)),
      "ROUND(CAST(1.2345 AS FLOAT), CAST(NULL AS INT))",
      "NULL")
    testSqlApi("ROUND(CAST(NULL AS FLOAT))", "NULL")

    testAllApis(1.2345.cast(DataTypes.DOUBLE).round(3), "ROUND(CAST(1.2345 AS DOUBLE), 3)", "1.235")
    testSqlApi("ROUND(CAST(1.2345 AS DOUBLE))", "1.0")
    testAllApis(nullOf(DataTypes.DOUBLE).round(3), "ROUND(CAST(NULL AS DOUBLE), 3)", "NULL")
    testAllApis(
      1.2345.cast(DataTypes.DOUBLE).round(nullOf(DataTypes.INT)),
      "ROUND(CAST(1.2345 AS DOUBLE), CAST(NULL AS INT))",
      "NULL")
    testSqlApi("ROUND(CAST(NULL AS DOUBLE))", "NULL")

    // table api string currently does not support decimal type
    testSqlApi("ROUND(CAST(1.2345 AS DECIMAL(5, 4)), 3)", "1.235")
    testSqlApi("ROUND(CAST(1.2345 AS DECIMAL(5, 4)))", "1")
    testSqlApi("ROUND(CAST(NULL AS DECIMAL(5, 4)), 3)", "NULL")
    testSqlApi("ROUND(CAST(1.2345 AS DECIMAL(5, 4)), CAST(NULL AS INT))", "NULL")
    testSqlApi("ROUND(CAST(NULL AS DECIMAL(5, 4)))", "NULL")
  }

  @Test
  def testPi(): Unit = {
    testAllApis(pi(), "pi()", math.Pi.toString)
  }

  @Test
  def testRandAndRandInteger(): Unit = {
    val random1 = new java.util.Random(1)
    testAllApis(rand(1), "RAND(1)", random1.nextDouble().toString)

    val random2 = new java.util.Random(3)
    testAllApis(rand('f7), "RAND(f7)", random2.nextDouble().toString)

    val random3 = new java.util.Random(1)
    testAllApis(randInteger(1, 10), "RAND_INTEGER(1, 10)", random3.nextInt(10).toString)

    val random4 = new java.util.Random(3)
    testAllApis(
      randInteger('f7, 'f4.cast(DataTypes.INT)),
      "RAND_INTEGER(f7, CAST(f4 AS INT))",
      random4.nextInt(44).toString)

    val random5 = new java.util.Random(1)
    testAllApis(rand(1).plus(1), "RAND(1) + 1", (random5.nextDouble() + 1).toString)

    val random6 = new java.util.Random(1)
    val random7 = new java.util.Random(2)
    testAllApis(
      rand(1).plus(rand(2)),
      "RAND(1) + RAND(2)",
      (random6.nextDouble() + random7.nextDouble()).toString)

    // the f21 is null
    testAllApis(rand('f21.cast(DataTypes.INT())).plus(1), "rand(cast(null as int)) + 1", "NULL")
  }

  @Test
  def testE(): Unit = {
    testAllApis(e(), "E()", math.E.toString)

    testAllApis(e(), "e()", math.E.toString)
  }

  @Test
  def testLog(): Unit = {
    testAllApis('f6.log(), "LOG(f6)", "1.5260563034950492")

    testAllApis(('f6 - 'f6 + 100).log('f6 - 'f6 + 10), "LOG(f6 - f6 + 10, f6 - f6 + 100)", "2.0")

    testAllApis(('f6 + 20).log(), "LOG(f6+20)", "3.202746442938317")

    testAllApis(10.log(), "LOG(10)", "2.302585092994046")

    testAllApis(100.log(10), "LOG(10, 100)", "2.0")

    testSqlApi(
      "LOG(cast (null AS DOUBLE))",
      "NULL"
    )

    testSqlApi(
      "LOG(cast (null AS DOUBLE), 1)",
      "NULL"
    )

    testSqlApi(
      "LOG(1, cast (null AS DOUBLE))",
      "NULL"
    )
    testSqlApi(
      "LOG(cast (null AS DOUBLE), cast (null AS DOUBLE))",
      "NULL"
    )

    // test Infinite or NaN
    // Infinity
    testSqlApi("LOG(1, 100)", "Infinity")
    // NaN
    testSqlApi("LOG(-1)", "NaN")
  }

  @Test
  def testLog2(): Unit = {
    testAllApis('f6.log2(), "LOG2(f6)", "2.2016338611696504")

    testAllApis(('f6 - 'f6 + 100).log2(), "LOG2(f6 - f6 + 100)", "6.643856189774725")

    testAllApis(('f6 + 20).log2(), "LOG2(f6+20)", "4.620586410451877")

    testAllApis(10.log2(), "LOG2(10)", "3.3219280948873626")

    testSqlApi(
      "LOG2(cast (null AS DOUBLE))",
      "NULL"
    )
  }

  @Test
  def testChr(): Unit = {
    testAllApis(
      65.chr(),
      "CHR(65)",
      "A"
    )

    testAllApis(
      -9.chr(),
      "CHR(-9)",
      ""
    )

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
      "CHR(0)",
      Character.MIN_VALUE.toString
    )

    testSqlApi(
      "CHR(149)",
      149.toChar.toString
    )

    testSqlApi(
      "CHR(cast (null AS BIGINT))",
      "NULL"
    )
  }

  // ----------------------------------------------------------------------------------------------
  // Temporal functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testExtract(): Unit = {
    testAllApis('f16.extract(TimeIntervalUnit.MILLENNIUM), "EXTRACT(MILLENNIUM FROM f16)", "2")

    testAllApis('f16.extract(TimeIntervalUnit.CENTURY), "EXTRACT(CENTURY FROM f16)", "20")

    testAllApis('f16.extract(TimeIntervalUnit.DECADE), "EXTRACT(DECADE FROM f16)", "199")

    testAllApis('f16.extract(TimeIntervalUnit.YEAR), "EXTRACT(YEAR FROM f16)", "1996")

    testAllApis('f16.extract(TimeIntervalUnit.QUARTER), "EXTRACT(QUARTER FROM f16)", "4")

    testAllApis('f16.extract(TimeIntervalUnit.MONTH), "EXTRACT(MONTH FROM f16)", "11")

    testAllApis('f16.extract(TimeIntervalUnit.WEEK), "EXTRACT(WEEK FROM f16)", "45")

    testAllApis('f16.extract(TimeIntervalUnit.DAY), "EXTRACT(DAY FROM f16)", "10")

    testAllApis('f18.extract(TimeIntervalUnit.YEAR), "EXTRACT(YEAR FROM f18)", "1996")

    testAllApis('f18.extract(TimeIntervalUnit.QUARTER), "EXTRACT(QUARTER FROM f18)", "4")

    testAllApis('f16.extract(TimeIntervalUnit.QUARTER), "EXTRACT(QUARTER FROM f16)", "4")

    testAllApis('f18.extract(TimeIntervalUnit.MONTH), "EXTRACT(MONTH FROM f18)", "11")

    testAllApis('f18.extract(TimeIntervalUnit.WEEK), "EXTRACT(WEEK FROM f18)", "45")

    testAllApis('f18.extract(TimeIntervalUnit.DAY), "EXTRACT(DAY FROM f18)", "10")

    testAllApis('f18.extract(TimeIntervalUnit.HOUR), "EXTRACT(HOUR FROM f18)", "6")

    testAllApis('f17.extract(TimeIntervalUnit.HOUR), "EXTRACT(HOUR FROM f17)", "6")

    testAllApis('f18.extract(TimeIntervalUnit.MINUTE), "EXTRACT(MINUTE FROM f18)", "55")

    testAllApis('f17.extract(TimeIntervalUnit.MINUTE), "EXTRACT(MINUTE FROM f17)", "55")

    testAllApis('f18.extract(TimeIntervalUnit.SECOND), "EXTRACT(SECOND FROM f18)", "44")

    testAllApis('f17.extract(TimeIntervalUnit.SECOND), "EXTRACT(SECOND FROM f17)", "44")

    testAllApis('f18.extract(TimeIntervalUnit.MILLISECOND), "EXTRACT(MILLISECOND FROM f18)", "333")

    testAllApis('f17.extract(TimeIntervalUnit.MILLISECOND), "EXTRACT(MILLISECOND FROM f17)", "0")

    testAllApis(
      'f18.extract(TimeIntervalUnit.MICROSECOND),
      "EXTRACT(MICROSECOND FROM f18)",
      "333000")

    testAllApis(
      'f18.extract(TimeIntervalUnit.NANOSECOND),
      "EXTRACT(NANOSECOND FROM f18)",
      "333000000")

    testAllApis('f19.extract(TimeIntervalUnit.DAY), "EXTRACT(DAY FROM f19)", "16979")

    testAllApis('f19.extract(TimeIntervalUnit.HOUR), "EXTRACT(HOUR FROM f19)", "7")

    testAllApis('f19.extract(TimeIntervalUnit.MINUTE), "EXTRACT(MINUTE FROM f19)", "23")

    testAllApis('f19.extract(TimeIntervalUnit.SECOND), "EXTRACT(SECOND FROM f19)", "33")

    testAllApis('f20.extract(TimeIntervalUnit.MONTH), "EXTRACT(MONTH FROM f20)", "1")

    testAllApis('f20.extract(TimeIntervalUnit.QUARTER), "EXTRACT(QUARTER FROM f20)", "1")

    testAllApis('f20.extract(TimeIntervalUnit.YEAR), "EXTRACT(YEAR FROM f20)", "2")

    // test SQL only time units
    testSqlApi("EXTRACT(MILLENNIUM FROM f18)", "2")

    testSqlApi("EXTRACT(MILLENNIUM FROM f16)", "2")

    testSqlApi("EXTRACT(CENTURY FROM f18)", "20")

    testSqlApi("EXTRACT(CENTURY FROM f16)", "20")

    testSqlApi("EXTRACT(DECADE FROM f18)", "199")

    testSqlApi("EXTRACT(DECADE FROM f16)", "199")

    testSqlApi("EXTRACT(DOY FROM f18)", "315")

    testSqlApi("EXTRACT(DOY FROM f16)", "315")

    testSqlApi("EXTRACT(ISOYEAR FROM f18)", "1996")

    testSqlApi("EXTRACT(ISOYEAR FROM f16)", "1996")

    testSqlApi("EXTRACT(DOW FROM f18)", "1")

    testSqlApi("EXTRACT(DOW FROM f16)", "1")

    testSqlApi("EXTRACT(ISODOW FROM f18)", "7")

    testSqlApi("EXTRACT(ISODOW FROM f16)", "7")

    testSqlApi("EXTRACT(QUARTER FROM f18)", "4")

    testSqlApi("EXTRACT(QUARTER FROM f16)", "4")

    testSqlApi("EXTRACT(WEEK FROM f18)", "45")

    testSqlApi("EXTRACT(WEEK FROM f16)", "45")

    testSqlApi("YEAR(f18)", "1996")

    testSqlApi("YEAR(f16)", "1996")

    testSqlApi("QUARTER(f18)", "4")

    testSqlApi("QUARTER(f16)", "4")

    testSqlApi("MONTH(f18)", "11")

    testSqlApi("MONTH(f16)", "11")

    testSqlApi("WEEK(f18)", "45")

    testSqlApi("WEEK(f16)", "45")

    testSqlApi("DAYOFYEAR(f18)", "315")

    testSqlApi("DAYOFYEAR(f16)", "315")

    testSqlApi("DAYOFMONTH(f18)", "10")

    testSqlApi("DAYOFMONTH(f16)", "10")

    testSqlApi("DAYOFWEEK(f18)", "1")

    testSqlApi("DAYOFWEEK(f16)", "1")

    testSqlApi("HOUR(f17)", "6")

    testSqlApi("HOUR(f19)", "7")

    testSqlApi("MINUTE(f17)", "55")

    testSqlApi("MINUTE(f19)", "23")

    testSqlApi("SECOND(f17)", "44")

    testSqlApi("SECOND(f19)", "33")
  }

  @Test
  def testTemporalFloor(): Unit = {
    testAllApis('f18.floor(TimeIntervalUnit.YEAR), "FLOOR(f18 TO YEAR)", "1996-01-01 00:00:00.000")

    testAllApis(
      'f18.floor(TimeIntervalUnit.MONTH),
      "FLOOR(f18 TO MONTH)",
      "1996-11-01 00:00:00.000")

    testAllApis('f18.floor(TimeIntervalUnit.DAY), "FLOOR(f18 TO DAY)", "1996-11-10 00:00:00.000")

    testAllApis('f18.floor(TimeIntervalUnit.HOUR), "FLOOR(f18 TO HOUR)", "1996-11-10 06:00:00.000")

    testAllApis(
      'f18.floor(TimeIntervalUnit.MINUTE),
      "FLOOR(f18 TO MINUTE)",
      "1996-11-10 06:55:00.000")

    testAllApis(
      'f18.floor(TimeIntervalUnit.SECOND),
      "FLOOR(f18 TO SECOND)",
      "1996-11-10 06:55:44.000")

    testAllApis('f17.floor(TimeIntervalUnit.HOUR), "FLOOR(f17 TO HOUR)", "06:00:00")

    testAllApis('f17.floor(TimeIntervalUnit.MINUTE), "FLOOR(f17 TO MINUTE)", "06:55:00")

    testAllApis('f17.floor(TimeIntervalUnit.SECOND), "FLOOR(f17 TO SECOND)", "06:55:44")

    testAllApis('f16.floor(TimeIntervalUnit.YEAR), "FLOOR(f16 TO YEAR)", "1996-01-01")

    testAllApis('f16.floor(TimeIntervalUnit.MONTH), "FLOOR(f16 TO MONTH)", "1996-11-01")

    testAllApis('f18.ceil(TimeIntervalUnit.YEAR), "CEIL(f18 TO YEAR)", "1997-01-01 00:00:00.000")

    testAllApis('f18.ceil(TimeIntervalUnit.MONTH), "CEIL(f18 TO MONTH)", "1996-12-01 00:00:00.000")

    testAllApis('f18.ceil(TimeIntervalUnit.DAY), "CEIL(f18 TO DAY)", "1996-11-11 00:00:00.000")

    testAllApis('f18.ceil(TimeIntervalUnit.HOUR), "CEIL(f18 TO HOUR)", "1996-11-10 07:00:00.000")

    testAllApis(
      'f18.ceil(TimeIntervalUnit.MINUTE),
      "CEIL(f18 TO MINUTE)",
      "1996-11-10 06:56:00.000")

    testAllApis(
      'f18.ceil(TimeIntervalUnit.SECOND),
      "CEIL(f18 TO SECOND)",
      "1996-11-10 06:55:45.000")

    testAllApis('f17.ceil(TimeIntervalUnit.HOUR), "CEIL(f17 TO HOUR)", "07:00:00")

    testAllApis('f17.ceil(TimeIntervalUnit.MINUTE), "CEIL(f17 TO MINUTE)", "06:56:00")

    testAllApis('f17.ceil(TimeIntervalUnit.SECOND), "CEIL(f17 TO SECOND)", "06:55:44")

    testAllApis('f16.ceil(TimeIntervalUnit.YEAR), "CEIL(f16 TO YEAR)", "1997-01-01")

    testAllApis('f16.ceil(TimeIntervalUnit.MONTH), "CEIL(f16 TO MONTH)", "1996-12-01")
  }

  @Test
  def testCurrentTimePoint(): Unit = {

    // current time points are non-deterministic
    // we just test the format of the output
    // manual test can be found in NonDeterministicTests

    // e.g: 2021-04-19
    testAllApis(
      currentDate().cast(DataTypes.STRING).charLength(),
      "CHAR_LENGTH(CAST(CURRENT_DATE AS VARCHAR))",
      "10")

    // e.g: 12:13:43
    testAllApis(
      localTime().cast(DataTypes.STRING).charLength(),
      "CHAR_LENGTH(CAST(LOCALTIME AS VARCHAR))",
      "8")

    // e.g: 12:13:43
    testAllApis(
      currentTime().cast(DataTypes.STRING).charLength(),
      "CHAR_LENGTH(CAST(CURRENT_TIME AS VARCHAR))",
      "8")

    // e.g: 2021-04-19 12:13:43.678
    testAllApis(
      localTimestamp().cast(DataTypes.STRING).charLength(),
      "CHAR_LENGTH(CAST(LOCALTIMESTAMP AS VARCHAR))",
      "23")

    // e.g: 2021-04-19 12:13:43.678
    testAllApis(
      currentTimestamp().cast(DataTypes.STRING).charLength(),
      "CHAR_LENGTH(CAST(CURRENT_TIMESTAMP AS VARCHAR))",
      "23")

    // e.g: 2021-04-19 12:13:43.678
    testSqlApi("CHAR_LENGTH(CAST(NOW() AS VARCHAR))", "23")

    // e.g: 2021-04-19 12:13:43.678
    testSqlApi("CHAR_LENGTH(CAST(CURRENT_ROW_TIMESTAMP() AS VARCHAR))", "23")

    // comparisons are deterministic
    testAllApis(localTimestamp() === localTimestamp(), "LOCALTIMESTAMP = LOCALTIMESTAMP", "TRUE")
  }

  @Test
  def testOverlaps(): Unit = {
    testAllApis(
      temporalOverlaps("2:55:00".toTime, 1.hour, "3:30:00".toTime, 2.hours),
      "(TIME '2:55:00', INTERVAL '1' HOUR) OVERLAPS (TIME '3:30:00', INTERVAL '2' HOUR)",
      "TRUE"
    )

    testAllApis(
      temporalOverlaps("9:00:00".toTime, "9:30:00".toTime, "9:29:00".toTime, "9:31:00".toTime),
      "(TIME '9:00:00', TIME '9:30:00') OVERLAPS (TIME '9:29:00', TIME '9:31:00')",
      "TRUE"
    )

    testAllApis(
      temporalOverlaps("9:00:00".toTime, "10:00:00".toTime, "10:15:00".toTime, 3.hours),
      "(TIME '9:00:00', TIME '10:00:00') OVERLAPS (TIME '10:15:00', INTERVAL '3' HOUR)",
      "FALSE"
    )

    testAllApis(
      temporalOverlaps("2011-03-10".toDate, 10.days, "2011-03-19".toDate, 10.days),
      "(DATE '2011-03-10', INTERVAL '10' DAY) OVERLAPS (DATE '2011-03-19', INTERVAL '10' DAY)",
      "TRUE"
    )

    testAllApis(
      temporalOverlaps(
        "2011-03-10 05:02:02".toTimestamp,
        0.milli,
        "2011-03-10 05:02:02".toTimestamp,
        "2011-03-10 05:02:01".toTimestamp),
      "(TIMESTAMP '2011-03-10 05:02:02', INTERVAL '0' SECOND) OVERLAPS " +
        "(TIMESTAMP '2011-03-10 05:02:02', TIMESTAMP '2011-03-10 05:02:01')",
      "TRUE"
    )

    testAllApis(
      temporalOverlaps(
        "2011-03-10 02:02:02.001".toTimestamp,
        0.milli,
        "2011-03-10 02:02:02.002".toTimestamp,
        "2011-03-10 02:02:02.002".toTimestamp),
      "(TIMESTAMP '2011-03-10 02:02:02.001', INTERVAL '0' SECOND) OVERLAPS " +
        "(TIMESTAMP '2011-03-10 02:02:02.002', TIMESTAMP '2011-03-10 02:02:02.002')",
      "FALSE"
    )
  }

  @Test
  def testQuarter(): Unit = {
    testSqlApi("QUARTER(DATE '1997-01-27')", "1")

    testSqlApi("QUARTER(DATE '1997-04-27')", "2")

    testSqlApi("QUARTER(DATE '1997-12-31')", "4")
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
      for ((data, index) <- dataParts.zipWithIndex) {
        index match {
          case 0 => // timestamp, timestamp
            testAllApis(
              timestampDiff(unitParts._2, data._1.toTimestamp, data._2.toTimestamp),
              s"TIMESTAMPDIFF(${unitParts._1}, TIMESTAMP '${data._1}', TIMESTAMP '${data._2}')",
              data._3
            )
            testSqlApi( // sql tsi
              s"TIMESTAMPDIFF(${unitParts._3}, TIMESTAMP '${data._1}', TIMESTAMP '${data._2}')",
              data._3)
          case 1 => // date, timestamp
            testAllApis(
              timestampDiff(unitParts._2, data._1.toDate, data._2.toTimestamp),
              s"TIMESTAMPDIFF(${unitParts._1}, DATE '${data._1}', TIMESTAMP '${data._2}')",
              data._3)
            testSqlApi( // sql tsi
              s"TIMESTAMPDIFF(${unitParts._3}, DATE '${data._1}', TIMESTAMP '${data._2}')",
              data._3)
          case 2 => // timestamp, date
            testAllApis(
              timestampDiff(unitParts._2, data._1.toTimestamp, data._2.toDate),
              s"TIMESTAMPDIFF(${unitParts._1}, TIMESTAMP '${data._1}', DATE '${data._2}')",
              data._3)
            testSqlApi( // sql tsi
              s"TIMESTAMPDIFF(${unitParts._3}, TIMESTAMP '${data._1}', DATE '${data._2}')",
              data._3)
          case 3 => // date, date
            testAllApis(
              timestampDiff(unitParts._2, data._1.toDate, data._2.toDate),
              s"TIMESTAMPDIFF(${unitParts._1}, DATE '${data._1}', DATE '${data._2}')",
              data._3)
            testSqlApi( // sql tsi
              s"TIMESTAMPDIFF(${unitParts._3}, DATE '${data._1}', DATE '${data._2}')",
              data._3)
        }
      }
    }

    testAllApis(
      timestampDiff(
        TimePointUnit.DAY,
        nullOf(DataTypes.TIMESTAMP(3)),
        "2016-02-24 12:42:25".toTimestamp),
      "TIMESTAMPDIFF(DAY, CAST(NULL AS TIMESTAMP), TIMESTAMP '2016-02-24 12:42:25')",
      "NULL"
    )

    testAllApis(
      timestampDiff(
        TimePointUnit.DAY,
        "2016-02-24 12:42:25".toTimestamp,
        nullOf(DataTypes.TIMESTAMP(3))),
      "TIMESTAMPDIFF(DAY, TIMESTAMP '2016-02-24 12:42:25',  CAST(NULL AS TIMESTAMP))",
      "NULL"
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
      "2018-02-28 22:58:58.998",
      "2018-08-29 22:58:58.998",
      "2017-08-29 22:58:58.998",
      "2002-08-29 22:58:58.998",
      "1767-11-29 22:58:58.998")

    val MONTH = Seq(
      "2017-12-29 22:58:58.998",
      "2018-02-28 22:58:58.998",
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
          s"TIMESTAMPADD($interval, $offset, TIMESTAMP '$ts')",
          result(i))
      }
    }

    testAllApis(
      "2016-02-24 12:42:25".toTimestamp + nullOf(DataTypes.INTERVAL(DataTypes.MINUTE())),
      "TIMESTAMPADD(HOUR, CAST(NULL AS INTEGER), TIMESTAMP '2016-02-24 12:42:25')",
      "NULL"
    )

    testAllApis(
      nullOf(DataTypes.TIMESTAMP(3)) + -200.hours,
      "TIMESTAMPADD(HOUR, -200, CAST(NULL AS TIMESTAMP))",
      "NULL")

    testAllApis(
      "2016-06-15".toDate + 1.day,
      "TIMESTAMPADD(DAY, 1, DATE '2016-06-15')",
      "2016-06-16")

    testAllApis(
      nullOf(DataTypes.TIMESTAMP(3)) + 3.months,
      "TIMESTAMPADD(MONTH, 3, CAST(NULL AS TIMESTAMP))",
      "NULL")

    testAllApis(
      "2016-02-24 12:42:25".toTimestamp + nullOf(Types.INTERVAL_MILLIS),
      "TIMESTAMPADD(HOUR, CAST(NULL AS INTEGER), TIMESTAMP '2016-02-24 12:42:25')",
      "NULL")

    testAllApis(
      nullOf(Types.SQL_TIMESTAMP) + -200.hours,
      "TIMESTAMPADD(HOUR, -200, CAST(NULL AS TIMESTAMP))",
      "NULL")

    testAllApis(
      nullOf(Types.SQL_TIMESTAMP) + 3.months,
      "TIMESTAMPADD(MONTH, 3, CAST(NULL AS TIMESTAMP))",
      "NULL")

    // TIMESTAMPADD with DATE returns a TIMESTAMP value for sub-day intervals.
    testAllApis(
      "2016-06-15".toDate + 1.month,
      "timestampadd(MONTH, 1, date '2016-06-15')",
      "2016-07-15")

    testAllApis(
      "2016-06-15".toDate + 1.day,
      "timestampadd(DAY, 1, date '2016-06-15')",
      "2016-06-16")

    // There is no timestamp literal function in Java String Table API,
    // toTimestamp is casting string to TIMESTAMP(3) which is not the same to timestamp literal.
    testTableApi("2016-06-15".toTimestamp - 1.hour, "2016-06-14 23:00:00.000")
    testSqlApi("timestampadd(HOUR, -1, date '2016-06-15')", "2016-06-14 23:00:00.000000")

    // There is no timestamp literal function in Java String Table API,
    // toTimestamp is casting string to TIMESTAMP(3) which is not the same to timestamp literal.
    testTableApi("2016-06-15".toTimestamp + 1.minute, "2016-06-15 00:01:00.000")
    testSqlApi("timestampadd(MINUTE, 1, date '2016-06-15')", "2016-06-15 00:01:00.000000")

    // There is no timestamp literal function in Java String Table API,
    // toTimestamp is casting string to TIMESTAMP(3) which is not the same to timestamp literal.
    testTableApi("2016-06-15".toTimestamp - 1.second, "2016-06-14 23:59:59.000")
    testSqlApi("timestampadd(SQL_TSI_SECOND, -1, date '2016-06-15')", "2016-06-14 23:59:59.000000")

    // There is no timestamp literal function in Java String Table API,
    // toTimestamp is casting string to TIMESTAMP(3) which is not the same to timestamp literal.
    testTableApi("2016-06-15".toTimestamp + 1.second, "2016-06-15 00:00:01.000")
    testSqlApi("timestampadd(SECOND, 1, date '2016-06-15')", "2016-06-15 00:00:01.000000")

    testAllApis(
      nullOf(Types.SQL_TIMESTAMP) + 1.second,
      "timestampadd(SECOND, 1, cast(null as date))",
      "NULL")

    testAllApis(
      nullOf(Types.SQL_TIMESTAMP) + 1.day,
      "timestampadd(DAY, 1, cast(null as date))",
      "NULL")

    // Round to the last day of previous month
    testAllApis(
      "2016-05-31".toDate + 1.month,
      "timestampadd(MONTH, 1, date '2016-05-31')",
      "2016-06-30")

    testAllApis(
      "2016-01-31".toDate + 5.month,
      "timestampadd(MONTH, 5, date '2016-01-31')",
      "2016-06-30")

    testAllApis(
      "2016-03-31".toDate - 1.month,
      "timestampadd(MONTH, -1, date '2016-03-31')",
      "2016-02-29")

    testAllApis(
      "2016-03-31".toDate - 1.week,
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

    // test TIMESTAMPADD with positive time interval expressed in TINYINT in various granularity.
    testSqlApi("TIMESTAMPADD(SECOND, CAST(1 AS TINYINT), time '23:59:59')", "00:00:00")
    testSqlApi("TIMESTAMPADD(MINUTE, CAST(1 AS TINYINT), time '00:00:00')", "00:01:00")
    testSqlApi("TIMESTAMPADD(MINUTE, CAST(1 AS TINYINT), time '23:59:59')", "00:00:59")
    testSqlApi("TIMESTAMPADD(HOUR, CAST(1 AS TINYINT), time '23:59:59')", "00:59:59")
    testSqlApi("TIMESTAMPADD(DAY, CAST(15 AS TINYINT), date '2021-10-30')", "2021-11-14")
    testSqlApi("TIMESTAMPADD(WEEK, CAST(3 AS TINYINT), date '2021-10-30')", "2021-11-20")
    testSqlApi("TIMESTAMPADD(MONTH, CAST(6 AS TINYINT), date '2021-10-30')", "2022-04-30")
    testSqlApi("TIMESTAMPADD(QUARTER, CAST(1 AS TINYINT), date '2021-10-30')", "2022-01-30")
    testSqlApi("TIMESTAMPADD(YEAR, CAST(10 AS TINYINT), date '2021-10-30')", "2031-10-30")

    // test TIMESTAMPADD with negative time interval expressed in TINYINT in various granularity.
    testSqlApi("TIMESTAMPADD(SECOND, CAST(-1 AS TINYINT), time '00:00:00')", "23:59:59")
    testSqlApi("TIMESTAMPADD(MINUTE, CAST(-1 AS TINYINT), time '00:00:00')", "23:59:00")
    testSqlApi("TIMESTAMPADD(MINUTE, CAST(-1 AS TINYINT), time '00:00:59')", "23:59:59")
    testSqlApi("TIMESTAMPADD(HOUR, CAST(-1 AS TINYINT), time '00:00:00')", "23:00:00")
    testSqlApi("TIMESTAMPADD(DAY, CAST(-1 AS TINYINT), date '2021-10-30')", "2021-10-29")
    testSqlApi("TIMESTAMPADD(WEEK, CAST(-1 AS TINYINT), date '2021-10-30')", "2021-10-23")
    testSqlApi("TIMESTAMPADD(MONTH, CAST(-1 AS TINYINT), date '2021-10-30')", "2021-09-30")
    testSqlApi("TIMESTAMPADD(QUARTER, CAST(-1 AS TINYINT), date '2021-10-30')", "2021-07-30")
    testSqlApi("TIMESTAMPADD(YEAR, CAST(-1 AS TINYINT), date '2021-10-30')", "2020-10-30")

    // test TIMESTAMPADD with positive time interval expressed in SMALLINT in various granularity.
    testSqlApi("TIMESTAMPADD(SECOND, CAST(1 AS SMALLINT), time '23:59:59')", "00:00:00")
    testSqlApi("TIMESTAMPADD(MINUTE, CAST(1 AS SMALLINT), time '00:00:00')", "00:01:00")
    testSqlApi("TIMESTAMPADD(MINUTE, CAST(1 AS SMALLINT), time '23:59:59')", "00:00:59")
    testSqlApi("TIMESTAMPADD(HOUR, CAST(1 AS SMALLINT), time '23:59:59')", "00:59:59")
    testSqlApi("TIMESTAMPADD(DAY, CAST(15 AS SMALLINT), date '2021-10-30')", "2021-11-14")
    testSqlApi("TIMESTAMPADD(WEEK, CAST(3 AS SMALLINT), date '2021-10-30')", "2021-11-20")
    testSqlApi("TIMESTAMPADD(MONTH, CAST(6 AS SMALLINT), date '2021-10-30')", "2022-04-30")
    testSqlApi("TIMESTAMPADD(QUARTER, CAST(1 AS SMALLINT), date '2021-10-30')", "2022-01-30")
    testSqlApi("TIMESTAMPADD(YEAR, CAST(10 AS SMALLINT), date '2021-10-30')", "2031-10-30")

    // test TIMESTAMPADD with negative time interval expressed in SMALLINT in various granularity.
    testSqlApi("TIMESTAMPADD(SECOND, CAST(-1 AS SMALLINT), time '00:00:00')", "23:59:59")
    testSqlApi("TIMESTAMPADD(MINUTE, CAST(-1 AS SMALLINT), time '00:00:00')", "23:59:00")
    testSqlApi("TIMESTAMPADD(MINUTE, CAST(-1 AS SMALLINT), time '00:00:59')", "23:59:59")
    testSqlApi("TIMESTAMPADD(HOUR, CAST(-1 AS SMALLINT), time '00:00:00')", "23:00:00")
    testSqlApi("TIMESTAMPADD(DAY, CAST(-1 AS SMALLINT), date '2021-10-30')", "2021-10-29")
    testSqlApi("TIMESTAMPADD(WEEK, CAST(-1 AS SMALLINT), date '2021-10-30')", "2021-10-23")
    testSqlApi("TIMESTAMPADD(MONTH, CAST(-1 AS SMALLINT), date '2021-10-30')", "2021-09-30")
    testSqlApi("TIMESTAMPADD(QUARTER, CAST(-1 AS SMALLINT), date '2021-10-30')", "2021-07-30")
    testSqlApi("TIMESTAMPADD(YEAR, CAST(-1 AS SMALLINT), date '2021-10-30')", "2020-10-30")
  }

  @Test
  def testToTimestamp(): Unit = {
    testSqlApi("to_timestamp('abc')", "NULL")
    testSqlApi("to_timestamp('2017-09-15 00:00:00')", "2017-09-15 00:00:00.000")
    testSqlApi("to_timestamp('20170915000000', 'yyyyMMddHHmmss')", "2017-09-15 00:00:00.000")
    testSqlApi("to_timestamp('2017-09-15', 'yyyy-MM-dd')", "2017-09-15 00:00:00.000")
    // test with null input
    testSqlApi("to_timestamp(cast(null as varchar))", "NULL")
  }

  @Test
  def testToDate(): Unit = {
    testSqlApi("to_date('2017-09-15 00:00:00')", "2017-09-15")
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

    testAllApis("test".md5(), "MD5('test')", expectedMd5)

    testAllApis("test".sha1(), "SHA1('test')", expectedSha1)

    // sha224
    testAllApis("test".sha224(), "SHA224('test')", expectedSha224)

    // sha-2 224
    testAllApis("test".sha2(224), "SHA2('test', 224)", expectedSha224)

    // sha256
    testAllApis("test".sha256(), "SHA256('test')", expectedSha256)

    // sha-2 256
    testAllApis("test".sha2(256), "SHA2('test', 256)", expectedSha256)

    // sha384
    testAllApis("test".sha384(), "SHA384('test')", expectedSha384)

    // sha-2 384
    testAllApis("test".sha2(384), "SHA2('test', 384)", expectedSha384)

    // sha512
    testAllApis("test".sha512(), "SHA512('test')", expectedSha512)

    // sha-2 512
    testAllApis("test".sha2(512), "SHA2('test', 512)", expectedSha512)

    // null tests
    testAllApis('f33.md5(), "MD5(f33)", "NULL")

    testAllApis('f33.sha1(), "SHA1(f33)", "NULL")

    testAllApis('f33.sha224(), "SHA2(f33, 224)", "NULL")

    testAllApis('f33.sha2(224), "SHA2(f33, 224)", "NULL")

    testAllApis('f33.sha256(), "SHA2(f33, 256)", "NULL")

    testAllApis('f33.sha384(), "SHA2(f33, 384)", "NULL")

    testAllApis('f33.sha512(), "SHA2(f33, 512)", "NULL")

    testAllApis("test".sha2(nullOf(DataTypes.INT)), "SHA2('test', CAST(NULL AS INT))", "NULL")

    // non-constant bit length
    testAllApis("test".sha2('f44), "SHA2('test', f44)", expectedSha256)

    // null test
    testSqlApi("MD5(cast(null as varbinary))", "NULL")
    testSqlApi("SHA1(cast(null as varbinary))", "NULL")
    testSqlApi("SHA224(cast(null as varbinary))", "NULL")
    testSqlApi("SHA2(cast(null as varbinary), 224)", "NULL")
    testSqlApi("SHA256(cast(null as varbinary))", "NULL")
    testSqlApi("SHA2(cast(null as varbinary), 256)", "NULL")
    testSqlApi("SHA384(cast(null as varbinary))", "NULL")
    testSqlApi("SHA2(cast(null as varbinary), 384)", "NULL")
    testSqlApi("SHA512(cast(null as varbinary))", "NULL")
    testSqlApi("SHA2(cast(null as varbinary), 512)", "NULL")
  }

  // ----------------------------------------------------------------------------------------------
  // Other functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testIsTrueIsFalse(): Unit = {
    testAllApis('f1.isTrue, "f1 IS TRUE", "TRUE")

    testAllApis('f21.isTrue, "f21 IS TRUE", "FALSE")

    testAllApis(false.isFalse, "FALSE IS FALSE", "TRUE")

    testAllApis('f21.isFalse, "f21 IS FALSE", "FALSE")

    testAllApis('f1.isNotTrue, "f1 IS NOT TRUE", "FALSE")

    testAllApis('f21.isNotTrue, "f21 IS NOT TRUE", "TRUE")

    testAllApis(false.isNotFalse, "FALSE IS NOT FALSE", "FALSE")

    testAllApis('f21.isNotFalse, "f21 IS NOT FALSE", "TRUE")
  }

  @Test
  def testStringFunctionsWithNull(): Unit = {
    val functions = List(
      ($"f40".substring(2), "SUBSTRING(f40, 2)"),
      ($"f40".trim(), "TRIM(f40)"),
      ($"f40".like("%%link"), "f40 LIKE '%%link'"),
      ($"f40".charLength(), "CHAR_LENGTH(f40)"),
      ($"f40".lowerCase(), "LOWER(f40)"),
      ($"f40".upperCase(), "UPPER(f40)"),
      ($"f40".initCap(), "INITCAP(f40)"),
      ($"f40".position("aa"), "POSITION('aa' IN f40)"),
      ($"f40".overlay("aa", 2), "OVERLAY(f40 PLACING 'aa' FROM 2 FOR 2)")
    )

    functions.foreach(
      x => {
        testAllApis(
          x._1,
          x._2,
          "NULL"
        )
      })
  }

  @Test
  def testCodeGenNPE(): Unit = {
    val fields = Seq("f0", "f40", "''")

    // case 1: non-null field argument, null result,
    // case 2: null field argument, null result
    // case 3: constant argument, null result
    val cases = List(
      ($"f0".substring(1, -1), "UPPER(SUBSTRING(f0, 1, -1))"),
      ($"f40".substring(1, -1), "UPPER(SUBSTRING(f40, 1, -1))"),
      (lit("").substring(1, -1), "UPPER(SUBSTRING('', 1, -1))")
    )

    cases.foreach(
      x => {
        testAllApis(
          x._1,
          x._2,
          "NULL"
        )
      })
  }

  @Test
  def testNullBigDecimal(): Unit = {
    testAllApis('f41.sign(), "SIGN(f41)", "NULL")
  }

  @Test
  def testEncodeAndDecode(): Unit = {
    testAllApis(
      "aabbef".encode("UTF-16LE").decode("UTF-16LE"),
      "decode(encode('aabbef', 'UTF-16LE'), 'UTF-16LE')",
      "aabbef")

    testSqlApi("decode(encode('aabbef', 'utf-8'), 'utf-8')", "aabbef")

    testSqlApi("decode(encode('', 'utf-8'), 'utf-8')", "")

    testSqlApi(
      "encode(cast (null AS VARCHAR), 'utf-8')",
      "NULL"
    )

    testSqlApi(
      "encode(cast (null AS VARCHAR), cast (null AS VARCHAR))",
      "NULL"
    )

    testSqlApi(
      "encode('aabbef', cast (null AS VARCHAR))",
      "NULL"
    )

    testSqlApi(
      "decode(cast (null AS BINARY), 'utf-8')",
      "NULL"
    )

    testSqlApi(
      "decode(cast (null AS BINARY), cast (null AS VARCHAR))",
      "NULL"
    )

    testSqlApi(
      "decode(encode('aabbef', 'utf-8'), cast (null AS VARCHAR))",
      "NULL"
    )

    testSqlApi("decode(encode('中国', 'UTF-16LE'), 'UTF-16LE')", "中国")

    testSqlApi(
      "decode(encode('val_238', 'US-ASCII'), 'US-ASCII') =" +
        " decode(encode('val_238', 'utf-8'), 'utf-8')",
      "TRUE")
  }

  @Test
  def testStringToMap(): Unit = {
    testAllApis("k1=v1,k2=v2".strToMap(), "STR_TO_MAP('k1=v1,k2=v2')", "{k1=v1, k2=v2}")
    testAllApis(
      "k1:v1;k2: v2".strToMap(";", ":"),
      "STR_TO_MAP('k1:v1;k2: v2', ';', ':')",
      "{k1=v1, k2= v2}")
    testSqlApi("STR_TO_MAP('k1$$v1|k2$$ v2', '\\|', '\\$\\$')", "{k1=v1, k2= v2}")

    // test empty
    testSqlApi("STR_TO_MAP('')", "{}")

    // test key access
    testSqlApi("STR_TO_MAP('k1=v1,k2=v2')['k1']", "v1")
    testSqlApi("STR_TO_MAP('k1:v1;k2:v2', ';', ':')['k2']", "v2")

    // test non-exist key access
    testSqlApi("STR_TO_MAP('k1=v1,k2=v2')['k3']", "NULL")

    testSqlApi("STR_TO_MAP(f46)", "{test1=1, test2=2, test3=3}")

    testSqlApi("STR_TO_MAP(f47)", "NULL")
  }

  @Test
  def testIf(): Unit = {
    // test IF(BOOL, INT, BIGINT), will do implicit type coercion.
    testSqlApi("IF(f7 > 5, f14, f4)", "44")
    // test input with null
    testSqlApi("IF(f7 < 5, cast(null as int), f4)", "NULL")

    // f0 is a STRING, cast(f0 as double) should never be ran
    testSqlApi("IF(1 = 1, f6, cast(f0 as double))", "4.6")

    // test STRING, STRING
    testSqlApi("IF(f7 > 5, f0, f8)", " This is a test String. ")

    // test BYTE, BYTE
    testSqlApi("IF(f7 < 5, f2, f9)", "42")

    // test INT, INT
    testSqlApi("IF(f7 < 5, f14, f7)", "-3")

    // test SHORT, SHORT
    testSqlApi("IF(f7 < 5, f3, f10)", "43")

    // test Long, Long
    testSqlApi("IF(f7 < 5, f4, f11)", "44")

    // test Double, Double
    testSqlApi("IF(f7 < 5, f6, f13)", "4.6")

    // test BOOL, BOOL
    testSqlApi("IF(f7 < 5, f1, f48)", "TRUE")

    // test DECIMAL, DECIMAL
    testSqlApi("IF(f7 < 5, f15, f49)", "-1231.1231231321321321111")

    // test BINARY, BINARY
    // the answer BINARY will cast to STRING in ExpressionTestBase.scala
    testSqlApi("IF(f7 < 5, f53, f54)", "hello world")

    // test DATE, DATE
    testSqlApi("IF(f7 < 5, f16, f50)", "1996-11-10")

    // test TIME, TIME
    testSqlApi("IF(f7 < 5, f17, f51)", "06:55:44")

    // test TIMESTAMP, TIMESTAMP
    // temporarily disabled until FLIP-162 is ready
    //    testSqlApi(
    //      "IF(f7 < 5, f18, f52)",
    //      "1996-11-10 06:55:44.333")
  }

  @Test
  def testIfDecimal(): Unit = {
    // test DECIMAL, DECIMAL
    testAllApis(ifThenElse('f7 < 5, 'f31, 'f34), "IF(f7 < 5, f31, f34)", "-0.1231231321321321111")
  }

  @Test
  def testIsDecimal(): Unit = {
    testSqlApi("IS_DECIMAL('1')", "TRUE")

    testSqlApi("IS_DECIMAL('123')", "TRUE")

    testSqlApi("IS_DECIMAL('2')", "TRUE")

    testSqlApi("IS_DECIMAL('11.4445')", "TRUE")

    testSqlApi("IS_DECIMAL('3')", "TRUE")

    testSqlApi("IS_DECIMAL('abc')", "FALSE")

    // test null string field
    testSqlApi("IS_DECIMAL(f33)", "FALSE")
  }

  @Test
  def testIsDigit(): Unit = {
    testSqlApi("IS_DIGIT('1')", "TRUE")

    testSqlApi("IS_DIGIT('123')", "TRUE")

    testSqlApi("IS_DIGIT('2')", "TRUE")

    testSqlApi("IS_DIGIT('11.4445')", "FALSE")

    testSqlApi("IS_DIGIT('3')", "TRUE")

    testSqlApi("IS_DIGIT('abc')", "FALSE")

    // test null string field
    testSqlApi("IS_DIGIT(f33)", "FALSE")
  }

  @Test
  def testIsAlpha(): Unit = {
    testSqlApi("IS_ALPHA('1')", "FALSE")

    testSqlApi("IS_ALPHA('123')", "FALSE")

    testSqlApi("IS_ALPHA('2')", "FALSE")

    testSqlApi("IS_ALPHA('11.4445')", "FALSE")

    testSqlApi("IS_ALPHA('3')", "FALSE")

    testSqlApi("IS_ALPHA('abc')", "TRUE")

    // test null string field
    testSqlApi("IS_ALPHA(f33)", "FALSE")
  }

  @Test
  def testRawTypeEquality(): Unit = {
    testSqlApi("f55=f56", "FALSE")

    testSqlApi("f55=f57", "TRUE")
  }

  @Test
  def testStringFunctionAndExpressionResultType(): Unit = {
    // this test is to check if the `resultType` of the `GeneratedExpression`
    // of these string functions match their definition in `FlinkSqlOperatorTable`,
    // if not exceptions will be thrown from BridgingFunctionGenUtil#verifyArgumentTypes

    val str1 = "CAST('Hello' AS VARCHAR(5))"
    val str2 = "CAST('Hi' AS VARCHAR(2))"
    val str3 = "CAST('hello world' AS VARCHAR(11))"
    val str4 = "CAST(' hello ' AS VARCHAR(7))"
    val url = "CAST('http://user:pass@host' AS VARCHAR(50))"
    val base64 = "CAST('aGVsbG8gd29ybGQ=' AS VARCHAR(20))"

    testSqlApi(s"IFNULL(SUBSTR($str1, 2, 3), $str2)", "el")
    testSqlApi(s"IFNULL(SUBSTRING($str1, 2, 3), $str2)", "el")
    testSqlApi(s"IFNULL(LEFT($str1, 3), $str2)", "He")
    testSqlApi(s"IFNULL(RIGHT($str1, 3), $str2)", "ll")
    testSqlApi(s"IFNULL(REGEXP_EXTRACT($str1, 'H(.+?)l(.+?)$$', 2), $str2)", "lo")
    testSqlApi(s"IFNULL(REGEXP_REPLACE($str1, 'e.l', 'EXL'), $str2)", "HEXLo")
    testSqlApi(s"IFNULL(UPPER($str1), $str2)", "HELLO")
    testSqlApi(s"IFNULL(LOWER($str1), $str2)", "hello")
    testSqlApi(s"IFNULL(INITCAP($str3), $str2)", "Hello World")
    testSqlApi(s"IFNULL(OVERLAY($str1 PLACING $str3 FROM 2 FOR 3), $str2)", "Hhello worldo")
    testSqlApi(s"IFNULL(LPAD($str1, 7, $str3), $str2)", "heHello")
    testSqlApi(s"IFNULL(RPAD($str1, 7, $str3), $str2)", "Hellohe")
    testSqlApi(s"IFNULL(REPEAT($str1, 2), $str2)", "HelloHello")
    testSqlApi(s"IFNULL(REVERSE($str1), $str2)", "ol")
    testSqlApi(s"IFNULL(REPLACE($str3, ' ', '_'), $str2)", "hello_world")
    testSqlApi(s"IFNULL(SPLIT_INDEX($str3, ' ', 1), $str2)", "wo")
    testSqlApi(s"IFNULL(MD5($str1), $str2)", "8b1a9953c4611296a827abf8c47804d7")
    testSqlApi(s"IFNULL(SHA1($str1), $str2)", "f7ff9e8b7bb2e09b70935a5d785e0cc5d9d0abf0")
    testSqlApi(
      s"IFNULL(SHA224($str1), $str2)",
      "4149da18aa8bfc2b1e382c6c26556d01a92c261b6436dad5e3be3fcc")
    testSqlApi(
      s"IFNULL(SHA256($str1), $str2)",
      "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969")
    testSqlApi(
      s"IFNULL(SHA384($str1), $str2)",
      "3519fe5ad2c596efe3e276a6f351b8fc0b03db861782490d45" +
        "f7598ebd0ab5fd5520ed102f38c4a5ec834e98668035fc")
    testSqlApi(
      s"IFNULL(SHA512($str1), $str2)",
      "3615f80c9d293ed7402687f94b22d58e529b8cc7916f8fac7fddf7fbd5af4cf777d" +
        "3d795a7a00a16bf7e7f3fb9561ee9baae480da9fe7a18769e71886b03f315"
    )
    testSqlApi(
      s"IFNULL(SHA2($str1, 256), $str2)",
      "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969")
    testSqlApi(s"IFNULL(PARSE_URL($url, 'HOST'), $str2)", "ho")
    testSqlApi(s"IFNULL(FROM_BASE64($base64), $str2)", "hello world")
    testSqlApi(s"IFNULL(TO_BASE64($str3), $str2)", "aGVsbG8gd29ybGQ=")
    testSqlApi(s"IFNULL(CHR(65), $str2)", "A")
    testSqlApi(s"IFNULL(BIN(10), $str2)", "1010")
    testSqlApi(s"IFNULL(CONCAT($str1, $str2), $str2)", "HelloHi")
    testSqlApi(s"IFNULL(CONCAT_WS('~', $str1, $str2), $str2)", "Hello~Hi")
    testSqlApi(s"IFNULL(TRIM($str4), $str2)", "hello")
    testSqlApi(s"IFNULL(LTRIM($str4), $str2)", "hello ")
    testSqlApi(s"IFNULL(RTRIM($str4), $str2)", " hello")
    testSqlApi(s"IFNULL($str1 || $str2, $str2)", "HelloHi")
    testSqlApi(s"IFNULL(SUBSTRING(UUID(), 9, 1), $str2)", "-")
    testSqlApi(s"IFNULL(DECODE(ENCODE($str1, 'utf-8'), 'utf-8'), $str2)", "He")

    testSqlApi(s"IFNULL(CAST(DATE '2021-04-06' AS VARCHAR(10)), $str2)", "2021-04-06")
    testSqlApi(s"IFNULL(CAST(TIME '11:05:30' AS VARCHAR(8)), $str2)", "11:05:30")
    testSqlApi(
      s"IFNULL(CAST(TIMESTAMP '2021-04-06 11:05:30' AS VARCHAR(19)), $str2)",
      "2021-04-06 11:05:30")
    testSqlApi(s"IFNULL(CAST(INTERVAL '2' YEAR AS VARCHAR(20)), $str2)", "+2-00")
    testSqlApi(s"IFNULL(CAST(INTERVAL '2' DAY AS VARCHAR(20)), $str2)", "+2 00:00:00.000")
    testSqlApi(s"IFNULL(CAST(f53 AS VARCHAR(100)), $str2)", "hello world")
  }
}
