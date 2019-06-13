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

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils.ScalarTypesTestBase

import org.junit.{Ignore, Test}

import java.util.TimeZone

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
      testAllApis(
        ExpressionParser.parseExpression(x._1),
        x._1,
        x._2,
        x._3
      )
    })

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

  // TODO
  @Test
  def testLocate(): Unit = {
    //    testAllApis(
    //      "test".locate("xxxtest"),
    //      "'test'.locate('xxxtest')",
    //      "locate('test', 'xxxtest')",
    //      "4")
    //
    //    testAllApis(
    //      "testx".locate("xxxtest"),
    //      "'testx'.locate('xxxtest')",
    //      "locate('testx', 'xxxtest')",
    //      "0")

    testSqlApi("locate('aa',  'aaads')", "1")

    testSqlApi("locate('aa', 'aaads', 2)", "2")
  }

  //TODO
  @Test
  def testLeft(): Unit = {
    //    testAllApis(
    //      'f0.left(2),
    //      "f0.left(2)",
    //      "`LEFT`(f0, 2)",
    //      "Th")
    //
    //    testAllApis(
    //      'f0.left(100),
    //      "f0.left(100)",
    //      "`LEFT`(f0, 100)",
    //      "This is a test String.")
    //
    //    testAllApis(
    //      'f0.left(-2),
    //      "f0.left(-2)",
    //      "`LEFT`(f0, -2)",
    //      "")
    //
    //    testAllApis(
    //      'f0.left(0),
    //      "f0.left(0)",
    //      "`LEFT`(f0, 0)",
    //      "")

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

  //TODO
  @Test
  def testRight(): Unit = {
    //    testAllApis(
    //      'f0.right(2),
    //      "f0.right(2)",
    //      "`right`(f0, 2)",
    //      "g.")
    //
    //    testAllApis(
    //      'f0.right(100),
    //      "f0.right(100)",
    //      "`right`(f0, 100)",
    //      "This is a test String.")
    //
    //    testAllApis(
    //      'f0.right(-2),
    //      "f0.right(-2)",
    //      "`right`(f0, -2)",
    //      "")
    //
    //    testAllApis(
    //      'f0.right(0),
    //      "f0.right(0)",
    //      "`right`(f0, 0)",
    //      "")

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

  //TODO
  @Test
  def testAscii(): Unit = {
    //    testAllApis(
    //      "efg".ascii(),
    //      "'efg'.ascii()",
    //      "ascii('efg')",
    //      "101")
    //
    //    testAllApis(
    //      "abcdef".ascii(),
    //      "'abcdef'.ascii()",
    //      "ascii('abcdef')",
    //      "97")
    //
    //    testAllApis(
    //      "".ascii(),
    //      "''.ascii()",
    //      "ascii('')",
    //      "0")

    testSqlApi(
      "ascii(cast (null AS VARCHAR))",
      "null"
    )
  }

  @Test
  def testInstr(): Unit = {
    // TODO
    //    testAllApis(
    //      "Corporate Floor".instr("or", 3, 2),
    //      "'Corporate Floor'.instr('or', 3, 2)",
    //      "instr('Corporate Floor', 'or', 3, 2)",
    //      "14")
    //
    //    testAllApis(
    //      "Corporate Floor".instr("or", 3, 0),
    //      "'Corporate Floor'.instr('or', 3, 0)",
    //      "instr('Corporate Floor', 'or', 3, 0)",
    //      "0")
    //
    //    testAllApis(
    //      "Corporate Floor".instr("or", -3, 2),
    //      "'Corporate Floor'.instr('or', -3, 2)",
    //      "instr('Corporate Floor', 'or', -3, 2)",
    //      "2")
    //
    //    testAllApis(
    //      "Tech on the net".instr("e"),
    //      "'Tech on the net'.instr('e')",
    //      "instr('Tech on the net', 'e')",
    //      "2")
    //
    //    testAllApis(
    //      "Tech on the net".instr("e", 1, 2),
    //      "'Tech on the net'.instr('e', 1, 2)",
    //      "instr('Tech on the net', 'e', 1, 2)",
    //      "11")
    //
    //    testAllApis(
    //      "Tech on the net".instr("e", 1, 3),
    //      "'Tech on the net'.instr('e', 1, 3)",
    //      "instr('Tech on the net', 'e', 1, 3)",
    //      "14")
    //
    //    testAllApis(
    //      "Tech on the net".instr("e", -3, 2),
    //      "'Tech on the net'.instr('e', -3, 2)",
    //      "instr('Tech on the net', 'e', -3, 2)",
    //      "2")
    //
    //    testAllApis(
    //      "myteststring".instr("st"),
    //      "'myteststring'.instr('st')",
    //      "instr('myteststring', 'st')",
    //      "5")

    testSqlApi(
      "instr(cast (null AS VARCHAR), 'e')",
      "null"
    )

    testSqlApi(
      "instr('e', cast (null AS VARCHAR))",
      "null"
    )
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
      'f0.substring(1.cast(DataTypes.TINYINT), 'f7),
      "f0.substring(1.cast(BYTE), f7)",
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
      'f0.replace(nullOf(DataTypes.STRING), ""),
      "f0.replace(Null(STRING), '')",
      "REPLACE(f0, NULLIF('', ''), '')",
      "null")

    testAllApis(
      'f0.replace(" ", nullOf(DataTypes.STRING)),
      "f0.replace(' ', Null(STRING))",
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
  }

  @Test
  def testLTrim(): Unit = {
    testAllApis(
      'f8.ltrim(),
      "f8.ltrim()",
      "LTRIM(f8)",
      "This is a test String. ")

    // TODO
    //    testAllApis(
    //      'f8.ltrim(" "),
    //      "ltrim(f8)",
    //      "LTRIM(f8)",
    //      "This is a test String. ")
    //
    //    testAllApis(
    //      'f0.ltrim("This "),
    //      "ltrim(f0, 'This ')",
    //      "LTRIM(f0, 'This ')",
    //      "a test String.")
    //
    //    testAllApis(
    //      "abcddcba".ltrim("abc"),
    //      "'abcddcba'.ltrim('abc')",
    //      "ltrim('abcddcba', 'abc')",
    //      "ddcba")
    //
    //    testAllApis(
    //      "abcddcba".ltrim("abd"),
    //      "'abcddcba'.ltrim('abd')",
    //      "LTRIM('abcddcba', 'abd')",
    //      "cddcba")

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
    testAllApis(
      'f8.rtrim(),
      "f8.rtrim()",
      "rtrim(f8)",
      " This is a test String.")

    //TODO
    //    testAllApis(
    //      'f8.rtrim(" "),
    //      "rtrim(f8)",
    //      "rtrim(f8)",
    //      " This is a test String.")
    //
    //    testAllApis(
    //      'f0.rtrim("String. "),
    //      "rtrim(f0, 'String. ')",
    //      "rtrim(f0, 'String. ')",
    //      "This is a tes")
    //
    //    testAllApis(
    //      "abcddcba".rtrim("abc"),
    //      "'abcddcba'.rtrim('abc')",
    //      "rtrim('abcddcba', 'abc')",
    //      "abcdd")
    //
    //    testAllApis(
    //      "abcddcba".rtrim("abd"),
    //      "'abcddcba'.rtrim('abd')",
    //      "rtrim('abcddcba', 'abd')",
    //      "abcddc")

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

  //TODO
  @Test
  def testLength(): Unit = {
    //    testAllApis(
    //      'f0.length(),
    //      "f0.length()",
    //      "LENGTH(f0)",
    //      "22")
    //
    //    testAllApis(
    //      'f0.length(),
    //      "length(f0)",
    //      "LENGTH(f0)",
    //      "22")
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

  @Ignore // TODO
  @Test
  def testMultiConcat(): Unit = {
    testAllApis(
      concat("xx", 'f33),
      "concat('xx', f33)",
      "CONCAT('xx', f33)",
      "xx")
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
      "AA")
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

  /// string tests borrowed from spark

  @Ignore // TODO
  @Test
  def testConcat2(): Unit = {
    testSqlApi("concat(f35)", "a")
    testSqlApi("concat(f1,f2,f3,f4,f5,f16,f17,f18,f33,f43)",
      "true4243444.51996-11-1006:55:441996-11-10 06:55:44.333-1")
    testSqlApi("concat(f35,f36,f33,f1)", "abtrue") // note: Spark expects the result to be `null`
  }

  @Test
  def testConcatWs2(): Unit = {
    testSqlApi("concat_ws('||', f35, f36, f33)", "a||b")
  }

  @Ignore // TODO
  @Test
  def testRegex(): Unit = {
    testSqlApi("regexp_replace('100-200', '(\\\\d+)', 'num')", "num-num")
    testSqlApi("regexp_replace('100-200', '(\\\\d+)-(\\\\d+)', '400')", "400")
    testSqlApi("regexp_replace('100-200', '(\\\\d+)', '400')", "400-400")
    testSqlApi("regexp_replace('100-200', '', '400')", "100-200")
    testSqlApi("regexp_replace(f40, '(\\\\d+)', '400')", "null")
    testSqlApi("regexp_replace(CAST(null as VARCHAR), '(\\\\d+)', 'num')", "null")
    testSqlApi("regexp_replace('100-200', CAST(null as VARCHAR), '400')", "null")
    testSqlApi("regexp_replace('100-200', '(\\\\d+)', CAST(null as VARCHAR))", "null")

    testSqlApi("regexp_extract('100-200', '(\\\\d+)-(\\\\d+)', 1)", "100")
    testSqlApi("regexp_extract('100-200', '', 1)", "null")
    testSqlApi("regexp_extract('100-200', '(\\\\d+)-(\\\\d+)', -1)", "null")
    testSqlApi("regexp_extract(f40, '(\\\\d+)-(\\\\d+)', 1)", "null")
    testSqlApi("regexp_extract(CAST(null as VARCHAR), '(\\\\d+)-(\\\\d+)', 1)", "null")
    testSqlApi("regexp_extract('100-200', CAST(null as VARCHAR), 1)", "null")
    testSqlApi("regexp_extract('100-200', '(\\\\d+)-(\\\\d+)', CAST(null as BIGINT))", "null")
  }

  @Test
  def testBase64(): Unit = {
    val v4 = testData.getField(38).toString
    testSqlApi("to_base64(f37)", v4)
    testSqlApi("to_base64(from_base64(f38))", v4)
  }

  @Test
  def testSubstring2(): Unit = {
    testSqlApi("substring(f39, 1, 2)", "1世")
  }

  @Test
  def testTrim2(): Unit = {

    testSqlApi("trim(LEADING  from '  example  ')", "example  ")
    testSqlApi("trim(TRAILING from '  example  ')", "  example")
    testSqlApi("trim(BOTH     from '  example  ')", "example")

    testSqlApi("trim(LEADING  'e' from 'example')", "xample")
    testSqlApi("trim(TRAILING 'e' from 'example')", "exampl")
    testSqlApi("trim(BOTH     'e' from 'example')", "xampl")

    testSqlApi("trim(BOTH     'xyz' from 'example')", "example")

    // scalastyle:off line.size.limit
    // todo: semantics if multiple chars are candidate for removal.
    // calcite uses only the 1st char (against the spec on its own website)
    // calcite's response: http://mail-archives.apache.org/mod_mbox/calcite-dev/201712.mbox/%3c72EFEFCF-1101-496B-BB07-462F9A4C2633@apache.org%3e
    // scalastyle:on line.size.limit
    //
    // the following tests are from spark.
    //testSqlApi("trim(LEADING  'xe' from 'example')",  "ample")
    //testSqlApi("trim(TRAILING 'emlp' from 'example')", "exa")
    //testSqlApi("trim(BOTH     'elxp' from 'example')",  "am")
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
    }
  }

  @Test
  def testPosition2(): Unit = {

    // NOTE: Spark names this function as instr() , i.e. "in string"
    testSqlApi("position('aa' in 'aaads')", "1")

    // NOTE: Spark names this as locate()
    //testSqlApi("position('aa' in 'aaads' from 2)", "2")
    // todo: that does not work in blink.
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

    testAllApis(
      "äää".lpad(13, "12345"),
      "'äää'.lpad(13, '12345')",
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

    testAllApis(
      "äää".rpad(13, "12345"),
      "'äää'.rpad(13, '12345')",
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
      "http://userinfo@spark.apache.org/path?query=1#Ref",
      "spark.apache.org", "/path", "query=1", "Ref",
      "http", "/path?query=1", "userinfo@spark.apache.org", "userinfo", "1")

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
      "inva lid://user:pass@host/file;param?query;p2",
      "null", "null", "null", "null", "null", "null", "null", "null", "null")
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

  @Test
  def testReverse(): Unit = {
    testSqlApi("reverse(f38)", "==ABDIQA")
    testSqlApi("reverse(f40)", "null")
    testSqlApi("reverse('hi')", "ih")
    testSqlApi("reverse('hhhi')", "ihhh")
    testSqlApi("reverse(CAST(null as VARCHAR))", "null")
  }

  @Ignore // TODO
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
      "REGEXP_EXTRACT('foothebar', 'foo([\\\\w]+)', 1)",
      "thebar")

    testAllApis(
      "foothebar".regexpExtract("foo([\\d]+)", 1),
      "'foothebar'.regexpExtract('foo([\\d]+)', 1)",
      "REGEXP_EXTRACT('foothebar', 'foo([\\\\d]+)', 1)",
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
  def testInitCap2(): Unit = {
    testSqlApi("initCap('ab')", "Ab")
    testSqlApi("initCap('a B')", "A B")
    testSqlApi("initCap('bLinK')", "Blink")
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

  @Ignore //TODO
  @Test
  def testRegexp(): Unit = {
    testSqlApi("regexp('100-200', '(\\\\d+)')", "true")
    testSqlApi("regexp('abc-def', '(\\\\d+)')", "false")
    testSqlApi("regexp(f35, 'a')", "true")
    testSqlApi("regexp(f40, '(\\\\d+)')", "null")
    testSqlApi("regexp(CAST(null as VARCHAR), '(\\\\d+)')", "null")
    testSqlApi("regexp('100-200', CAST(null as VARCHAR))", "null")
  }

  @Ignore // TODO
  @Test
  def testJsonValue(): Unit = {
    testSqlApi("json_value('[10, 20, [30, 40]]', '$[2][*]')", "[30,40]")
    testSqlApi("json_value('[10, 20, [30, [40, 50, 60]]]', '$[2][*][1][*]')", "[30,[40,50,60]]")
    testSqlApi("json_value(f40, '$[2][*][1][*]')", "null")
    testSqlApi("json_value('[10, 20, [30, [40, 50, 60]]]', '')", "null")
    testSqlApi("json_value('', '$[2][*][1][*]')", "null")
    testSqlApi("json_value(CAST(null as VARCHAR), '$[2][*][1][*]')", "null")
    testSqlApi("json_value('[10, 20, [30, [40, 50, 60]]]', CAST(null as VARCHAR))", "null")
  }

  // TODO
  @Test
  def testConv(): Unit = {
    //    testAllApis(
    //      'f4.conv(10, 16),
    //      "f4.conv(10, 16)",
    //      "CONV(f4, 10, 16)",
    //      "2C")
    //
    //    testAllApis(
    //      100.conv(10, -8),
    //      "100.conv(10, -8)",
    //      "CONV(100, 10, -8)",
    //      "144")
    //
    //    testAllApis(
    //      'f33.conv(10, 8),
    //      "f33.conv(10, 8)",
    //      "CONV(f33, 10, 8)",
    //      "null")
    //
    //    testAllApis(
    //      'f3.conv(10, 16),
    //      "f3.conv(10, 16)",
    //      "CONV(f3, 10, 16)",
    //      "2B")
    //
    //    // fromBase cannot be negative.
    //    testAllApis(
    //      'f3.conv(-10, 16),
    //      "f3.conv(-10, 16)",
    //      "CONV(f3, -10, 16)",
    //      "null")
    //
    //    testAllApis(
    //      'f46.conv(2, 10),
    //      "f46.conv(2, 10)",
    //      "CONV(f46, 2, 10)",
    //      "15")
    //
    //    testAllApis(
    //      'f0.conv(10, 16),
    //      "f0.conv(10, 16)",
    //      "CONV(f0, 10, 16)",
    //      "null")
    //
    //    testAllApis(
    //      'f32.conv(10, 16),
    //      "f32.conv(10, 16)",
    //      "CONV(f32, 10, 16)",
    //      "FFFFFFFFFFFFFFFF")
    //
    //    testAllApis(
    //      'f32.conv(10, -16),
    //      "f32.conv(10, -16)",
    //      "CONV(f32, 10, -16)",
    //      "-1")
    //
    //    testAllApis(
    //      "facebook".conv(36, 16),
    //      "'facebook'.conv(36, 16)",
    //      "CONV('facebook', 36, 16)",
    //      "116ED2B2FB4")
    //
    //    // 2^64 - 1
    //    testAllApis(
    //      "18446744073709551615".conv(10, 10),
    //      "'18446744073709551615'.conv(10, 10)",
    //      "CONV('18446744073709551615', 10, 10)",
    //      "18446744073709551615")
    //
    //    // 2^64 - 1
    //    testAllApis(
    //      "18446744073709551615".conv(10, -10),
    //      "'18446744073709551615'.conv(10, -10)",
    //      "CONV('18446744073709551615', 10, -10)",
    //      "-1")
    //
    //    // overflow, return maximum (2^64 - 1), which is consistent with Mysql.
    //    testAllApis(
    //      "18446744073709551615".conv(10, 10),
    //      "'38446744073709551615'.conv(10, 10)",
    //      "CONV('38446744073709551615', 10, 10)",
    //      "18446744073709551615")
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
      nullOf(DataTypes.TINYINT).hex(),
      "hex(Null(BYTE))",
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
  }

  @Test
  def testBin(): Unit = {
    testAllApis(
      nullOf(DataTypes.TINYINT).bin(),
      "bin(Null(BYTE))",
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

  @Ignore // TODO
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
      "regexp_replace('foobar', '\\\\d', '')",
      "foobar")

    testAllApis(
      "foobar".regexpReplace("\\w", ""),
      "'foobar'.regexpReplace('\\w', '')",
      "regexp_replace('foobar', '\\\\w', '')",
      "")

    testAllApis(
      "fooobar".regexpReplace("oo", "\\$"),
      "'fooobar'.regexpReplace('oo', '\\$')",
      "regexp_replace('fooobar', 'oo', '\\\\$')",
      "f$obar")

    testAllApis(
      "foobar".regexpReplace("oo", "\\\\"),
      "'foobar'.regexpReplace('oo', '\\\\')",
      "regexp_replace('foobar', 'oo', '\\\\\\\\')",
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

  // ----------------------------------------------------------------------------------------------
  // Math functions
  // ----------------------------------------------------------------------------------------------
  @Test
  def testAdd(): Unit = {

    testAllApis(
      1514356320000L + 6000,
      "1514356320000L + 6000",
      "1514356320000 + 6000",
      "1514356326000")

    testAllApis(
      'f34 + 6,
      "f34 + 6",
      "f34 + 6",
      "1514356320006")

    testAllApis(
      'f34 + 'f34,
      "f34 + f34",
      "f34 + f34",
      "3028712640000")
  }

  @Test
  def testSubtract(): Unit = {

    testAllApis(
      1514356320000L - 6000,
      "1514356320000L - 6000",
      "1514356320000 - 6000",
      "1514356314000")

    testAllApis(
      'f34 - 6,
      "f34 - 6",
      "f34 - 6",
      "1514356319994")

    testAllApis(
      'f34 - 'f34,
      "f34 - f34",
      "f34 - f34",
      "0")
  }

  @Test
  def testMultiply(): Unit = {

    testAllApis(
      1514356320000L * 60000,
      "1514356320000L * 60000",
      "1514356320000 * 60000",
      "90861379200000000")

    testAllApis(
      'f34 * 6,
      "f34 * 6",
      "f34 * 6",
      "9086137920000")


    testAllApis(
      'f34 * 'f34,
      "f34 * f34",
      "f34 * f34",
      "2293275063923942400000000")

  }

  @Test
  def testDivide(): Unit = {

    testAllApis(
      1514356320000L / 60000.0, // the `/` is Scala operator, not Flink TableApi operator
      "1514356320000L / 60000",
      "1514356320000 / 60000",
      "2.5239272E7")

    testAllApis(
      'f7 / 2,
      "f7 / 2",
      "f7 / 2",
      "1.5")

    // f34 => Decimal(19,0)
    // 6 => Integer => Decimal(10,0)
    // Decimal(19,0) / Decimal(10,0) => Decimal(30,11)
    testAllApis(
      'f34 / 6,
      "f34 / 6",
      "f34 / 6",
      "252392720000.00000000000")

    // Decimal(19,0) / Decimal(19,0) => Decimal(39,20) => Decimal(38,19)
    testAllApis(
      'f34 / 'f34,
      "f34 / f34",
      "f34 / f34",
      "1.0000000000000000000")
  }

  @Test
  def testMod(): Unit = {

    testAllApis(
      1514356320000L % 60000,
      "1514356320000L % 60000",
      "mod(1514356320000,60000)",
      "0")

    testAllApis(
      'f34.mod('f34),
      "f34.mod(f34)",
      "mod(f34,f34)",
      "0")

    testAllApis(
      'f34.mod(6),
      "f34.mod(6)",
      "mod(f34,6)",
      "0")

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
      'f22.cast(DataTypes.DOUBLE).power('f5),
      "f22.cast(DOUBLE).power(f5)",
      "power(CAST(f22 AS DOUBLE), f5)",
      math.pow(2, 4.5F).toString)

    testAllApis(
      'f22.cast(DataTypes.DOUBLE).power('f6),
      "f22.cast(DOUBLE).power(f6)",
      "power(CAST(f22 AS DOUBLE), f6)",
      math.pow(2, 4.6D).toString)

    testAllApis(
      'f22.cast(DataTypes.DOUBLE).power('f7),
      "f22.cast(DOUBLE).power(f7)",
      "power(CAST(f22 AS DOUBLE), f7)",
      math.pow(2, 3).toString)

    testAllApis(
      'f22.cast(DataTypes.DOUBLE).power('f4),
      "f22.cast(DOUBLE).power(f4)",
      "power(CAST(f22 AS DOUBLE), f4)",
      math.pow(2, 44L).toString)

    testAllApis(
      'f6.power('f22.cast(DataTypes.DOUBLE)),
      "f6.power(f22.cast(DOUBLE))",
      "power(f6, f22)",
      math.pow(4.6D, 2).toString)
  }

  @Ignore // TODO
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
      'f22.cast(DataTypes.DOUBLE).sqrt(),
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
      "-1.0000000000000000000") // calcite: SIGN(Decimal(p,s)) => Decimal(p,s)
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
      "-0.123")

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
      "pi()",
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
      randInteger('f7, 'f4.cast(DataTypes.INT)),
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
      "1996-01-01 00:00:00.000")

    testAllApis(
      'f18.floor(TimeIntervalUnit.MONTH),
      "f18.floor(MONTH)",
      "FLOOR(f18 TO MONTH)",
      "1996-11-01 00:00:00.000")

    testAllApis(
      'f18.floor(TimeIntervalUnit.DAY),
      "f18.floor(DAY)",
      "FLOOR(f18 TO DAY)",
      "1996-11-10 00:00:00.000")

    testAllApis(
      'f18.floor(TimeIntervalUnit.MINUTE),
      "f18.floor(MINUTE)",
      "FLOOR(f18 TO MINUTE)",
      "1996-11-10 06:55:00.000")

    testAllApis(
      'f18.floor(TimeIntervalUnit.SECOND),
      "f18.floor(SECOND)",
      "FLOOR(f18 TO SECOND)",
      "1996-11-10 06:55:44.000")

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
      "1997-01-01 00:00:00.000")

    testAllApis(
      'f18.ceil(TimeIntervalUnit.MONTH),
      "f18.ceil(MONTH)",
      "CEIL(f18 TO MONTH)",
      "1996-12-01 00:00:00.000")

    testAllApis(
      'f18.ceil(TimeIntervalUnit.DAY),
      "f18.ceil(DAY)",
      "CEIL(f18 TO DAY)",
      "1996-11-11 00:00:00.000")

    testAllApis(
      'f18.ceil(TimeIntervalUnit.MINUTE),
      "f18.ceil(MINUTE)",
      "CEIL(f18 TO MINUTE)",
      "1996-11-10 06:56:00.000")

    testAllApis(
      'f18.ceil(TimeIntervalUnit.SECOND),
      "f18.ceil(SECOND)",
      "CEIL(f18 TO SECOND)",
      "1996-11-10 06:55:45.000")

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
      currentDate().cast(DataTypes.STRING).charLength() >= 5,
      "currentDate().cast(STRING).charLength() >= 5",
      "CHAR_LENGTH(CAST(CURRENT_DATE AS VARCHAR)) >= 5",
      "true")

    testAllApis(
      currentTime().cast(DataTypes.STRING).charLength() >= 5,
      "currentTime().cast(STRING).charLength() >= 5",
      "CHAR_LENGTH(CAST(CURRENT_TIME AS VARCHAR)) >= 5",
      "true")

    testAllApis(
      currentTimestamp().cast(DataTypes.STRING).charLength() >= 12,
      "currentTimestamp().cast(STRING).charLength() >= 12",
      "CHAR_LENGTH(CAST(CURRENT_TIMESTAMP AS VARCHAR)) >= 12",
      "true")

    testAllApis(
      localTimestamp().cast(DataTypes.STRING).charLength() >= 12,
      "localTimestamp().cast(STRING).charLength() >= 12",
      "CHAR_LENGTH(CAST(LOCALTIMESTAMP AS VARCHAR)) >= 12",
      "true")

    testAllApis(
      localTime().cast(DataTypes.STRING).charLength() >= 5,
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

  @Ignore // TODO
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
      timestampDiff(TimePointUnit.DAY, nullOf(DataTypes.TIMESTAMP),
        "2016-02-24 12:42:25".toTimestamp),
      "timestampDiff(DAY, Null(SQL_TIMESTAMP), '2016-02-24 12:42:25'.toTimestamp)",
      "TIMESTAMPDIFF(DAY, CAST(NULL AS TIMESTAMP), TIMESTAMP '2016-02-24 12:42:25')",
      "null"
    )

    testAllApis(
      timestampDiff(TimePointUnit.DAY, "2016-02-24 12:42:25".toTimestamp,
        nullOf(DataTypes.TIMESTAMP)),
      "timestampDiff(DAY, '2016-02-24 12:42:25'.toTimestamp,  Null(SQL_TIMESTAMP))",
      "TIMESTAMPDIFF(DAY, TIMESTAMP '2016-02-24 12:42:25',  CAST(NULL AS TIMESTAMP))",
      "null"
    )
  }

  @Ignore // TODO
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
      "2016-02-24 12:42:25".toTimestamp + nullOf(DataTypes.INTERVAL(DataTypes.MINUTE())),
      "'2016-02-24 12:42:25'.toTimestamp + Null(INTERVAL_MILLIS)",
      "TIMESTAMPADD(HOUR, CAST(NULL AS INTEGER), TIMESTAMP '2016-02-24 12:42:25')",
      "null")

    testAllApis(
      nullOf(DataTypes.TIMESTAMP) + -200.hours,
      "Null(SQL_TIMESTAMP) + -200.hours",
      "TIMESTAMPADD(HOUR, -200, CAST(NULL AS TIMESTAMP))",
      "null")

    testAllApis(
      "2016-06-15".toDate + 1.day,
      "'2016-06-15'.toDate + 1.day",
      "TIMESTAMPADD(DAY, 1, DATE '2016-06-15')",
      "2016-06-16")

    testAllApis(
      nullOf(DataTypes.TIMESTAMP) + 3.months,
      "Null(SQL_TIMESTAMP) + 3.months",
      "TIMESTAMPADD(MONTH, 3, CAST(NULL AS TIMESTAMP))",
      "null")
  }

  @Test
  def testToTimestamp(): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    testSqlApi("to_timestamp(1513135677000)", "2017-12-13 03:27:57.000")
    testSqlApi("to_timestamp('2017-09-15 00:00:00')", "2017-09-15 00:00:00.000")
    testSqlApi("to_timestamp('20170915000000', 'yyyyMMddHHmmss')", "2017-09-15 00:00:00.000")
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
      "test".sha2(nullOf(DataTypes.INT)),
      "sha2('test', Null(INT))",
      "SHA2('test', CAST(NULL AS INT))",
      "null")

    // non-constant bit length
    testAllApis(
      "test".sha2('f44),
      "sha2('test', f44)",
      "SHA2('test', f44)",
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
      testAllApis(
        ExpressionParser.parseExpression(tableApiString),
        tableApiString,
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

      testAllApis(
        ExpressionParser.parseExpression(tableApiString),
        tableApiString,
        sqlApiString,
        "null"
      )
    })
  }

  @Test
  def testNullBigDecimal(): Unit = {
    testAllApis(
      ExpressionParser.parseExpression("f41.sign()"),
      "f41.sign()",
      "SIGN(f41)",
      "null")
    testSqlApi("SIGN(f41)", "null")
  }

  @Test
  def testEncodeAndDecode(): Unit = {
    // TODO
    //    testAllApis(
    //      "aabbef".encode("UTF-16LE").decode("UTF-16LE"),
    //      "('aabbef'.encode('UTF-16LE')).decode('UTF-16LE')",
    //      "decode(encode('aabbef', 'UTF-16LE'), 'UTF-16LE')",
    //      "aabbef")
    //
    //    testAllApis(
    //      "aabbef".encode("utf-8").decode("utf-8"),
    //      "('aabbef'.encode('utf-8')).decode('utf-8')",
    //      "decode(encode('aabbef', 'utf-8'), 'utf-8')",
    //      "aabbef")
    //
    //    testAllApis(
    //      "".encode("utf-8").decode("utf-8"),
    //      "(''.encode('utf-8')).decode('utf-8')",
    //      "decode(encode('', 'utf-8'), 'utf-8')",
    //      "")

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

    //    testAllApis(
    //      "中国".encode("UTF-16LE").decode("UTF-16LE"),
    //      "('中国'.encode('UTF-16LE')).decode('UTF-16LE')",
    //      "decode(encode('中国', 'UTF-16LE'), 'UTF-16LE')",
    //      "中国")
  }

  @Ignore // TODO
  @Test
  def testEmptyStringToTime(): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    addSqlTestExpr("to_date('2017-09-15 00:00:00') <> ''", "null")
    addSqlTestExpr("to_timestamp(1513135677000) <> ''", "null")
  }
}
