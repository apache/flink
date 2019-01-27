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

package org.apache.flink.table.runtime.batch.sql

import java.util.TimeZone

import org.apache.calcite.tools.ValidationException
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.row
import org.apache.flink.table.runtime.batch.sql.TestData._
import org.apache.flink.table.util.DateTimeTestUtil._
import org.junit.{Before, Test}

class BuildInFunctionsITCase extends BatchTestBase {

  @Before
  def before(): Unit = {
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)
    registerCollection("testTable", buildInData, buildInType, "a,b,c,d,e,f,g,h,i,j")
  }

  @Test
  def testBitOperateFunction() = {
    checkResult(
      "SELECT BITOR(3, 4), BITOR(-3, 2)," +
        "BITXOR(3, 4), BITXOR(-3, 2)," +
        "BITNOT(-3), BITNOT(3)," +
        "BITAND(3, 2), BITAND(-3, -2)," +
        "BIN(2), BIN(-7), BIN(-1) FROM testTable WHERE c = 2",
      Seq(row(7, -1, 7, -1, 2, -4, 2, -4, "10",
        "1111111111111111111111111111111111111111111111111111111111111001",
        "1111111111111111111111111111111111111111111111111111111111111111"
      )))
  }

  @Test
  def testLogicalOperateFunction() = {
    checkResult(
      "SELECT TRUE OR TRUE, TRUE OR FALSE, FALSE OR FALSE, TRUE OR a, a OR FALSE, a OR a" +
        " FROM testTable WHERE b = 2",
      Seq(row(true, true, false, true, null, null))
    )

    checkResult(
      "SELECT TRUE AND TRUE, TRUE AND FALSE, FALSE AND FALSE, TRUE AND a, a AND FALSE, a AND a" +
        " FROM testTable WHERE b = 2",
      Seq(row(true, false, false, null, false, null))
    )

    checkResult(
      "SELECT NOT TRUE, NOT FALSE, NOT a FROM testTable WHERE b = 2",
      Seq(row(false, true, null))
    )

    checkResult(
      "SELECT a IS TRUE, a IS NOT TRUE, a IS FALSE, a IS NOT FALSE, a IS UNKNOWN," +
        "a IS NOT UNKNOWN FROM testTable",
      Seq(
        row(false, true, true, false, false, true),//a=false
        row(false, true, false, true, true, false),//a=null
        row(true, false, false, true, false, true))//a=true
    )
  }

  @Test
  def testConditionFunction() = {
    checkResult(
      "SELECT b," +
        "CASE b WHEN 1 THEN 1 WHEN 2 THEN 4 WHEN 3 THEN 9 ELSE 99 END," +
        "CASE b WHEN 2 THEN 4 WHEN 3 THEN 9 WHEN 4 THEN 16 ELSE 66 END," +
        "CASE WHEN b < 0 THEN 1 WHEN b = 0 THEN 2 WHEN b > 0 THEN 3 ELSE 88 END," +
        "CASE WHEN b < 0 THEN 1 WHEN b = 2 THEN 2 WHEN b > 2 THEN 3 ELSE 77 END," +
        "NULLIF(b, 1), NULLIF(b, 2), NULLIF(9, b)," +
        "COALESCE(NULLIF(b, 1), 3, 4)," +
        "COALESCE(33, NULLIF(b, 1))," +
        "COALESCE(NULLIF(b, 1), NULLIF(67, b), NULLIF(b, 1))," +
        "IF(b > 1, 'abc', 'def')," +
        "IF(b = 1, 100, 200)" +
        " FROM testTable WHERE a = FALSE",
      Seq(row(1, 1, 66, 3, 77, null, 1, 9, 3, 33, 67, "def", 100))
    )

    checkResult(
      "SELECT b," +
        "IS_DECIMAL('98'), IS_DECIMAL('-4.9'), IS_DECIMAL('7a')," +
        "IS_DIGIT('908'), IS_DIGIT('90.8'), IS_DIGIT('-1')," +
        "IS_ALPHA('abcd'), IS_ALPHA('ab cd'), IS_ALPHA('2a')" +
        " FROM testTable WHERE a = FALSE",
      Seq(row(1, true, true, false, true, false, false, true, false, false))
    )
  }

  @Test
  def testMathFunction() = {
    checkResult(
      "SELECT b," +
        "+b, -c, (d + e) * (d - e) / (c - b), DIV(c, b), ABS(c), MOD(d, c)" +
        " FROM testTable WHERE a = TRUE",
      Seq(row(3, 3, 4, -3.48, -1, 4, -1))
    )

    checkResult(
      "SELECT b," +
        "SQRT(0), SQRT(16), POWER(2, b), POWER(2.5, -2.0), EXP(0)," +
        "LN(E()), LN(EXP(4)), LOG10(10), LOG10(10000), LOG(E(), EXP(-2)), LOG(E())," +
        "CEIL(-7.2), CEILING(0.1), FLOOR(-2.7), FLOOR(999.999)," +
        "SIGN(-2.9), SIGN(CAST(b AS INTEGER)), SIGN(0)," +
        "ABS(PI() - 3.141592653589793) < 0.000001," +
        "ABS(RAND(2) - 0.7311469360199058) < 0.000001," +
        "ABS(RAND(3) - 0.7310573691488621) < 0.000001" +
        " FROM testTable WHERE a = TRUE",
      Seq(row(3,
        0.0, 4.0, 8.0, 0.16, 1.0,
        1.0, 4.0, 1.0, 4.0, -2.0, 1.0,
        -7, 1, -3, 999,
        "-1.0", // calcite: SIGN(DECIMAL(p,s)) => DECIMAL(p,s)
        1, 0,
        true, true, true))
    )

    checkResult(
      "SELECT b," +
        "ABS(SIN(RADIANS(30)) - COS(RADIANS(60))) < 0.000001," +
        "ABS(TAN(RADIANS(45)) - COT(RADIANS(45))) < 0.000001," +
        "ABS(DEGREES(ASIN(1)) - DEGREES(ACOS(0))) < 0.000001," +
        "ABS(DEGREES(ATAN(SQRT(3))) - 60) < 0.000001," +
        "DEGREES(PI() / 2)," +
        "DEGREES(PI())," +
        "ABS(POWER(SIN(3), 2) + POWER(COS(3), 2) - 1) < 0.000001," +
        "ABS(SIN(4) / COS(4) - TAN(4)) < 0.000001," +
        "ABS(TAN(5) * COT(5) - 1) < 0.000001" +
        " FROM testTable WHERE a = TRUE",
      Seq(row(3, true, true, true, true, 90.0, 180.0, true, true, true))
    )
  }

  @Test
  def testStringOperateFunction(): Unit = {
    checkResult("SELECT b," +
      "'ab' || 'cd', CHAR_LENGTH('abc'), CHARACTER_LENGTH('abc'), UPPER('aBc2'), LOWER('aBc3')," +
      "POSITION('ab' IN 'cab ab'), POSITION('ab' IN 'ca bad')," +
      "TRIM(BOTH ' ' FROM '  abc  '), TRIM(LEADING ' ' FROM '  abc  '), " +
      "TRIM(TRAILING ' ' FROM '  abc  '), " +
      "LTRIM('  abc  '), RTRIM('  abc  '), " +
      "TRIM(BOTH 'a' FROM 'aabcda'), TRIM(LEADING 'a' FROM 'aabcda')," +
      "TRIM(TRAILING 'a' FROM 'aabcda')," +
      "LTRIM('aabcda', 'a'), RTRIM('aabcda', 'a')," +
      "TRIM(BOTH 'abd' FROM 'aabcda'), TRIM(LEADING 'abd' FROM 'aabcda')," +
      "TRIM(TRAILING 'abd' FROM 'aabcda')," +
      "LTRIM('aabcda', 'abd'), RTRIM('aabcda', 'abd')," +
      "OVERLAY('abcde' PLACING 'xyz' FROM 2 FOR 3), OVERLAY('abcde' PLACING 'xyz' FROM 3)," +
      "SUBSTRING('abcd' FROM 2), SUBSTRING('abcd' FROM 2 FOR 2)," +
      "SUBSTRING('abcd', 2), SUBSTRING('abcd', 2, 2)," +
      "SUBSTR('abcd', 2), SUBSTR('abcd', 2, 2)," +
      "INITCAP('b3_d2hF AAa'), CHR(97), CHR(65), CHR(255)," +
      "CONCAT('a', 'b', g, 'def'), CONCAT_WS('|', 'a', 'b', g, 'def')," +
      "LPAD('ab', 5, 'cd'), LPAD('ab', -8, 'cde')," +
      "RPAD('abcd', 5, 'efg'), RPAD('abcd', 5, '')," +
      "REPEAT('abcd', 3), REPEAT('abcd', 0)," +
      "REVERSE('abcd'), REVERSE('Alibaba Group')," +
      "HASH_CODE('abcd'), HASH_CODE('')," +
      "MD5('abcd'), MD5('')," +
      "TO_BASE64(FROM_BASE64('abcd'))" +
      " FROM testTable WHERE a = TRUE",
      Seq(row(3, "abcd", 3, 3, "ABC2", "abc3", 2, 0,
        "abc", "abc  ", "  abc", "abc  ", "  abc",
        "bcd", "bcda", "aabcd", "bcda", "aabcd",
        "c", "cda", "aabc",  "cda", "aabc",
        "axyze", "abxyz",
        "bcd", "bc", "bcd", "bc", "bcd", "bc", "B3_D2hf Aaa", "a", "A", "ÿ",
        "abdef", "a|b|def", "cdcab", null, "abcde", null, "abcdabcdabcd", "", "dcba",
        "puorG ababilA", 2987074, 0, "e2fc714c4727ee9395f324cd2e7f331f",
        "d41d8cd98f00b204e9800998ecf8427e", "abcd")))

    checkResult("SELECT b," +
      "SPLIT_INDEX('ab|cd|e', '|', 1), SPLIT_INDEX('ab|cd|e', '|', 3)," +
      "REGEXP_REPLACE('ab-cd-ef', '-', '|')," +
      "REGEXP_REPLACE('ab-cd-200', '(', '888')," +
      "REGEXP_EXTRACT('foothebar', 'foo(.*?)(bar)', 2)," +
      "REGEXP_EXTRACT('100-200', ')', 1)," +
      "REGEXP_EXTRACT('100-200', '', 1)," +
      "REGEXP('k1=v1;k2=v2', 'k2*')," +
      "REGEXP('k1=v1;k2=v2', 'k3')," +
      "KEYVALUE('k1=v1;k2=v2', ';', '=', 'k2')," +
      "KEYVALUE('k1=v1;k2=v2', ';', '=', 'k3')," +
      "JSON_VALUE('[10, 20, [30, 40]]', '$[2][*]')," +
      "JSON_VALUE('[10, 20, [30, [40, 50, 60]]]', '$[2][*][1][*]')," +
      "JSON_VALUE('{xx]', '$[2][*]')," +
      "PARSE_URL('http://www.alibabagroup.com/cn/global/home?query=1', 'QUERY', 'query')," +
      "PARSE_URL('http://www.alibabagroup.com/cn/global/home?query=1', 'QUERY')," +
      "PARSE_URL('http://www.alibabagroup.com/cn/global/home?query=1', 'HOST')," +
      "PARSE_URL('http://www.alibabagroup.com/cn/global/home?query=1', 'PATH')" +
      " FROM testTable WHERE a = true",
      Seq(row(3, "cd", null, "ab|cd|ef", null, "bar", null, null, true, false,
        "v2", null, "[30,40]", "[30,[40,50,60]]", null, "1", "query=1", "www.alibabagroup.com",
        "/cn/global/home"))
    )

    checkResult("SELECT " +
        "`left`('abcd', 2), `left`('abcd', 5), `left`('abcd', CAST(null as Integer)), " +
        "`left`(CAST(null as VARCHAR), -2), `left`('abcd', -2), `left`('abcd', 0) " +
        " FROM testTable WHERE a = TRUE",
      Seq(row("ab", "abcd", null, null, "", "")))

    checkResult("SELECT " +
        "`right`('abcd', 2), `right`('abcd', 5), `right`('abcd', CAST(null as Integer)), " +
        "`right`(CAST(null as VARCHAR), -2), `right`('abcd', -2), `right`('abcd', 0) " +
        " FROM testTable WHERE a = TRUE",
      Seq(row("cd", "abcd", null, null, "", "")))

    checkResult("SELECT " +
        "locate('a', 'abcdabcd', 3), " +
        "locate(cast('a' as varchar(1)), cast('abcdabcd' as varchar(10)), 3), " +
        "locate('a', 'abcdabcd', 3) = " +
        "locate(cast('a' as varchar(1)), cast('abcdabcd' as varchar(10)), 3) " +
        "FROM testTable WHERE a = TRUE",
      Seq(row(5, 5, true)))

    checkResult("SELECT length(uuid()) FROM testTable WHERE a = TRUE",
      Seq(row(36)))

    checkResult("SELECT " +
        "ascii('val_238'), " +
        "ascii('val_239'), " +
        "ascii('val_238') = ascii('val_239')," +
        "ascii(''), " +
        "ascii(CAST(null as VARCHAR))" +
        "FROM testTable WHERE a = TRUE",
      Seq(row(118, 118, true, 0, null)))

    checkResult("SELECT decode(encode('val_238', 'US-ASCII'), 'US-ASCII'), " +
        "decode(encode('val_238', 'utf-8'), 'utf-8'), " +
        "decode(encode('val_238', 'US-ASCII'), 'US-ASCII') = " +
        "decode(encode('val_238', 'utf-8'), 'utf-8'), " +
        "encode(CAST(null as VARCHAR), 'utf-8'), " +
        "encode('val_238', CAST(null as VARCHAR)), " +
        "decode(CAST(null as BINARY), 'utf-8'), " +
        "decode(encode('val_238', 'utf-8'), CAST(null as VARCHAR))," +
        "decode(encode('中国', 'utf-8'), 'utf-8') " +
        "FROM testTable WHERE a = TRUE",
      Seq(row("val_238", "val_238", true, null, null, null, null, "中国")))

    checkResult("SELECT " +
        "instr('val_238', '_'), " +
        "instr('val_239', '_'), " +
        "instr('val_238', '_') = instr('val_239', '_')" +
        "FROM testTable WHERE a = TRUE",
      Seq(row(4, 4, true)))
  }

  @Test
  def testConvertFunction() = {
    checkResult("SELECT CAST(b AS SMALLINT)," +
      "CAST(2 AS BIGINT), CAST(-9 AS DOUBLE), CAST(888 AS VARCHAR), CAST('99' AS INTEGER)" +
      " FROM testTable WHERE a = FALSE",
      Seq(row(1, 2, -9.0, "888", 99)))
  }

  @Test
  def testTimeOperateFunction() = {
    checkResult("SELECT b," +
      "DATE_FORMAT(TIMESTAMP '2017-12-13 19:03:35', 'MM-dd-yyyy HH:mm:ss')," +
      "DATE_FORMAT('2017-12-13 19:03:35', 'yyyy/MM/dd HH:mm:ss')," +
      "DATE_FORMAT('2017-12-13 19:03:35', 'yyyy-MM-dd HH:mm:ss', 'MM/dd/yyyy HH:mm:ss')," +
      "UNIX_TIMESTAMP('2017-12-13 19:25:30')," +
      "UNIX_TIMESTAMP('12/13/2017 19:25:30', 'MM/dd/yyyy HH:mm:ss')," +
      "FROM_UNIXTIME(1513193130), FROM_UNIXTIME(1513193130, 'MM/dd/yyyy HH:mm:ss')," +
      "DATEDIFF('2017-12-14 01:00:34', '2016-12-14 12:00:00')," +
      "DATEDIFF(TIMESTAMP '2017-12-14 01:00:23', '2016-08-14 12:00:00')," +
      "DATEDIFF('2017-12-14 09:00:23', TIMESTAMP '2013-08-19 11:00:00')," +
      "DATEDIFF(TIMESTAMP '2017-12-14 09:00:23', TIMESTAMP '2018-08-19 11:00:00')," +
      "DATE_SUB(TIMESTAMP '2017-12-14 09:00:23', 3)," +
      "DATE_SUB('2017-12-14 09:00:23', -3)," +
      "DATE_ADD('2017-12-14', 4), DATE_ADD(TIMESTAMP '2017-12-14 09:10:20',-4)" +
      " FROM testTable WHERE a = TRUE",
      Seq(row(3, "12-13-2017 19:03:35", "2017/12/13 19:03:35", "12/13/2017 19:03:35", 1513193130,
        1513193130, "2017-12-13 19:25:30", "12/13/2017 19:25:30", 365, 487, 1578, -248,
        "2017-12-11", "2017-12-17", "2017-12-18", "2017-12-10")))

    //j 2015-05-20 10:00:00.887
    checkResult("SELECT b," +
      "FLOOR(j TO DAY), CEIL(j TO DAY), YEAR(j), QUARTER(j), MONTH(j), WEEK(j)," +
      "DAYOFYEAR(j), DAYOFMONTH(j), DAYOFWEEK(j), HOUR(j), MINUTE(j), SECOND(j)," +
      "TIMESTAMPADD(MINUTE, 1, j), TIMESTAMPADD(HOUR, -34, j)" +
      " FROM testTable WHERE a = TRUE",
      Seq(row(3, UTCTimestamp("2015-05-20 00:00:00.0"),
        UTCTimestamp("2015-05-21 00:00:00.0"), 2015, 2, 5, 21, 140, 20, 4, 10, 0, 0,
        UTCTimestamp("2015-05-20 10:01:00.887"),
        UTCTimestamp("2015-05-19 00:00:00.887"))))

    // LOCALXXX === CURRENT_XXX iff in UTC Zone
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    checkResult("SELECT b," +
      "LOCALTIME = CURRENT_TIME, LOCALTIMESTAMP = CURRENT_TIMESTAMP," +
      "CONCAT_WS('-', CAST(YEAR(CURRENT_DATE) AS VARCHAR), CAST(MONTH(CURRENT_DATE) AS VARCHAR)," +
      "CAST(DAYOFMONTH(CURRENT_DATE) AS VARCHAR)) = FROM_UNIXTIME(NOW(), 'yyyy-M-d')" +
      " FROM testTable WHERE a = TRUE",
      Seq(row(3, true, true, true)))
  }

  @Test
  def testCompareFunction() = {
    env.setParallelism(4)
    checkResult("SELECT b," +
      "c > d, c = d, c <> d, c >= d, c < d, c <= d, g IS NULL, g IS NOT NULL," +
      "g IS DISTINCT FROM h, g IS NOT DISTINCT FROM h," +
      "c BETWEEN -5 AND 0, c NOT BETWEEN -5 AND 0 FROM testTable WHERE a = TRUE",
      Seq(row(3, true, false, true, true, false, false, true, false, false, true, true, false)))

    checkResult("SELECT a FROM testTable WHERE f LIKE 'a%'",
      Seq(row(false)))

    checkResult("SELECT a FROM testTable WHERE f NOT LIKE 'e_%g'",
      Seq(row(false)))

    checkResult("SELECT a FROM testTable WHERE g LIKE '_x%%' ESCAPE 'x'",
      Seq(row(false)))

    checkResult("SELECT a FROM testTable WHERE f SIMILAR TO '(e|c)%'",
      Seq(row(true)))

    checkResult("SELECT a FROM testTable WHERE g NOT SIMILAR TO '(f|g)y%%' ESCAPE 'y'",
      Seq(row(null)))

    checkResult("SELECT " +
      "b IN (3, 4, 5)," +
      "b NOT IN (3, 4, 5)," +
      "EXISTS (SELECT c FROM testTable WHERE c > 2)," +
      "NOT EXISTS (SELECT c FROM testTable WHERE c = 2)" +
      " FROM testTable WHERE a = TRUE",
      Seq(row(true, false, false, false)))

    checkResult("SELECT a, b FROM testTable WHERE CAST(b AS INTEGER) IN " +
      "(SELECT ABS(c) FROM testTable)",
      Seq(row(null, 2), row(true, 3)))

    checkResult("SELECT a, b FROM testTable WHERE CAST(b AS INTEGER) NOT IN " +
      "(SELECT ABS(c) FROM testTable)",
      Seq(row(false, 1)))

    checkResult("SELECT a, b FROM testTable WHERE EXISTS (SELECT c FROM testTable WHERE c > b) " +
      "AND b = 2",
      Seq(row(null, 2)))

    checkResult("SELECT a, b FROM testTable WHERE  NOT EXISTS " +
      "(SELECT c FROM testTable WHERE c > b) OR b <> 2",
      Seq(row(false, 1), row(true, 3)))
  }

  @Test
  def testTableGenerateFunction() = {
    checkResult("SELECT f, g, v FROM testTable," +
      "LATERAL TABLE(STRING_SPLIT(f, ' ')) AS T(v)",
      Seq(
        row("abcd", "f%g", "abcd"),
        row("e fg", null, "e"),
        row("e fg", null, "fg")))

    checkResult("SELECT f, g, v FROM testTable," +
      "LATERAL TABLE(GENERATE_SERIES(0, CAST(b AS INTEGER))) AS T(v)",
      Seq(
        row("abcd", "f%g", 0),
        row(null, "hij_k", 0),
        row(null, "hij_k", 1),
        row("e fg", null, 0),
        row("e fg", null, 1),
        row("e fg", null, 2)))

    checkResult("SELECT f, g, v FROM testTable," +
      "LATERAL TABLE(JSON_TUPLE('{\"a1\": \"b1\", \"a2\": \"b2\", \"e fg\": \"b3\"}'," +
      "'a1', f)) AS T(v)",
      Seq(
        row("abcd", "f%g", "b1"),
        row("abcd", "f%g", null),
        row(null, "hij_k", "b1"),
        row(null, "hij_k", null),
        row("e fg", null, "b1"),
        row("e fg", null, "b3")))

    checkResult("SELECT f, g, v FROM " +
      "testTable JOIN LATERAL TABLE(JSON_TUPLE" +
      "('{\"a1\": \"b1\", \"a2\": \"b2\", \"e fg\": \"b3\"}', 'a1', f)) AS T(v) " +
      "ON CHAR_LENGTH(f) = CHAR_LENGTH(v) + 2 OR CHAR_LENGTH(g) = CHAR_LENGTH(v) + 3",
      Seq(
        row("abcd", "f%g", "b1"),
        row(null, "hij_k", "b1"),
        row("e fg", null, "b1"),
        row("e fg", null, "b3")))

    checkResult("SELECT f, g, v FROM " +
        "testTable JOIN LATERAL TABLE(JSON_TUPLE" +
        "('{\"a1\": \"b1\", \"a2\": \"b2\", \"e fg\": \"b3\"}', 'a1', f)) AS T(v) " +
        "ON CHAR_LENGTH(f) = CHAR_LENGTH(v) + 2 OR CHAR_LENGTH(g) = CHAR_LENGTH(v) + 3",
      Seq(
        row("abcd", "f%g", "b1"),
        row(null, "hij_k", "b1"),
        row("e fg", null, "b1"),
        row("e fg", null, "b3")))
  }

  /**
    * Due to the improper translation of TableFunction left outer join (see CALCITE-2004), the
    * join predicate can only be empty or literal true (the restriction should be removed in
    * FLINK-7865).
    */
  @Test(expected = classOf[ValidationException])
  def testTableGenerateFunctionLeftJoin(): Unit = {
    checkResult("SELECT f, g, v FROM " +
        "testTable LEFT OUTER JOIN LATERAL TABLE(GENERATE_SERIES(0, CAST(b AS INTEGER))) AS T(v) " +
        "ON LENGTH(f) = v + 2 OR LENGTH(g) = v + 4",
      Seq(
        row(null, "hij_k", 1),
        row("e fg", null, 2)))
  }
}
