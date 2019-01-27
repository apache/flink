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

import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.functions.ScalarFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.row
import org.apache.flink.table.runtime.batch.sql.TestData._
import org.apache.flink.table.runtime.utils.RowsCollectTableSink
import org.apache.flink.types.Row
import org.apache.flink.util.AbstractID
import org.junit.{Test, _}

class StringITCase() extends BatchTestBase {

  @Before
  def before(): Unit = {
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)
    registerCollection("Table3", nullData3, type3, nullablesOfData3, 'a, 'b, 'c)
  }

  @Test
  def testCast(): Unit = {
    checkResult(
      "SELECT CAST(a AS VARCHAR(10)) FROM Table3 WHERE CAST(a AS VARCHAR(10)) = '1'",
      Seq(row(1)))
  }

  @Test
  def testLike(): Unit = {
    checkResult(
      "SELECT a FROM Table3 WHERE c LIKE '%llo%'",
      Seq(row(2), row(3), row(4)))

    checkResult(
      "SELECT a FROM Table3 WHERE CAST(a as VARCHAR(10)) LIKE CAST(b as VARCHAR(10))",
      Seq(row(1), row(2)))

    checkResult(
      "SELECT a FROM Table3 WHERE c NOT LIKE '%Comment%' AND c NOT LIKE '%Hello%'",
      Seq(row(1), row(5), row(6), row(null), row(null)))

    checkResult(
      "SELECT a FROM Table3 WHERE c LIKE 'Comment#%' and c LIKE '%2'",
      Seq(row(8), row(18)))

    checkResult(
      "SELECT a FROM Table3 WHERE c LIKE 'Comment#12'",
      Seq(row(18)))

    checkResult(
      "SELECT a FROM Table3 WHERE c LIKE '%omm%nt#12'",
      Seq(row(18)))
  }

  @Test
  def testLikeWithEscape(): Unit = {

    val rows = Seq(
      (1, "ha_ha"),
      (2, "ffhaha_hahaff"),
      (3, "aaffhaha_hahaffaa"),
      (4, "aaffhaaa_aahaffaa"),
      (5, "a%_ha")
    )

    tEnv.registerCollection("MyT", rows, 'a, 'b)

    checkResult(
      "SELECT a FROM MyT WHERE b LIKE '%ha?_ha%' ESCAPE '?'",
      Seq(row(1), row(2), row(3)))

    checkResult(
      "SELECT a FROM MyT WHERE b LIKE '%ha?_ha' ESCAPE '?'",
      Seq(row(1)))

    checkResult(
      "SELECT a FROM MyT WHERE b LIKE 'ha?_ha%' ESCAPE '?'",
      Seq(row(1)))

    checkResult(
      "SELECT a FROM MyT WHERE b LIKE 'ha?_ha' ESCAPE '?'",
      Seq(row(1)))

    checkResult(
      "SELECT a FROM MyT WHERE b LIKE '%affh%ha?_ha%' ESCAPE '?'",
      Seq(row(3)))

    checkResult(
      "SELECT a FROM MyT WHERE b LIKE 'a?%?_ha' ESCAPE '?'",
      Seq(row(5)))

    checkResult(
      "SELECT a FROM MyT WHERE b LIKE 'h_?_ha' ESCAPE '?'",
      Seq(row(1)))
  }

  @Test
  def testChainLike(): Unit = {
    // special case to test CHAIN_PATTERN.
    checkResult(
      "SELECT a FROM Table3 WHERE c LIKE '% /sys/kvengine/KVServerRole/kvengine/kv_server%'",
      Seq())

    // special case to test CHAIN_PATTERN.
    checkResult(
      "SELECT a FROM Table3 WHERE c LIKE '%Tuple%%'",
      Seq(row(null), row(null)))

    // special case to test CHAIN_PATTERN.
    checkResult(
      "SELECT a FROM Table3 WHERE c LIKE '%/order/inter/touch/backwayprice.do%%'",
      Seq())
  }

  @Test
  def testEqual(): Unit = {
    checkResult(
      "SELECT a FROM Table3 WHERE c = 'Hi'",
      Seq(row(1)))

    checkResult(
      "SELECT c FROM Table3 WHERE c <> 'Hello' AND b = 2",
      Seq(row("Hello world")))
  }

  @Test
  def testSubString(): Unit = {
    checkResult(
      "SELECT SUBSTRING(c, 6, 13) FROM Table3 WHERE a = 6",
      Seq(row("Skywalker")))
  }

  @Test
  def testConcat(): Unit = {
    checkResult(
      "SELECT CONCAT(c, '-haha') FROM Table3 WHERE a = 1",
      Seq(row("Hi-haha")))

    checkResult(
      "SELECT CONCAT_WS('-x-', c, 'haha') FROM Table3 WHERE a = 1",
      Seq(row("Hi-x-haha")))
  }

  @Test
  def testStringAgg(): Unit = {
    checkResult(
      "SELECT MIN(c) FROM Table3",
      Seq(row("Comment#1")))

    checkResult(
      "SELECT SUM(b) FROM Table3 WHERE c = 'NullTuple' OR c LIKE '%Hello world%' GROUP BY c",
      Seq(row(1998), row(2), row(3)))
  }

  @Test
  def testStringUdf(): Unit = {
    tEnv.registerFunction("myFunc", MyStringFunc)
    checkResult(
      "SELECT myFunc(c) FROM Table3 WHERE a = 1",
      Seq(row("Hihaha")))
  }

  /**
    * We should use correct type in segmentation optimization.
    * See [[org.apache.flink.table.plan.schema.IntermediateBoundedStreamTable.getRowType()]].
    */
  @Test
  def testMultiSink(): Unit = {
    /*
    Type mismatch:
    CREATE TABLE TableA(a VARCHAR) WITH (type='kafka',path='123123');
    CREATE TABLE TableB(b VARCHAR) WITH (type='kafka',path='123123');
    CREATE TABLE TableC(b VARCHAR) WITH (type='kafka',path='123123');
    CREATE VIEW TMP_VIEW AS SELECT CONCAT(a, '1') AS log_diu FROM TableA;
    INSERT INTO TableB SELECT log_diu FROM TMP_VIEW;
    INSERT INTO TableC SELECT log_diu FROM TMP_VIEW;
    */
    conf.setSubsectionOptimization(true)
    val typeSerializer = TypeExtractor.getForClass(classOf[Seq[Row]])
        .createSerializer(env.getConfig)
    val tableSink1 = new RowsCollectTableSink
    val tableSink2 = new RowsCollectTableSink
    tableSink1.init(typeSerializer, new AbstractID().toString)
    tableSink2.init(typeSerializer, new AbstractID().toString)

    val table = tEnv.scan("Table3").select('a, concat('c, 'c) as 'd)
    table.where('a > 10).select('a + 1, 'd).writeToSink(tableSink1)
    table.where('a < 10).select('a + 5, 'd).writeToSink(tableSink2)

    tEnv.execute()
  }

  @Test
  def testNestUdf(): Unit = {
    registerCollection("SmallTable3", smallData3, type3, nullablesOfData3, 'a, 'b, 'c)
    tEnv.registerFunction("func", MyStringFunc)
    checkResult(
      "SELECT func(func(func(c))) FROM SmallTable3",
      Seq(row("Hello worldhahahahahaha"), row("Hellohahahahahaha"), row("Hihahahahahaha")))
  }
}

object MyStringFunc extends ScalarFunction {
  def eval(s: String): String = s + "haha"
}
