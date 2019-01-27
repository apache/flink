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

package org.apache.flink.table.codegen

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.functions.ScalarFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableConfig, TableConfigOptions, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.runtime.utils._
import org.apache.flink.types.Row
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{Before, Test}

class CodeGenCommonSubexpressionEliminationITCase extends StreamingTestBase {

  val sourceData = List(
    (1, "Pink Floyd"),
    (2, "Dead Can Dance"),
    (3, "The Doors"),
    (4, "Dou Wei"),
    (5, "Cui Jian")
  )

  val albumsData = List(
    ("Pink Floyd", "The Dark Side of the Moon"),
    ("Pink Floyd", "Wish You Were Here"),
    ("Dead Can Dance", "Aion"),
    ("Nirvana", "Nevermind")
  )

  @Before
  def clear(): Unit = {
    StreamTestSink.clear()
  }

  @Test
  def testIf(): Unit = {
    val configs = Array(new TableConfig, new TableConfig)
    configs(0).getConf.setInteger(TableConfigOptions.SQL_CODEGEN_LENGTH_MAX,1)

    configs.foreach { config =>
      val tEnv = TableEnvironment.getTableEnvironment(env, config)
      val t1 = env.fromCollection(sourceData)
        .toTable(tEnv)
        .as('id, 'band)

      tEnv.registerTable("T1", t1)

      val sqlQuery =
        """
          | SELECT
          |   id,
          |   if (char_length(band) >= 10, 1, 0),
          |   if (char_length(band) >= 10, 1, 0)
          | FROM T1
        """.stripMargin

      val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
      val sink = new TestingAppendSink
      result.addSink(sink)
      env.execute()

      val expected = List(
        "1,1,1",
        "2,1,1",
        "3,0,0",
        "4,0,0",
        "5,0,0"
      )
      assertEquals(expected.sorted, sink.getAppendResults.sorted)
    }
  }

  @Test
  def testIfWithDifferentScope(): Unit = {
    val configs = Array(new TableConfig(), new TableConfig)

    configs(0).getConf.setInteger(TableConfigOptions.SQL_CODEGEN_LENGTH_MAX,1)

    configs.foreach(tableConfig => {
      val tEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env, tableConfig)
      tEnv.registerFunction("UpperUdf", new UpperUdf)
      tEnv.registerFunction("LowerUdf", new LowerUdf)

      val t1 = env.fromCollection(sourceData)
        .toTable(tEnv)
        .as('id, 'band)

      tEnv.registerTable("T1", t1)

      val sqlQuery =
        """
          | SELECT
          |   id,
          |   trim(UpperUdf(band)),
          |   if (char_length(band) >= 10, UpperUdf(band), LowerUdf(band)),
          |   if (char_length(band) >= 10, UpperUdf(band), LowerUdf(band)),
          |   trim(LowerUdf(band))
          | FROM T1
        """.stripMargin

      val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
      val sink = new TestingAppendSink
      result.addSink(sink)
      env.execute()

      val expected = List(
        "1,PINK FLOYD,PINK FLOYD,PINK FLOYD,pink floyd",
        "2,DEAD CAN DANCE,DEAD CAN DANCE,DEAD CAN DANCE,dead can dance",
        "3,THE DOORS,the doors,the doors,the doors",
        "4,DOU WEI,dou wei,dou wei,dou wei",
        "5,CUI JIAN,cui jian,cui jian,cui jian"
      )
      assertEquals(expected.sorted, sink.getAppendResults.sorted)
    })
  }

  @Test
  def testCaseWhen(): Unit = {
    tEnv.registerFunction("UpperUdf", new UpperUdf)
    tEnv.registerFunction("LowerUdf", new LowerUdf)

    val t1 = env.fromCollection(sourceData)
      .toTable(tEnv)
      .as('id, 'band)

    tEnv.registerTable("T1", t1)

    val sqlQuery =
      """
        | SELECT
        |   id,
        |   CASE WHEN char_length(UpperUdf(band)) >= 10  THEN UpperUdf(band) ELSE LowerUdf(band) END
        | FROM T1
        | WHERE char_length(UpperUdf(band)) > 1 AND LowerUdf(band) IS NOT NULL
      """.stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "1,PINK FLOYD",
      "2,DEAD CAN DANCE",
      "3,the doors",
      "4,dou wei",
      "5,cui jian"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testCoalesce(): Unit = {
    tEnv.registerFunction("UpperUdf", new UpperUdf)
    tEnv.registerFunction("LowerUdf", new LowerUdf)

    val t1 = env.fromCollection(sourceData)
      .toTable(tEnv)
      .as('id, 'band)

    tEnv.registerTable("T1", t1)

    // in this case, UpperUdf() is generated only once
    // LowerUdf() has to be generated twice, because it was generated in
    // different variable scope.
    val sqlQuery =
      """
        | SELECT
        |   COALESCE(UpperUdf(band), LowerUdf(band)),
        |   UpperUdf(band),
        |   LowerUdf(band)
        | FROM T1
        | WHERE id = 1 AND LowerUdf(band) = 'pink floyd'
      """.stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "PINK FLOYD,PINK FLOYD,pink floyd"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testNonDeterministic(): Unit = {
    tEnv.registerFunction("NonDeterministicUdf", new NonDeterministicUdf)

    val t1 = env.fromCollection(sourceData)
      .toTable(tEnv)
      .as('id, 'band)

    tEnv.registerTable("T1", t1)

    val sqlQuery =
    """
      | SELECT
      |   NonDeterministicUdf(band),
      |   NonDeterministicUdf(band)
      | FROM T1
      | WHERE char_length(NonDeterministicUdf(band)) > 0
    """.stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    assertTrue(sink.getAppendResults.forall({ s =>
      val fields = s.split(",")
      fields.length == 2 && !fields(0).equals(fields(1))
    }))
  }

  @Test
  def testJoin(): Unit = {
    env.getConfig.disableObjectReuse()
    tEnv.registerFunction("UpperUdf", new UpperUdf)
    tEnv.registerFunction("LowerUdf", new LowerUdf)

    val t1 = env.fromCollection(sourceData)
      .toTable(tEnv)
      .as('id, 'band)
    val t2 = env.fromCollection(albumsData)
      .toTable(tEnv)
      .as('band, 'album)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sqlQuery =
      """
        | SELECT UpperUdf(T1.band), T2.album
        | FROM T1 JOIN T2
        |      ON  UpperUdf(T1.band) = UpperUdf(T2.band)
        |          AND LowerUdf(T1.band) = LowerUdf(T2.band)
      """.stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "PINK FLOYD,The Dark Side of the Moon",
      "PINK FLOYD,Wish You Were Here",
      "DEAD CAN DANCE,Aion"
    )

    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testSubexpressionEliminationWithCodeSplit(): Unit = {
    val configs = Array(new TableConfig(), new TableConfig)

    configs(0).getConf.setInteger(TableConfigOptions.SQL_CODEGEN_LENGTH_MAX,1)

    configs.foreach( tableConfig => {
      val tEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env, tableConfig)

      val t1 = env.fromCollection(sourceData)
        .toTable(tEnv)
        .as('id, 'band)

      tEnv.registerTable("T1", t1)

      val sqlQuery =
        """
          | SELECT
          |   id, UPPER(band), Lower(UPPER(band))
          | FROM T1
        """.stripMargin

      val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
      val sink = new TestingAppendSink
      result.addSink(sink)
      env.execute()

      val expected = List(
        "1,PINK FLOYD,pink floyd",
        "2,DEAD CAN DANCE,dead can dance",
        "3,THE DOORS,the doors",
        "4,DOU WEI,dou wei",
        "5,CUI JIAN,cui jian"
      )
      assertEquals(expected.sorted, sink.getAppendResults.sorted)
    })
  }

  @Test
  def testMapArray(): Unit = {
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_CODEGEN_LENGTH_MAX,1)

    val t1 = env.fromCollection(albumsData)
      .toTable(tEnv)
      .as('band, 'album)

    tEnv.registerTable("T1", t1)

    val sqlQuery =
    """
      | SELECT a['band'], a['album'], CARDINALITY(a), CARDINALITY(a), ELEMENT(arr), ELEMENT(arr)
      | FROM (
      |   SELECT STR_TO_MAP('band=Nirvana,album=Nevermind') as a,
      |           band, album, array['Nirvana'] as arr
      |   FROM T1
      | )
      | WHERE a['band'] = band AND a['album'] = album AND CARDINALITY(a) = 2
    """.stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "Nirvana,Nevermind,2,2,Nirvana,Nirvana"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

}

class UpperUdf extends ScalarFunction {
  def eval(value: String): String = {
    value.toUpperCase()
  }
}

class LowerUdf extends ScalarFunction {
  def eval(value: String): String = {
    value.toLowerCase()
  }
}

class NonDeterministicUdf extends ScalarFunction {

  override def isDeterministic: Boolean = false

  def eval(value: String): String = {
    value + " - " + (new scala.util.Random).nextInt(10000)
  }
}
