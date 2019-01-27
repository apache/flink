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

package org.apache.flink.table.runtime.batch.sql.subquery

import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.types.{DataTypes, DecimalType, InternalType}
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.row
import org.apache.flink.table.sources.csv.CsvTableSource
import org.junit.{Before, Test}

import scala.collection.Seq

class ScalaSubqueryITCase extends BatchTestBase {

  def getDataFile(tableName: String): String = {
    getClass.getResource(s"/runtime/batch/$tableName").getFile
  }

  private lazy val tableSchema = Seq[InternalType](
    DataTypes.STRING,
    DataTypes.SHORT,
    DataTypes.INT,
    DataTypes.LONG,
    DataTypes.FLOAT,
    DataTypes.DOUBLE,
    DecimalType.USER_DEFAULT,
    DataTypes.TIMESTAMP,
    DataTypes.DATE).toArray

  private val defaultNullableSeq = Array.fill(tableSchema.length)(true)

  private val tableNames = Seq("t1", "t2", "t3")

  private def fieldList(tableName: String): Array[String] = {
    val columnSuffix = "a,b,c,d,e,f,g,h,i".split(",")
    val fields = new Array[String](columnSuffix.length)
    columnSuffix.zipWithIndex.foreach(item => fields(item._2) = tableName + item._1)
    fields
  }

  @Before
  def before(): Unit = {
    for (tableName <- tableNames) {
      val tableSource = new CsvTableSource(
        getDataFile(tableName),
        fieldList(tableName),
        tableSchema,
        defaultNullableSeq,
        fieldDelim = "|",
        rowDelim = "\n",
        quoteCharacter = null,
        ignoreFirstLine = false,
        ignoreComments = null,
        lenient = false,
        charset = "UTF-8",
        emptyColumnAsNull = true)
      tEnv.registerTableSource(tableName, tableSource)
    }
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)
  }

  @Test
  def testTC101(): Unit = {
    checkResult(
      """
        |-- TC 01.01
        |            SELECT  (SELECT min(t3d) FROM t3) min_t3d,
        |                    (SELECT max(t2h) FROM t2) max_t2h
        |            FROM   t1
        |            WHERE  t1a = 'val1c'
      """.stripMargin, Seq(
        row(10, "2017-05-04 01:01:00.0")))
  }

  @Test
  def testTC102(): Unit = {
    checkResult(
      """
        |-- TC 01.02
        |            SELECT   t1a, count(*)
        |            FROM     t1
        |            WHERE    t1c IN (SELECT   (SELECT min(t3c) FROM t3)
        |                            FROM     t2
        |                            GROUP BY t2g
        |                            HAVING   count(*) > 1)
        |            GROUP BY t1a
      """.stripMargin, Seq(
        row("val1a", 2)))
  }

  @Test
  def testTC103(): Unit = {
    checkResult(
      """
        |-- TC 01.03
        |            SELECT (SELECT min(t3d) FROM t3) min_t3d,
        |                    cast(null as TIMESTAMP)
        |            FROM   t1
        |            WHERE  t1a = 'val1c'
        |            UNION
        |            SELECT cast(null as BIGINT),
        |                    (SELECT max(t2h) FROM t2) max_t2h
        |            FROM   t1
        |            WHERE  t1a = 'val1c'
      """.stripMargin, Seq(
        row(10, null),
        row(null, "2017-05-04 01:01:00.0")))
  }

  @Test
  def testTC104(): Unit = {
    checkResult(
      """
        |-- TC 01.04
        |            SELECT (SELECT min(t3c) FROM t3) min_t3d
        |            FROM   t1
        |            WHERE  t1a = 'val1a'
        |            INTERSECT
        |            SELECT (SELECT min(t2c) FROM t2) min_t2d
        |            FROM   t1
        |            WHERE  t1a = 'val1d'
      """.stripMargin, Seq(
        row(12)))
  }

  @Test
  def testTC105(): Unit = {
    checkResult(
      """
        |-- TC 01.05
        |            SELECT q1.t1a, q2.t2a, q1.min_t3d, q2.avg_t3d
        |            FROM   (SELECT t1a, (SELECT min(t3d) FROM t3) min_t3d
        |                    FROM   t1
        |                    WHERE  t1a IN ('val1e', 'val1c')) q1
        |                    FULL OUTER JOIN
        |                    (SELECT t2a, (SELECT avg(t3d) FROM t3) avg_t3d
        |                      FROM   t2
        |                      WHERE  t2a IN ('val1c', 'val2a')) q2
        |            ON     q1.t1a = q2.t2a
        |            AND    q1.min_t3d < q2.avg_t3d
      """.stripMargin, Seq(
        row(null, "val2a", null, 2410d/12),
        row("val1c", "val1c", 10, 2410d/12),
        row("val1c", "val1c", 10, 2410d/12),
        row("val1e", null, 10, null),
        row("val1e", null, 10, null),
        row("val1e", null, 10, null)))
  }

  @Test(expected = classOf[RuntimeException])
  def testTC203(): Unit = {
    checkResult(
      """
        |-- TC 02.03
        |            SELECT t1a, t1b
        |            FROM   t1
        |            WHERE  NOT EXISTS (SELECT (SELECT max(t2b)
        |                                      FROM   t2 LEFT JOIN t1
        |                                      ON     t2a = t1a
        |                                      WHERE  t2c = t3c) dummy
        |                              FROM   t3
        |                              WHERE  t3b < (SELECT max(t2b)
        |                                            FROM   t2 LEFT JOIN t1
        |                                            ON     t2a = t1a
        |                                            WHERE  t2c = t3c)
        |                              AND    t3a = t1a)
      """.stripMargin, Seq(
        row("val1a", 16),
        row("val1a", 16),
        row("val1a", 6),
        row("val1a", 6),
        row("val1c", 8),
        row("val1d", 10),
        row("val1d", null),
        row("val1d", null),
        row("val1e", 10),
        row("val1e", 10),
        row("val1e", 10)))
  }

  @Test(expected = classOf[RuntimeException])
  def testTC201(): Unit = {
    checkResult(
      """
        |-- TC 02.01
        |            SELECT (SELECT min(t3d) FROM t3 WHERE t3.t3a = t1.t1a) min_t3d,
        |                   (SELECT max(t2h) FROM t2 WHERE t2.t2a = t1.t1a) max_t2h
        |            FROM   t1
        |            WHERE  t1a = 'val1b'
      """.stripMargin, Seq(
        row(19, "2017-05-04 01:01:00.0")))
  }

  @Test(expected = classOf[RuntimeException])
  def testTC205(): Unit = {
    checkResult(
      """
        |-- TC 02.05
        |            SELECT (SELECT max(t2h) FROM t2
        |                LEFT OUTER JOIN t1 ttt
        |                ON t2.t2a=t1.t1a) max_t2h
        |            FROM   t1
        |            WHERE  t1a = 'val1b'
      """.stripMargin, Seq(
        row("2017-05-03 17:01:00.0")))
  }
}
