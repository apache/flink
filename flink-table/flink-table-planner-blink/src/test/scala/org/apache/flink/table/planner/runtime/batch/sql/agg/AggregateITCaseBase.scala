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
package org.apache.flink.table.planner.runtime.batch.sql.agg

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{TableException, Types}
import org.apache.flink.table.data.DecimalDataUtils
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.types.Row
import org.junit.{Before, Test}

import scala.collection.Seq

/**
  * Aggregate IT case base class.
  */
abstract class AggregateITCaseBase(testName: String) extends BatchTestBase {

  def prepareAggOp(): Unit

  @Before
  override def before(): Unit = {
    super.before()
    registerCollection("SmallTable3", smallData3, type3, "a, b, c", nullablesOfSmallData3)
    registerCollection("Table3", data3, type3, "a, b, c", nullablesOfData3)
    registerCollection("Table5", data5, type5, "d, e, f, g, h", nullablesOfData5)
    registerCollection("EmptyTable5", Seq(), type5, "d, e, f, g, h")
    registerCollection("NullTable3", nullData3, type3, "a, b, c", nullablesOfNullData3)
    registerCollection("AllNullTable3", allNullData3, type3, "a, b, c", allNullablesOfNullData3)
    registerCollection("NullTable5", nullData5, type5, "d, e, f, g, h", nullablesOfNullData5)
    registerCollection("DuplicateTable5", duplicateData5, type5, "d, e, f, g, h",
      nullablesOfDuplicateData5)
    registerCollection("GenericTypedTable3", genericData3, genericType3, "i, j, k",
      nullablesOfData3)

    prepareAggOp()
  }

  @Test
  def testTypedGroupByKey(): Unit = {
    checkResult(
      "SELECT j, sum(k) FROM GenericTypedTable3 GROUP BY i, j",
      Seq(
        row("1,1", 2),
        row("1,1", 2),
        row("10,1", 3)
      )
    )

    checkResult(
      "SELECT k, count(j) FROM GenericTypedTable3 GROUP BY i, k",
      Seq(
        row(1, 2),
        row(3, 1),
        row(2, 1)
      )
    )
  }

  @Test
  def testBigData(): Unit = {
    // for hash agg mode it will fallback
    val largeData5 = for (i <- 0 until 100000) yield row(i, 1L, 10, "Hallo", 1L)
    registerCollection("LargeTable5", largeData5, type5, "d, e, f, g, h")
    val expected = for (i <- 0 until 100000) yield row(i, "Hallo", 1L, 10, 1L)
    checkResult(
      "SELECT d, g, sum(e), avg(f), min(h) FROM LargeTable5 GROUP BY d, g",
      expected
    )

    // composite type group key fallback case
    val largeTypedData5 =
      for (i <- 0 until 100000) yield row(new JTuple2(i, i), 1L, 10, "Hallo", 1L)
    registerCollection("LargeTypedTable5", largeTypedData5, genericType5, "d, e, f, g, h")
    val expectedTypedData5 =
      for (i <- 0 until 100000) yield
        row(row(i, i), "Hallo", 1L, 10, 1L)
    checkResult(
      "SELECT d, g, sum(e), avg(f), min(h) FROM LargeTypedTable5 GROUP BY d, g",
      expectedTypedData5
    )

    // for hash agg mode it wont fallback
    val singleGrouplargeData5 = for (i <- 0 until 100000) yield row(999, 1L, 10, "Hallo", 1L)
    registerCollection("SingleGroupLargeTable5", singleGrouplargeData5, type5, "d, e, f, g, h")
    checkResult(
      "SELECT d, g, sum(e), avg(f), min(h) FROM SingleGroupLargeTable5 GROUP BY d, g",
      Seq(row(999, "Hallo", 100000L, 10, 1L))
    )
  }

  @Test
  def testGroupByOnly(): Unit = {
    checkResult(
      "SELECT h FROM Table5 GROUP BY h",
      Seq(
        row(1),
        row(2),
        row(3)
      )
    )
  }

  @Test
  def testSimpleAndDistinctAggWithCommonFilter(): Unit = {
    val sql =
      """
        |SELECT
        |   h,
        |   COUNT(1) FILTER(WHERE d > 1),
        |   COUNT(1) FILTER(WHERE d < 2),
        |   COUNT(DISTINCT e) FILTER(WHERE d > 1)
        |FROM Table5
        |GROUP BY h
        |""".stripMargin
    checkResult(
      sql,
      Seq(
        row(1,0,1,4),
        row(2,0,0,7),
        row(3,0,0,3)
      )
    )
  }

  @Test
  def testTwoPhasesAggregation(): Unit = {
    checkResult(
      "SELECT sum(d), avg(d), count(g), min(e), h FROM Table5 GROUP BY h",
      Seq(
        row(16, 16 / 5, 5, 1L, 1),
        row(26, 26 / 7, 7, 2L, 2),
        row(13, 13 / 3, 3, 6L, 3)
      )
    )
  }

  @Test
  def testPhaseAggregation(): Unit = {
    // TODO
  }

  @Test
  def testEmptyInputAggregation(): Unit = {
    checkResult("SELECT sum(d), avg(d), count(g), min(e) FROM EmptyTable5 GROUP BY h", Seq())
  }

  @Test
  def testNullGroupKeyAggregation(): Unit = {
    checkResult("SELECT sum(d), d, count(d) FROM NullTable5 GROUP BY d",
      Seq(
        row(1, 1, 1),
        row(25, 5, 5),
        row(null, null, 0),
        row(16, 4, 4),
        row(4, 2, 2),
        row(9, 3, 3)
      )
    )
  }

  @Test
  def testAggregationWithoutGroupby(): Unit = {
    checkResult(
      "SELECT sum(d), avg(d), count(g), min(e) FROM Table5",
      Seq(
        row(55, 55 / 15, 15, 1L)
      )
    )
  }

  @Test
  def testEmptyInputAggregationWithoutGroupby(): Unit = {
    checkResult(
      "SELECT sum(d), avg(d), count(g), min(e) FROM EmptyTable5",
      Seq(
        row(null, null, 0, null)
      )
    )
  }

  @Test
  def testAggregationAfterProjection(): Unit = {
    checkResult(
      "SELECT c, count(a) FROM " +
        "(SELECT d as a, f as b, h as c FROM Table5) GROUP BY c",
      Seq(
        row(1, 5),
        row(2, 7),
        row(3, 3)
      )
    )
  }

  @Test
  def testAggregationWithArithmetic(): Unit = {
    checkResult(
      "SELECT avg(d + 2) + 2 FROM Table5",
      Seq(
        row(85 / 15 + 2)
      )
    )
  }

  @Test
  def testGroupedDistinctAggregate(): Unit = {
    checkResult(
      "SELECT count(distinct g), h FROM DuplicateTable5 GROUP BY h",
      Seq(
        row(5, 1),
        row(5, 2),
        row(2, 3)
      )
    )
  }

  @Test
  def testDistinctAggregate(): Unit = {
    checkResult(
      "SELECT count(distinct h) FROM DuplicateTable5",
      Seq(
        row(3)
      )
    )
  }

  @Test
  def testUV(): Unit = {
    val data = (0 until 100).map { i => row("1", "1", s"${i % 10}", "1") }.toList
    val type4 = new RowTypeInfo(
      Types.STRING,
      Types.STRING,
      Types.STRING,
      Types.STRING)
    registerCollection(
      "src",
      data,
      type4,
      "a, b, c, d")

    val sql =
      s"""
         |SELECT
         |  a,
         |  b,
         |  COUNT(distinct c) as uv
         |FROM (
         |  SELECT
         |    a, b, c, d
         |  FROM
         |    src where b <> ''
         |  UNION ALL
         |  SELECT
         |    a, 'ALL' as b, c, d
         |  FROM
         |    src where b <> ''
         |) t
         |GROUP BY
         |  a, b
     """.stripMargin

    checkResult(sql, Seq(row("1", "1", 10), row("1", "ALL", 10)))
  }

  //
  // tests borrowed from org.apache.spark.sql.DataFrameAggregateSuite
  //

  private var newTableId = 0

  def checkQuery[T <: Product : TypeInformation](
      tableData: Seq[T],
      sqlQuery: String,
      expected: Seq[_ <: Product],
      tableName: String = "t")
  : Unit = {

    val toRow = (p: Product) =>
      Row.of(p.productIterator.map(_.asInstanceOf[AnyRef]).toArray: _*)
    val tableRows = tableData.map(toRow)

    val tupleTypeInfo = implicitly[TypeInformation[T]]
    val fieldInfos = tupleTypeInfo.getGenericParameters.values()
    import scala.collection.JavaConverters._
    val rowTypeInfo = new RowTypeInfo(fieldInfos.asScala.toArray: _*)

    newTableId += 1
    val tableName = "TestTableX" + newTableId

    val fields = rowTypeInfo.getFieldNames.mkString(",")

    registerCollection(tableName, tableRows, rowTypeInfo, fields)

    val sqlQueryX = sqlQuery.replace("TableName", tableName)

    checkResult(sqlQueryX, expected.map(toRow))
  }

  def big(i: Int): java.math.BigDecimal = new java.math.BigDecimal(i)

  def big(s: String): java.math.BigDecimal = new java.math.BigDecimal(s)

  val (b1, b2, b3) = (big(1), big(2), big(3))

  // with default scale for BigDecimal.class
  def bigX(i: Int): java.math.BigDecimal = big(i).setScale(
    DecimalDataUtils.DECIMAL_SYSTEM_DEFAULT.getScale)

  val (b1x, b2x, b3x) = (bigX(1), bigX(2), bigX(3))

  val bN = null: java.math.BigDecimal

  @Test
  def testGroupBy(): Unit = {
    checkQuery(
      Seq((1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)),
      "select f0, sum(f1) from TableName group by f0",
      Seq((1, 3), (2, 3), (3, 3))
    )
    checkQuery(
      Seq((1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)),
      "select sum(totB) from (select f0, sum(f1) as totB from TableName group by f0)",
      Seq(Tuple1(9))
    )
    checkQuery(
      Seq((1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)),
      "select f0, count(*) from TableName group by f0",
      Seq((1, 2L), (2, 2L), (3, 2L)) // count=>long
    )
    checkQuery(
      Seq(("a", 1, 0), ("b", 2, 4), ("a", 2, 3)),
      "select f0, min(f1), min(f2) from TableName group by f0",
      Seq(("a", 1, 0), ("b", 2, 4))
    )
    checkQuery(
      Seq((b1, b1), (b1, b2), (b2, b1), (b2, b2), (b3, b1), (b3, b2)),
      "select f0, sum(f1) from TableName group by f0",
      Seq((b1x, b3x), (b2x, b3x), (b3x, b3x))
    )
    // nulls in key/value
    checkQuery(
      Seq((b1, b1), (b1, bN), (b2, b1), (b2, bN), (b3, b1), (b3, b2), (bN, b2)),
      "select f0, sum(f1) from TableName group by f0",
      Seq((b1x, b1x), (b2x, b1x), (b3x, b3x), (bN, b2x))
    )
  }

  @Test(expected = classOf[TableException])
  def testCountCannotByMultiFields(): Unit = {
    checkQuery(
      Seq((1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)),
      "select count(distinct f0, f1) from TableName",
      Seq()
    )
  }

  @Test
  def testSpark17124(): Unit = {
    checkQuery(
      Seq(Tuple1(0L), Tuple1(1L)),
      "select f0, sum(f0), count(f0), min(f0) from TableName group by f0",
      Seq((0L, 0L, 1L, 0L), (1L, 1L, 1L, 1L))
    )
  }

  @Test
  def testGroupByRegexp(): Unit = {
    val expr = "regexp_extract(f0, '([a-z]+)\\[', 1)"
    checkQuery(
      Seq(("some[thing]", "random-string")),
      s"select $expr, count(*) from TableName group by $expr",
      Seq(("some", 1L))
    )
  }

  @Test
  def testRollup(): Unit = {
    checkQuery(
      Seq(
        ("dotNET", 2012, 10000.0),
        ("Java", 2012, 20000.0),
        ("dotNET", 2012, 5000.0),
        ("dotNET", 2013, 48000.0),
        ("Java", 2013, 30000.0)
      ),
      "select f0, f1, sum(f2) from TableName group by rollup(f0, f1)",
      Seq(
        ("Java", 2012, 20000.0),
        ("Java", 2013, 30000.0),
        ("Java", null, 50000.0),
        ("dotNET", 2012, 15000.0),
        ("dotNET", 2013, 48000.0),
        ("dotNET", null, 63000.0),
        (null, null, 113000.0)
      )
    )
  }

  @Test
  def testCube(): Unit = {
    checkQuery(
      Seq(
        ("dotNET", 2012, 10000.0),
        ("Java", 2012, 20000.0),
        ("dotNET", 2012, 5000.0),
        ("dotNET", 2013, 48000.0),
        ("Java", 2013, 30000.0)
      ),
      "select f0, f1, sum(f2) from TableName group by cube(f0, f1)",
      Seq(
        ("Java", 2012, 20000.0),
        ("Java", 2013, 30000.0),
        ("Java", null, 50000.0),
        ("dotNET", 2012, 15000.0),
        ("dotNET", 2013, 48000.0),
        ("dotNET", null, 63000.0),
        (null, 2012, 35000.0),
        (null, 2013, 78000.0),
        (null, null, 113000.0)
      )
    )
  }

  @Test
  def testGrouping(): Unit = {
    checkQuery(
      Seq(
        ("dotNET", 2012, 10000.0),
        ("Java", 2012, 20000.0),
        ("dotNET", 2012, 5000.0),
        ("dotNET", 2013, 48000.0),
        ("Java", 2013, 30000.0)
      ),
      "select f0, f1, grouping(f0), grouping(f1), grouping_id(f0,f1) " +
        "from TableName group by cube(f0, f1)",
      Seq(
        ("Java", 2012, 0, 0, 0),
        ("Java", 2013, 0, 0, 0),
        ("Java", null, 0, 1, 1),
        ("dotNET", 2012, 0, 0, 0),
        ("dotNET", 2013, 0, 0, 0),
        ("dotNET", null, 0, 1, 1),
        (null, 2012, 1, 0, 2),
        (null, 2013, 1, 0, 2),
        (null, null, 1, 1, 3)
      )
    )
  }

  @Test
  def testGroupingInsideWindowFunction(): Unit = {
    checkQuery(
      Seq(
        ("dotNET", 2012, 10000.0),
        ("Java", 2012, 20000.0),
        ("dotNET", 2012, 5000.0),
        ("dotNET", 2013, 48000.0),
        ("Java", 2013, 30000.0)
      ),
      "select f0, f1, sum(f2), grouping_id(f0, f1), " +
        "rank() over (partition by grouping_id(f0, f1) order by sum(f2)) " +
        "from TableName group by cube(f0, f1)",
      Seq(
        ("Java", 2012, 20000.0, 0, 2),
        ("Java", 2013, 30000.0, 0, 3),
        ("Java", null, 50000.0, 1, 1),
        ("dotNET", 2012, 15000.0, 0, 1),
        ("dotNET", 2013, 48000.0, 0, 4),
        ("dotNET", null, 63000.0, 1, 2),
        (null, 2012, 35000.0, 2, 1),
        (null, 2013, 78000.0, 2, 2),
        (null, null, 113000.0, 3, 1)
      )
    )
  }

  @Test
  def testRollupOverlappingColumns(): Unit = {
    checkQuery(
      Seq((1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)),
      "select f0+f1, f1, sum(f0-f1) from TableName group by rollup(f0+f1, f1)",
      Seq((2, 1, 0), (3, 2, -1), (3, 1, 1), (4, 2, 0), (4, 1, 2), (5, 2, 1),
        (2, null, 0), (3, null, 0), (4, null, 2), (5, null, 1), (null, null, 3))
    )

    checkQuery(
      Seq((1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)),
      "select f0, f1, sum(f1) from TableName group by rollup(f0, f1)",
      Seq((1, 1, 1), (1, 2, 2), (2, 1, 1), (2, 2, 2), (3, 1, 1), (3, 2, 2),
        (1, null, 3), (2, null, 3), (3, null, 3), (null, null, 9))
    )
  }

  @Test
  def testCubeOverlappingColumns(): Unit = {
    checkQuery(
      Seq((1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)),
      "select f0+f1, f1, sum(f0-f1) from TableName group by cube(f0+f1, f1)",
      Seq((2, 1, 0), (3, 2, -1), (3, 1, 1), (4, 2, 0), (4, 1, 2), (5, 2, 1),
        (2, null, 0), (3, null, 0), (4, null, 2), (5, null, 1), (null, 1, 3),
        (null, 2, 0), (null, null, 3))
    )

    checkQuery(
      Seq((1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)),
      "select f0, f1, sum(f1) from TableName group by cube(f0, f1)",
      Seq((1, 1, 1), (1, 2, 2), (2, 1, 1), (2, 2, 2), (3, 1, 1), (3, 2, 2),
        (1, null, 3), (2, null, 3), (3, null, 3), (null, 1, 3), (null, 2, 6),
        (null, null, 9))
    )
  }

  @Test
  def testAggWithoutGroups(): Unit = {
    checkQuery(
      Seq((1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)),
      "select sum(f1) from TableName",
      Seq(Tuple1(9))
    )
  }

  @Test
  def testAggWithoutGroupsAndFunctions(): Unit = {
    val one = Tuple1
    checkQuery(
      Seq((1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)),
      "select 1 from TableName",
      List.fill(6)(Tuple1(1))
    )
  }

  @Test
  def testAverage(): Unit = {
    checkQuery(
      Seq[(Integer, Integer)]((1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)),
      "select avg(f0), avg(f0) from TableName", // spark has mean(), but we don't
      Seq((2, 2))
    )

    checkQuery(
      Seq((b1, b1), (b1, b2), (b2, b1), (b2, b2), (b3, b1), (b3, b2)),
      "select avg(f0), sum(f0) from TableName",
      Seq((bigX(2), bigX(12)))
    )
    checkQuery(
      Seq((b1, b1), (b1, b2), (b2, b1), (b2, b2), (b3, b1), (b3, b2)),
      "select avg(cast (f0 as decimal(10,2))) from TableName",
      Seq(Tuple1(big("2.000000")))
    )
  }

  @Test
  def testAverageWithDistinct(): Unit = {
    checkQuery(
      Seq[(Integer, Integer)]((1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)),
      "select avg(f0), sum(distinct f0) from TableName",
      Seq((2, 6))
    )
    checkQuery(
      Seq((b1, b1), (b1, b2), (b2, b1), (b2, b2), (b3, b1), (b3, b2)),
      "select avg(f0), sum(distinct f0) from TableName",
      Seq((bigX(2), bigX(6)))
    )
    checkQuery(
      Seq((b1, b1), (b1, b2), (b2, b1), (b2, b2), (b3, b1), (b3, b2)),
      "select avg(f0), sum(distinct cast (f0 as decimal(10,2))) from TableName",
      Seq((bigX(2), big(6).setScale(2)))
    )
  }

  @Test
  def testNullAverage(): Unit = {
    val testData3: Seq[(Integer, Integer)] =
      Seq((1, null), (2, 2))

    checkQuery(
      testData3,
      "select avg(f1) from TableName",
      Seq(Tuple1(2))
    )
  }

  @Test
  def testNullAverageWithDistinct(): Unit = {
    val testData3: Seq[(Integer, Integer)] =
      Seq((1, null), (2, 2))

    checkQuery(
      testData3,
      "select avg(f1), count(distinct f1) from TableName",
      Seq((2, 1L))
    )
    checkQuery(
      testData3,
      "select avg(f1), sum(distinct f1) from TableName",
      Seq((2, 2))
    )
  }

  @Test
  def testZeroAvg(): Unit = {
    checkQuery(
      Seq[(Int, Int)](),
      "select avg(f0) from TableName",
      Seq(Tuple1(null))
    )
  }

  @Test
  def testZeroAvgWithDistinct(): Unit = {
    checkQuery(
      Seq[(Int, Int)](),
      "select avg(f0), sum(distinct f0) from TableName",
      Seq((null, null))
    )
  }

  @Test
  def testCount(): Unit = {
    checkQuery(
      Seq((1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)),
      "select count(f0), sum(distinct f0) from TableName",
      Seq((6L, 6))
    )
  }

  @Test
  def testNullCount(): Unit = {
    checkQuery(
      Seq[(Integer, Integer)]((1, null), (2, 2)),
      "select f0, count(f1) from TableName group by f0",
      Seq((1, 0L), (2, 1L))
    )
    checkQuery(
      Seq[(Integer, Integer)]((1, null), (2, 2)),
      "select f0, count(f0+f1) from TableName group by f0",
      Seq((1, 0L), (2, 1L))
    )
  }

  @Test
  def testNullCountWithDistinct(): Unit = {
    checkQuery(
      Seq[(Integer, Integer)]((1, null), (2, 2)),
      "select count(f0), count(f1), count(1), " +
          "count(distinct f0), count(distinct f1) from TableName",
      Seq((2L, 1L, 2L, 2L, 1L))
    )
    checkQuery(
      Seq[(Integer, Integer)]((1, null), (2, 2)),
      "select count(f1), count(distinct f1), sum(distinct f1) from TableName",
      Seq((1L, 1L, 2))
    )
  }

  @Test(expected = classOf[TableException])
  def testMultipleColumnDistinctCount(): Unit = {
    val testData = Seq(
      ("a", "b", "c"),
      ("a", "b", "c"),
      ("a", "b", "d"),
      ("x", "y", "z"),
      ("x", "q", null: String))

    checkQuery(
      testData,
      "select count(distinct f0, f1) from TableName",
      Seq(Tuple1(3L))
    )

    // Note: count distinct on multiple columns
    //       what if, in a row, some columns are null, some are not-null
    //       should the row be counted?
    //       Calcite doc says yes. Spark/MySQL says no.

    checkQuery(
      testData,
      "select count(distinct f0, f1, f2) from TableName",
      Seq(Tuple1(4L)) // NOTE: Spark and MySQL returns 3
    )
    checkQuery(
      testData,
      "select f0, count(distinct f1, f2) from TableName group by f0",
      Seq(("a", 2L), ("x", 2L)) // NOTE: Spark and MySQL returns 2
    )
  }

  @Test
  def testZeroCount(): Unit = {
    val emptyTable = Seq[(Int, Int)]()
    checkQuery(
      emptyTable,
      "select count(f0), sum(distinct f0) from TableName",
      Seq((0L, null))
    )
  }

  @Test
  def testStdDev(): Unit = {
    // NOTE: if f0 is INT type, our stddev functions return INT.
    checkQuery(
      Seq((1.0, 1), (1.0, 2), (2.0, 1), (2.0, 2), (3.0, 1), (3.0, 2)),
      "select stddev_pop(f0), stddev_samp(f0), stddev(f0) from TableName",
      Seq((math.sqrt(4.0 / 6.0), math.sqrt(4.0 / 5.0), math.sqrt(4.0 / 5.0)))
    )
  }

  @Test
  def test1RowStdDev(): Unit = {
    checkQuery(Seq((1.0, 1)),
      "select stddev_pop(f0), stddev_samp(f0), stddev(f0) from TableName",
      Seq((0.0, null, null))
    )
  }

  @Test
  def testVariance(): Unit = {
    checkQuery(Seq((1.0, 1), (2.0, 1)),
      "select var_pop(f0), var_samp(f0), variance(f0) from TableName",
      Seq((0.25, 0.5, 0.5))
    )
  }

  @Test
  def test1RowVariance(): Unit = {
    checkQuery(Seq((1.0, 1)),
      "select var_pop(f0), var_samp(f0), variance(f0) from TableName",
      Seq((0.0, null, null))
    )
  }

  @Test
  def testZeroStdDev(): Unit = {
    val emptyTable = Seq[(Int, Int)]()
    checkQuery(
      emptyTable,
      "select stddev_pop(f0), stddev_samp(f0) from TableName",
      Seq((null, null))
    )
  }

  @Test
  def testZeroSum(): Unit = {
    val emptyTable = Seq[(Int, Int)]()
    checkQuery(
      emptyTable,
      "select sum(f0) from TableName",
      Seq(Tuple1(null))
    )
  }

  @Test
  def testZeroSumDistinct(): Unit = {
    val emptyTable = Seq[(Int, Int)]()
    checkQuery(
      emptyTable,
      "select sum(distinct f0) from TableName",
      Seq(Tuple1(null))
    )
  }

  @Test
  def testMoments(): Unit = {
    checkQuery(
      Seq((1.0, 1), (1.0, 2), (2.0, 1), (2.0, 2), (3.0, 1), (3.0, 2)),
      "select var_pop(f0), var_samp(f0) from TableName",
      Seq((4.0 / 6.0, 4.0 / 5.0))
    )
    // todo: Spark has skewness() and kurtosis()
  }

  @Test
  def testZeroMoments(): Unit = {
    checkQuery(
      Seq((1.0, 2.0)),
      "select stddev_samp(f0), stddev_pop(f0), var_samp(f0), var_pop(f0) from TableName",
      Seq((null, 0.0, null, 0.0))
    )
    // todo: Spark returns Double.NaN instead of null
  }

  @Test
  def testNullMoments(): Unit = {
    checkQuery(
      Seq[(Int, Int)](),
      "select stddev_samp(f0), stddev_pop(f0), var_samp(f0), var_pop(f0) from TableName",
      Seq((null, null, null, null))
    )
  }

  // NOTE: select from values -- supported by Spark, but not Blink
  //       "select sum(a) over () from values 1.0, 2.0, 3.0 T(a)"

  @Test
  def testDecimalSumAvgOverWindow(): Unit = {
    checkQuery(
      Seq(Tuple1(1.0), Tuple1(2.0), Tuple1(3.0)),
      "select sum(f0) over () from TableName",
      Seq(Tuple1(6.0), Tuple1(6.0), Tuple1(6.0))
    )
    checkQuery(
      Seq(Tuple1(1.0), Tuple1(2.0), Tuple1(3.0)),
      "select avg(f0) over () from TableName",
      Seq(Tuple1(2.0), Tuple1(2.0), Tuple1(2.0))
    )
  }

  @Test
  def testDecimals(): Unit = {
    checkQuery(
      Seq((b1, b1), (b1, b2), (b2, b1), (b2, b2), (b3, b1), (b3, b2)),
      "select cast (f0 as decimal(10,2)), avg(cast (f1 as decimal(10,2))) " +
        " from TableName group by cast (f0 as decimal(10,2))",
      Seq((big("1.00"), big("1.500000")), (big("2.00"), big("1.500000")),
        (big("3.00"), big("1.500000")))
    )
  }

  @Test
  def testLimitPlusAgg(): Unit = {
    checkQuery(
      Seq(("a", 1), ("b", 2), ("c", 1), ("d", 5)),
      "select f0, count(*) from (select * from TableName limit 2) group by f0",
      Seq(("a", 1L), ("b", 1L))
    )
  }

  // TODO: supports `pivot`.

  @Test
  def testGroupByLiteral(): Unit = {
    checkQuery(
      Seq((1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)),
      "select 3, 4, sum(f1) from TableName group by 1, 2",
      Seq((3, 4, 9))
    )
    checkQuery(
      Seq((1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)),
      "SELECT 3, 4, SUM(f1) from TableName GROUP BY 3, 4",
      Seq((3, 4, 9))
    )
    // NOTE: Spark runs this query
    //       "SELECT 3 AS c, 4 AS d, SUM(f1) FROM t GROUP BY c, d"
    // with GROUP-BY clause referencing alias in SELECT clause.
    // that doesn't make sense, and we do not support it.
  }

  // TODO support csv
//  @Test
//  def testMultiGroupBys(): Unit = {
//    val csvPath = CommonTestData.writeToTempFile(
//      "7369,SMITH,CLERK,7902,1980-12-17,800.00,,20$" +
//        "7499,ALLEN,SALESMAN,7698,1981-02-20,1600.00,300.00,30$" +
//        "7521,WARD,SALESMAN,7698,1981-02-22,1250.00,500.00,30",
//      "csv-test", "tmp")
//    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal("emp",
//      CsvTableSource.builder()
//        .path(csvPath)
//        .fields(Array("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno"),
//          Array(DataTypes.INT, DataTypes.STRING, DataTypes.STRING, DataTypes.INT, DataTypes.DATE,
//            DataTypes.DOUBLE, DataTypes.DOUBLE, DataTypes.INT))
//        .enableEmptyColumnAsNull()
//        .fieldDelimiter(",")
//        .lineDelimiter("$")
//        .uniqueKeys(Set(Set("empno").asJava).asJava)
//        .build())
//
//    checkResult(
//      """
//        |SELECT empno, ename, hiredate, MIN(sal), MAX(comm)
//        |FROM (SELECT empno, ename, hiredate, AVG(sal) AS sal, MIN(comm) AS comm
//        |FROM emp
//        |GROUP BY ename, empno, hiredate)
//        |GROUP BY empno, ename, hiredate
//        |FETCH NEXT 10 ROWS ONLY
//      """.stripMargin,
//      Seq(row(7369, "SMITH", "1980-12-17", 800.0, null),
//        row(7499, "ALLEN", "1981-02-20", 1600.0, 300.0),
//        row(7521, "WARD", "1981-02-22", 1250.0, 500.0)))
//  }
}
