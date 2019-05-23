package org.apache.flink.table.runtime.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.{TableConfigOptions, TableException}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.{StreamingTestBase, TestSinkUtil, TestingAppendTableSink, TestingUpsertTableSink}

import org.junit.Assert.assertEquals
import org.junit.{Before, Test}

class Limit0RemoveITCase extends StreamingTestBase() {

  @Before
  def setup(): Unit = {
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_SOURCE_VALUES_INPUT_ENABLED, true)
  }

  @Test
  def testSimpleLimitRemove(): Unit = {
    val ds = env.fromCollection(Seq(1, 2, 3, 4, 5, 6))
    val table = ds.toTable(tEnv, 'a)
    tEnv.registerTable("MyTable", table)

    val sql = "SELECT * FROM MyTable LIMIT 0"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink())
    tEnv.writeToSink(result, sink)
    tEnv.execute()

    assertEquals(0, sink.getAppendResults.size)
  }

  @Test
  def testLimitRemoveWithOrderBy(): Unit = {
    val ds = env.fromCollection(Seq(1, 2, 3, 4, 5, 6))
    val table = ds.toTable(tEnv, 'a)
    tEnv.registerTable("MyTable", table)

    val sql = "SELECT * FROM MyTable ORDER BY a LIMIT 0"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink())
    tEnv.writeToSink(result, sink)
    tEnv.execute()

    assertEquals(0, sink.getAppendResults.size)
  }

  @Test
  def testLimitRemoveWithSelect(): Unit = {
    val ds = env.fromCollection(Seq(1, 2, 3, 4, 5, 6))
    val table = ds.toTable(tEnv, 'a)
    tEnv.registerTable("MyTable", table)

    val sql = "select a2 from (select cast(a as int) a2 from MyTable limit 0)"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink())
    tEnv.writeToSink(result, sink)
    tEnv.execute()

    assertEquals(0, sink.getAppendResults.size)
  }

  @Test
  def testLimitRemoveWithIn(): Unit = {
    val ds1 = env.fromCollection(Seq(1, 2, 3, 4, 5, 6))
    val table1 = ds1.toTable(tEnv, 'a)
    tEnv.registerTable("MyTable1", table1)

    val ds2 = env.fromCollection(Seq(1, 2, 3))
    val table2 = ds1.toTable(tEnv, 'a)
    tEnv.registerTable("MyTable2", table2)

    val sql = "SELECT * FROM MyTable1 WHERE a IN (SELECT a FROM MyTable2 LIMIT 0)"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink())
    tEnv.writeToSink(result, sink)
    tEnv.execute()

    assertEquals(0, sink.getAppendResults.size)
  }

  @Test
  def testLimitRemoveWithNotIn(): Unit = {
    val ds1 = env.fromCollection(Seq(1, 2, 3, 4, 5, 6))
    val table1 = ds1.toTable(tEnv, 'a)
    tEnv.registerTable("MyTable1", table1)

    val ds2 = env.fromCollection(Seq(1, 2, 3))
    val table2 = ds1.toTable(tEnv, 'a)
    tEnv.registerTable("MyTable2", table2)

    val sql = "SELECT * FROM MyTable1 WHERE a NOT IN (SELECT a FROM MyTable2 LIMIT 0)"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink())
    tEnv.writeToSink(result, sink)
    tEnv.execute()

    val expected = Seq("1", "2", "3", "4", "5", "6")
    assertEquals(expected, sink.getAppendResults.sorted)
  }

  @Test(expected = classOf[TableException])
  // TODO remove exception after translateToPlanInternal is implemented in StreamExecJoin
  def testLimitRemoveWithExists(): Unit = {
    val ds1 = env.fromCollection(Seq(1, 2, 3, 4, 5, 6))
    val table1 = ds1.toTable(tEnv, 'a)
    tEnv.registerTable("MyTable1", table1)

    val ds2 = env.fromCollection(Seq(1, 2, 3))
    val table2 = ds1.toTable(tEnv, 'a)
    tEnv.registerTable("MyTable2", table2)

    val sql = "SELECT * FROM MyTable1 WHERE EXISTS (SELECT a FROM MyTable2 LIMIT 0)"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingUpsertTableSink(Array(0)))
    tEnv.writeToSink(result, sink)
    tEnv.execute()

    assertEquals(0, sink.getRawResults.size)
  }

  @Test(expected = classOf[TableException])
  // TODO remove exception after translateToPlanInternal is implemented in StreamExecJoin
  def testLimitRemoveWithNotExists(): Unit = {
    val ds1 = env.fromCollection(Seq(1, 2, 3, 4, 5, 6))
    val table1 = ds1.toTable(tEnv, 'a)
    tEnv.registerTable("MyTable1", table1)

    val ds2 = env.fromCollection(Seq(1, 2, 3))
    val table2 = ds1.toTable(tEnv, 'a)
    tEnv.registerTable("MyTable2", table2)

    val sql = "SELECT * FROM MyTable1 WHERE NOT EXISTS (SELECT a FROM MyTable2 LIMIT 0)"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingUpsertTableSink(Array(0)))
    tEnv.writeToSink(result, sink)
    tEnv.execute()

    val expected = Seq("1", "2", "3", "4", "5", "6")
    assertEquals(expected, sink.getUpsertResults.sorted)
  }

  @Test
  def testLimitRemoveWithJoin(): Unit = {
    val ds1 = env.fromCollection(Seq(1, 2, 3, 4, 5, 6))
    val table1 = ds1.toTable(tEnv, 'a1)
    tEnv.registerTable("MyTable1", table1)

    val ds2 = env.fromCollection(Seq(1, 2, 3))
    val table2 = ds1.toTable(tEnv, 'a2)
    tEnv.registerTable("MyTable2", table2)

    val sql = "SELECT a1 FROM MyTable1 INNER JOIN (SELECT a2 FROM MyTable2 LIMIT 0) ON true"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink())
    tEnv.writeToSink(result, sink)
    tEnv.execute()

    assertEquals(0, sink.getAppendResults.size)
  }
}
