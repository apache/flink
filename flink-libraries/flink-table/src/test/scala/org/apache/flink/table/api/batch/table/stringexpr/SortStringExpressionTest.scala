package org.apache.flink.table.api.batch.table.stringexpr

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test

class SortStringExpressionTest extends TableTestBase {

  @Test
  def testOrdering(): Unit = {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3")

    val t1 = t.select('_1 as 'a, '_2 as 'b, '_3 as 'c).orderBy('a)
    val t2 = t.select("_1 as a, _2 as b, _3 as c").orderBy("a")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testExplicitAscendOrdering(): Unit = {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3")

    val t1 = t.select('_1, '_2).orderBy('_1.asc)
    val t2 = t.select("_1, _2").orderBy("_1.asc")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testExplicitDescendOrdering(): Unit = {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3")

    val t1 = t.select('_1, '_2).orderBy('_1.desc)
    val t2 = t.select("_1, _2").orderBy("_1.desc")

    verifyTableEquals(t1, t2)
  }
}
