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

package org.apache.flink.table.plan.rules

import org.apache.calcite.rel.RelNode
import org.apache.flink.table.api.Table
import org.apache.flink.table.plan.nodes.datastream._
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Assert._
import org.junit.{Ignore, Test}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._


class RetractionRulesTest extends TableTestBase {

  def streamTestForRetractionUtil(): StreamTableTestForRetractionUtil = {
    new StreamTableTestForRetractionUtil()
  }

  @Test
  def testSelect(): Unit = {
    val util = streamTestForRetractionUtil()
    val table = util.addTable[(String, Int)]('word, 'number)

    val resultTable = table.select('word, 'number)

    val expected = s"DataStreamScan(false, Acc)"

    util.verifyTableTrait(resultTable, expected)
  }

  // one level unbounded groupBy
  @Test
  def testGroupBy(): Unit = {
    val util = streamTestForRetractionUtil()
    val table = util.addTable[(String, Int)]('word, 'number)
    val defaultStatus = "false, Acc"

    val resultTable = table
      .groupBy('word)
      .select('number.count)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupAggregate",
          "DataStreamScan(true, Acc)",
          s"$defaultStatus"
        ),
        s"$defaultStatus"
      )

    util.verifyTableTrait(resultTable, expected)
  }

  // two level unbounded groupBy
  @Test
  def testTwoGroupBy(): Unit = {
    val util = streamTestForRetractionUtil()
    val table = util.addTable[(String, Int)]('word, 'number)
    val defaultStatus = "false, Acc"

    val resultTable = table
      .groupBy('word)
      .select('word, 'number.count as 'count)
      .groupBy('count)
      .select('count, 'count.count as 'frequency)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupAggregate",
          unaryNode(
            "DataStreamCalc",
            unaryNode(
              "DataStreamGroupAggregate",
              "DataStreamScan(true, Acc)",
              "true, AccRetract"
            ),
            "true, AccRetract"
          ),
          s"$defaultStatus"
        ),
        s"$defaultStatus"
      )

    util.verifyTableTrait(resultTable, expected)
  }

  // group window
  @Test
  def testGroupWindow(): Unit = {
    val util = streamTestForRetractionUtil()
    val table = util.addTable[(String, Int)]('word, 'number, 'rowtime.rowtime)
    val defaultStatus = "false, Acc"

    val resultTable = table
      .window(Tumble over 50.milli on 'rowtime as 'w)
      .groupBy('w, 'word)
      .select('word, 'number.count as 'count)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowAggregate",
          "DataStreamScan(true, Acc)",
          s"$defaultStatus"
        ),
        s"$defaultStatus"
      )

    util.verifyTableTrait(resultTable, expected)
  }

  // group window after unbounded groupBy
  @Test
  @Ignore // cannot pass rowtime through non-windowed aggregation
  def testGroupWindowAfterGroupBy(): Unit = {
    val util = streamTestForRetractionUtil()
    val table = util.addTable[(String, Int)]('word, 'number, 'rowtime.rowtime)
    val defaultStatus = "false, Acc"

    val resultTable = table
      .groupBy('word)
      .select('word, 'number.count as 'count)
      .window(Tumble over 50.milli on 'rowtime as 'w)
      .groupBy('w, 'count)
      .select('count, 'count.count as 'frequency)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowAggregate",
          unaryNode(
            "DataStreamCalc",
            unaryNode(
              "DataStreamGroupAggregate",
              "DataStreamScan(true, Acc)",
              "true, AccRetract"
            ),
            "true, AccRetract"
          ),
          s"$defaultStatus"
        ),
        s"$defaultStatus"
      )

    util.verifyTableTrait(resultTable, expected)
  }

  // over window
  @Test
  def testOverWindow(): Unit = {
    val util = streamTestForRetractionUtil()
    util.addTable[(String, Int)]("T1", 'word, 'number, 'proctime.proctime)
    val defaultStatus = "false, Acc"

    val sqlQuery =
      "SELECT " +
        "word, count(number) " +
        "OVER (PARTITION BY word ORDER BY proctime " +
        "ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW)" +
        "FROM T1"

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          "DataStreamScan(true, Acc)",
          s"$defaultStatus"
        ),
        s"$defaultStatus"
      )

    util.verifySqlTrait(sqlQuery, expected)
  }


  // over window after unbounded groupBy
  @Test
  @Ignore // cannot pass rowtime through non-windowed aggregation
  def testOverWindowAfterGroupBy(): Unit = {
    val util = streamTestForRetractionUtil()
    util.addTable[(String, Int)]("T1", 'word, 'number, 'proctime.proctime)
    val defaultStatus = "false, Acc"

    val sqlQuery =
      "SELECT " +
        "_count, count(word) " +
        "OVER (PARTITION BY _count ORDER BY proctime " +
        "ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW)" +
        "FROM " +
        "(SELECT word, count(number) as _count FROM T1 GROUP BY word) "

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            unaryNode(
              "DataStreamGroupAggregate",
              "DataStreamScan(true, Acc)",
              "true, AccRetract"
            ),
            "true, AccRetract"
          ),
          s"$defaultStatus"
        ),
        s"$defaultStatus"
      )

    util.verifySqlTrait(sqlQuery, expected)
  }

  // test binaryNode
  @Test
  def testBinaryNode(): Unit = {
    val util = streamTestForRetractionUtil()
    val lTable = util.addTable[(String, Int)]('word, 'number)
    val rTable = util.addTable[(String, Long)]('word_r, 'count_r)
    val defaultStatus = "false, Acc"

    val resultTable = lTable
      .groupBy('word)
      .select('word, 'number.count as 'count)
      .unionAll(rTable)
      .groupBy('count)
      .select('count, 'count.count as 'frequency)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupAggregate",
          unaryNode(
            "DataStreamCalc",
            binaryNode(
              "DataStreamUnion",
              unaryNode(
                "DataStreamCalc",
                unaryNode(
                  "DataStreamGroupAggregate",
                  "DataStreamScan(true, Acc)",
                  "true, AccRetract"
                ),
                "true, AccRetract"
              ),
              "DataStreamScan(true, Acc)",
              "true, AccRetract"
            ),
            "true, AccRetract"
          ),
          s"$defaultStatus"
        ),
        s"$defaultStatus"
      )

    util.verifyTableTrait(resultTable, expected)
  }
}

class StreamTableTestForRetractionUtil extends StreamTableTestUtil {

  def verifySqlTrait(query: String, expected: String): Unit = {
    verifyTableTrait(tableEnv.sql(query), expected)
  }

  def verifyTableTrait(resultTable: Table, expected: String): Unit = {
    val relNode = resultTable.getRelNode
    val optimized = tableEnv.optimize(relNode, updatesAsRetraction = false)
    val actual = TraitUtil.toString(optimized)
    assertEquals(
      expected.split("\n").map(_.trim).mkString("\n"),
      actual.split("\n").map(_.trim).mkString("\n"))
  }
}

object TraitUtil {
  def toString(rel: RelNode): String = {
    val className = rel.getClass.getSimpleName
    var childString: String = ""
    var i = 0
    while (i < rel.getInputs.size()) {
      childString += TraitUtil.toString(rel.getInput(i))
      i += 1
    }

    val retractString = rel.getTraitSet.getTrait(UpdateAsRetractionTraitDef.INSTANCE).toString
    val accModetString = rel.getTraitSet.getTrait(AccModeTraitDef.INSTANCE).toString

    s"""$className($retractString, $accModetString)
       |$childString
       |""".stripMargin.stripLineEnd
  }
}

