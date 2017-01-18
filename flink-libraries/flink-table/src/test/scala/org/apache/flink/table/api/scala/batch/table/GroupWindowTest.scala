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
package org.apache.flink.table.api.scala.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

class GroupWindowTest extends TableTestBase {

  //===============================================================================================
  // Tumbling Windows
  //===============================================================================================

  @Test(expected = classOf[ValidationException])
  def testProcessingTimeTumblingGroupWindowOverTime(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    table
      .groupBy('string)
      .window(Tumble over 50.milli)   // require a time attribute 
      .select('string, 'int.count)
  }

  @Test(expected = classOf[ValidationException])
  def testProcessingTimeTumblingGroupWindowOverCount(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    table
      .groupBy('string)
      .window(Tumble over 2.rows)   // require a time attribute
      .select('string, 'int.count)
  }

  @Test
  def testEventTimeTumblingGroupWindowOverCount(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Tumble over 2.rows on 'long)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      batchTableNode(0),
      term("groupBy", "string"),
      term("window", EventTimeTumblingGroupWindow(None, 'long, 2.rows)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeTumblingGroupWindowOverTime(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Tumble over 5.milli on 'long)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      batchTableNode(0),
      term("groupBy", "string"),
      term("window", EventTimeTumblingGroupWindow(None, 'long, 5.milli)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test(expected = classOf[ValidationException])
  def testAllProcessingTimeTumblingGroupWindowOverTime(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    table
      .window(Tumble over 50.milli)   // require a time attribute
      .select('string, 'int.count)
  }

  @Test(expected = classOf[ValidationException])
  def testAllProcessingTimeTumblingGroupWindowOverCount(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    table
      .window(Tumble over 2.rows)   // require a time attribute
      .select('int.count)
  }

  @Test
  def testAllEventTimeTumblingGroupWindowOverTime(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 5.milli on 'long)
      .select('int.count)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      unaryNode(
        "DataSetCalc",
        batchTableNode(0),
        term("select", "int", "long")
      ),
      term("window", EventTimeTumblingGroupWindow(None, 'long, 5.milli)),
      term("select", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllEventTimeTumblingGroupWindowOverCount(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 2.rows on 'long)
      .select('int.count)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      unaryNode(
        "DataSetCalc",
        batchTableNode(0),
        term("select", "int", "long")
      ),
      term("window", EventTimeTumblingGroupWindow(None, 'long, 2.rows)),
      term("select", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  //===============================================================================================
  // Sliding Windows
  //===============================================================================================

  @Test(expected = classOf[ValidationException])
  def testProcessingTimeSlidingGroupWindowOverTime(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    table
      .groupBy('string)
      .window(Slide over 50.milli every 50.milli) // require on a time attribute
      .select('string, 'int.count)
  }

  @Test(expected = classOf[ValidationException])
  def testProcessingTimeSlidingGroupWindowOverCount(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    table
      .groupBy('string)
      .window(Slide over 10.rows every 5.rows) // require on a time attribute
      .select('string, 'int.count)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTime(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Slide over 8.milli every 10.milli on 'long)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      batchTableNode(0),
      term("groupBy", "string"),
      term("window", EventTimeSlidingGroupWindow(None, 'long, 8.milli, 10.milli)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverCount(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Slide over 2.rows every 1.rows on 'long)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      batchTableNode(0),
      term("groupBy", "string"),
      term("window", EventTimeSlidingGroupWindow(None, 'long, 2.rows, 1.rows)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test(expected = classOf[ValidationException])
  def testAllProcessingTimeSlidingGroupWindowOverCount(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    table
      .window(Slide over 2.rows every 1.rows)   // require on a time attribute
      .select('int.count)
  }

  @Test
  def testAllEventTimeSlidingGroupWindowOverTime(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 8.milli every 10.milli on 'long)
      .select('int.count)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      unaryNode(
        "DataSetCalc",
        batchTableNode(0),
        term("select", "int", "long")
      ),
      term("window", EventTimeSlidingGroupWindow(None, 'long, 8.milli, 10.milli)),
      term("select", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllEventTimeSlidingGroupWindowOverCount(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'long)
      .select('int.count)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      unaryNode(
        "DataSetCalc",
        batchTableNode(0),
        term("select", "int", "long")
      ),
      term("window", EventTimeSlidingGroupWindow(None, 'long, 2.rows, 1.rows)),
      term("select", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  //===============================================================================================
  // Session Windows
  //===============================================================================================

  @Test
  def testEventTimeSessionGroupWindowOverTime(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Session withGap 7.milli on 'long)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      batchTableNode(0),
      term("groupBy", "string"),
      term("window", EventTimeSessionGroupWindow(None, 'long, 7.milli)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test(expected = classOf[ValidationException])
  def testProcessingTimeSessionGroupWindowOverTime(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Session withGap 7.milli) // require on a time attribute
      .select('string, 'int.count)
  }

}
