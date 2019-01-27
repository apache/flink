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

package org.apache.flink.table.runtime.stream.table

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.plan.optimize._
import org.apache.flink.table.runtime.utils.StreamingWithMiniBatchTestBase.MiniBatchMode
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.runtime.utils._
import org.apache.flink.table.util.{TableSchemaUtil, TestFlinkLogicalLastRowRule, TestTableSourceWithTime, TestTableSourceWithUniqueKeys}
import org.apache.flink.types.Row

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableSet

import org.apache.calcite.runtime.SqlFunctions.{internalToTimestamp => toTimestamp}
import org.apache.calcite.tools.RuleSets
import org.junit.Assert.assertEquals
import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.lang.{Integer => JInt, Long => JLong}

import scala.collection.mutable

@RunWith(classOf[Parameterized])
class LastRowITCase(miniBatch: MiniBatchMode, mode: StateBackendMode)
  extends StreamingWithMiniBatchTestBase(miniBatch, mode) {

  @Before
  override def before(): Unit = {
    super.before()
    FailingCollectionSource.failedBefore = true
  }

  @Test
  def testWithPkUpdateGenerateRetraction(): Unit = {
    val tableName = "MyTable"

    injectLastRowRule(tEnv)

    // set parallelism to 1 to avoid out of order
    env.setParallelism(1)
    tEnv.registerTableSource(tableName, new TestTableSourceWithUniqueKeys(
      StreamTestData.get3TupleData,
      Array("a", "pk", "c"),
      Array(0, 1, 2),
      ImmutableSet.of(ImmutableSet.copyOf(Array[String]("pk")))))

    val sink = new TestingRetractSink
    tEnv.scan(tableName)
      .groupBy('pk)
      .select('pk, 'a.count)
      .toRetractStream[Row]
      .addSink(sink).setParallelism(1)
    env.execute()

    val expected = mutable.MutableList("1,1", "2,1", "3,1", "4,1", "5,1", "6,1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testWithPkUpdateWithoutGenerateRetraction(): Unit = {
    val tableName = "MyTable"

    injectLastRowRule(tEnv)

    // set parallelism to 1 to avoid out of order
    env.setParallelism(1)
    tEnv.registerTableSource(tableName, new TestTableSourceWithUniqueKeys(
      StreamTestData.get3TupleData,
      Array("a", "pk", "c"),
      Array(0, 1, 2),
      ImmutableSet.of(ImmutableSet.copyOf(Array[String]("pk")))))

    val sink = new TestingUpsertTableSink(Array(0))
    tEnv.scan(tableName).select('pk, 'a).writeToSink(sink)
    env.execute()

    val expected = mutable.MutableList("1,1", "2,3", "3,6", "4,10", "5,15", "6,21")
    assertEquals(expected.sorted, sink.getUpsertResults.sorted)
  }

  @Test
  def testWithPkOrderUpdateWithRetraction(): Unit = {
    val tableName = "MyTable"
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    injectLastRowRule(tEnv)

    val data = Seq(
      Row.of(toTimestamp(3L), new JLong(2L), "Hello world", new JInt(3)),
      Row.of(toTimestamp(2L), new JLong(2L), "Hello", new JInt(2)),
      Row.of(toTimestamp(6L), new JLong(3L), "Luke Skywalker", new JInt(6)),
      Row.of(toTimestamp(5L), new JLong(3L), "I am fine.", new JInt(5)),
      Row.of(toTimestamp(7L), new JLong(4L), "Comment#1", new JInt(7)),
      Row.of(toTimestamp(9L), new JLong(4L), "Comment#3", new JInt(9)),
      Row.of(toTimestamp(10L), new JLong(4L), "Comment#4", new JInt(10)),
      Row.of(toTimestamp(8L), new JLong(4L), "Comment#2", new JInt(8)),
      Row.of(toTimestamp(1L), new JLong(1L), "Hi", new JInt(1)),
      Row.of(toTimestamp(4L), new JLong(3L), "Helloworld, how are you?", new JInt(4))
    )


    val rowType = new RowTypeInfo(
      Array(Types.SQL_TIMESTAMP, Types.LONG, Types.STRING, Types.INT)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("a", "pk", "c", "d"))

    val schema = TableSchemaUtil.builderFromDataType(rowType)
          .primaryKey("pk").build()

    tEnv.registerTableSource(tableName, new TestTableSourceWithTime(
      schema,
      rowType,
      data,
      "a"
    ))

    val sink = new TestingRetractSink
    tEnv.scan(tableName)
      .groupBy('pk)
      .select('pk, 'd.sum)
      .toRetractStream[Row]
      .addSink(sink).setParallelism(1)
    env.execute()

    val expected = mutable.MutableList("1,1", "2,3", "3,6", "4,10")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testWithPkOrderUpdateWithoutRetraction(): Unit = {
    val tableName = "MyTable"
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    injectLastRowRule(tEnv)

    val data = Seq(
      Row.of(toTimestamp(3L), new JLong(2L), "Hello world", new JInt(3)),
      Row.of(toTimestamp(2L), new JLong(2L), "Hello", new JInt(2)),
      Row.of(toTimestamp(6L), new JLong(3L), "Luke Skywalker", new JInt(6)),
      Row.of(toTimestamp(5L), new JLong(3L), "I am fine.", new JInt(5)),
      Row.of(toTimestamp(7L), new JLong(4L), "Comment#1", new JInt(7)),
      Row.of(toTimestamp(9L), new JLong(4L), "Comment#3", new JInt(9)),
      Row.of(toTimestamp(10L), new JLong(4L), "Comment#4", new JInt(10)),
      Row.of(toTimestamp(8L), new JLong(4L), "Comment#2", new JInt(8)),
      Row.of(toTimestamp(1L), new JLong(1L), "Hi", new JInt(1)),
      Row.of(toTimestamp(4L), new JLong(3L), "Helloworld, how are you?", new JInt(4))
    )

    val rowType = new RowTypeInfo(
      Array(Types.SQL_TIMESTAMP, Types.LONG, Types.STRING, Types.INT)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("a", "pk", "c", "d"))
    val schema = TableSchemaUtil.builderFromDataType(rowType)
          .primaryKey("pk").build()

    tEnv.registerTableSource(tableName, new TestTableSourceWithTime(
      schema,
      rowType,
      data,
      "a"
    ))


    val sink = new TestingUpsertTableSink(Array(0))
    tEnv.scan(tableName)
      .select('pk, 'd)
      .writeToSink(sink)
    env.execute()

    val expected = mutable.MutableList("1,1", "2,3", "3,6", "4,10")
    assertEquals(expected.sorted, sink.getUpsertResults.sorted)
  }

  def injectLastRowRule(tEnv: TableEnvironment): Unit = {
    val programs = FlinkStreamPrograms.buildPrograms(tEnv.getConfig.getConf)

    programs.get(FlinkStreamPrograms.LOGICAL_REWRITE)
      .getOrElse(throw new RuntimeException(s"${FlinkStreamPrograms.LOGICAL_REWRITE} not exist"))
      .asInstanceOf[FlinkHepRuleSetProgram[StreamOptimizeContext]]
      .add(RuleSets.ofList(TestFlinkLogicalLastRowRule.INSTANCE))

    val builder = CalciteConfig.createBuilder(tEnv.getConfig.getCalciteConfig)
      .replaceStreamPrograms(programs)
    tEnv.getConfig.setCalciteConfig(builder.build())
  }
}
