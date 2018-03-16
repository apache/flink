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

import java.lang.{Boolean => JBool}

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.operators.LegacyKeyedProcessOperator
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.runtime.harness.HarnessTestBase
import org.apache.flink.table.sinks.{QueryableStateProcessFunction, RowKeySelector}
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test

class QueryableTableSinkTest extends HarnessTestBase {
  @Test
  def testRowSelector(): Unit = {
    val keyTypes = Array(TypeInformation.of(classOf[List[Int]]),
      TypeInformation.of(classOf[String]))
    val selector = new RowKeySelector(Array(0, 2), new RowTypeInfo(keyTypes:_*))

    val src = Row.of(List(1), "a", "b")
    val key = selector.getKey(JTuple2.of(true, src))

    assertEquals(Row.of(List(1), "b"), key)
  }

  @Test
  def testProcessFunction(): Unit = {
    val queryConfig = new StreamQueryConfig()
      .withIdleStateRetentionTime(Time.milliseconds(2), Time.milliseconds(10))

    val keys = Array("id")
    val keyType = new RowTypeInfo(TypeInformation.of(classOf[String]))
    val fieldNames = Array("id", "is_manager", "name")
    val fieldTypes: Array[TypeInformation[_]] = Array(
      TypeInformation.of(classOf[String]).asInstanceOf[TypeInformation[_]],
      TypeInformation.of(classOf[JBool]).asInstanceOf[TypeInformation[_]],
      TypeInformation.of(classOf[String]).asInstanceOf[TypeInformation[_]])
    val func = new QueryableStateProcessFunction("test", queryConfig, keys, fieldNames, fieldTypes)

    val operator = new LegacyKeyedProcessOperator[Row, JTuple2[JBool, Row], Void](func)

    val testHarness = createHarnessTester(operator,
      new RowKeySelector(Array(0), keyType),
      keyType)

    testHarness.open()

    val stateDesc1 = new ValueStateDescriptor[JBool]("is_manager",
      TypeInformation.of(classOf[JBool]))
    stateDesc1.initializeSerializerUnlessSet(operator.getExecutionConfig)
    val stateDesc2 = new ValueStateDescriptor[String]("name", TypeInformation.of(classOf[String]))
    stateDesc2.initializeSerializerUnlessSet(operator.getExecutionConfig)
    val key1 = Row.of("1")
    val key2 = Row.of("2")

    testHarness.processElement(JTuple2.of(true, Row.of("1", JBool.valueOf(true), "jeff")), 2)
    testHarness.processElement(JTuple2.of(true, Row.of("2", JBool.valueOf(false), "dean")), 6)

    val stateOf = (key: Row, sd: ValueStateDescriptor[_]) => {
      testHarness.getState(key, sd).value().asInstanceOf[AnyRef]
    }

    var expectedData = Array(
      Row.of(JBool.valueOf(true), "jeff"),
      Row.of(JBool.valueOf(false), "dean"))
    var storedData = Array(
      Row.of(stateOf(key1, stateDesc1), stateOf(key1, stateDesc2)),
      Row.of(stateOf(key2, stateDesc1), stateOf(key2, stateDesc2)))

    verify(expectedData, storedData)



    testHarness.processElement(JTuple2.of(false, Row.of("1", JBool.valueOf(true), "jeff")), 2)
    testHarness.processElement(JTuple2.of(false, Row.of("2", JBool.valueOf(false), "dean")), 6)

    expectedData = Array(Row.of(null, null), Row.of(null, null))
    storedData = Array(
      Row.of(stateOf(key1, stateDesc1), stateOf(key1, stateDesc2)),
      Row.of(stateOf(key2, stateDesc1), stateOf(key2, stateDesc2)))

    verify(expectedData, storedData)
  }
}
