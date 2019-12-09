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

package org.apache.flink.table.runtime.types

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.operators.{AbstractStreamOperator, KeyedProcessOperator}
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, InstantiationUtil, TestLogger}

import org.junit.{Assert, Test}

class CRowSerializerTest extends TestLogger {

  /**
    * This empty constructor is required for deserializing the configuration.
    */
  @Test
  def testDefaultConstructor(): Unit = {
    new CRowSerializer.CRowSerializerConfigSnapshot()

    InstantiationUtil.instantiate(classOf[CRowSerializer.CRowSerializerConfigSnapshot])
  }

  @Test
  def testStateRestore(): Unit = {

    class IKeyedProcessFunction extends KeyedProcessFunction[Integer, Integer, Integer] {
      var state: ListState[CRow] = _
      override def open(parameters: Configuration): Unit = {
        val stateDesc = new ListStateDescriptor[CRow]("CRow",
          new CRowTypeInfo(new RowTypeInfo(Types.INT)))
        state = getRuntimeContext.getListState(stateDesc)
      }
      override def processElement(value: Integer,
          ctx: KeyedProcessFunction[Integer, Integer, Integer]#Context,
          out: Collector[Integer]): Unit = {
        state.add(new CRow(Row.of(value), true))
      }
    }

    val operator = new KeyedProcessOperator[Integer, Integer, Integer](new IKeyedProcessFunction)

    var testHarness = new KeyedOneInputStreamOperatorTestHarness[Integer, Integer, Integer](
      operator,
      new KeySelector[Integer, Integer] {
        override def getKey(value: Integer): Integer= -1
      },
      Types.INT, 1, 1, 0)
    testHarness.setup()
    testHarness.open()

    testHarness.processElement(new StreamRecord[Integer](1, 1L))
    testHarness.processElement(new StreamRecord[Integer](2, 1L))
    testHarness.processElement(new StreamRecord[Integer](3, 1L))

    Assert.assertEquals(1, numKeyedStateEntries(operator))

    val snapshot = testHarness.snapshot(0L, 0L)
    testHarness.close()

    testHarness = new KeyedOneInputStreamOperatorTestHarness[Integer, Integer, Integer](
      operator,
      new KeySelector[Integer, Integer] {
        override def getKey(value: Integer): Integer= -1
      },
      Types.INT, 1, 1, 0)
    testHarness.setup()

    testHarness.initializeState(snapshot)

    testHarness.open()

    Assert.assertEquals(1, numKeyedStateEntries(operator))

    testHarness.close()
  }

  def numKeyedStateEntries(operator: AbstractStreamOperator[_]): Int = {
    val keyedStateBackend = operator.getKeyedStateBackend
    keyedStateBackend match {
      case hksb: HeapKeyedStateBackend[_] => hksb.numKeyValueStateEntries
      case _ => throw new UnsupportedOperationException
    }
  }

}
