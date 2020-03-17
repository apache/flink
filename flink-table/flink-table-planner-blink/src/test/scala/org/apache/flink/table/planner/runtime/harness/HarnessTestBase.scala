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
package org.apache.flink.table.planner.runtime.harness

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.dag.Transformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.{CheckpointingOptions, Configuration}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.apache.flink.table.api.{StreamQueryConfig, TableConfig}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.planner.JLong
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.{HEAP_BACKEND, ROCKSDB_BACKEND, StateBackendMode}

import org.junit.runners.Parameterized

import java.util

import scala.collection.JavaConversions._

class HarnessTestBase(mode: StateBackendMode) extends StreamingTestBase {

  private val classLoader = Thread.currentThread.getContextClassLoader

  protected def getStateBackend: StateBackend = {
    mode match {
      case HEAP_BACKEND =>
        val conf = new Configuration()
        conf.setBoolean(CheckpointingOptions.ASYNC_SNAPSHOTS, true)
        new MemoryStateBackend().configure(conf, classLoader)

      case ROCKSDB_BACKEND =>
        new RocksDBStateBackend("file://" + tempFolder.newFolder().getAbsoluteFile)
    }
  }

  def createHarnessTester[IN, OUT, KEY](
      operator: OneInputStreamOperator[IN, OUT],
      keySelector: KeySelector[IN, KEY],
      keyType: TypeInformation[KEY]): KeyedOneInputStreamOperatorTestHarness[KEY, IN, OUT] = {
    val harness = new KeyedOneInputStreamOperatorTestHarness[KEY, IN, OUT](
      operator,
      keySelector,
      keyType)
    harness.setStateBackend(getStateBackend)
    harness
  }

  def createHarnessTester(
      ds: DataStream[_],
      prefixOperatorName: String)
  : KeyedOneInputStreamOperatorTestHarness[BaseRow, BaseRow, BaseRow] = {

    val transformation = extractExpectedTransformation(
      ds.javaStream.getTransformation,
      prefixOperatorName)
    val processOperator = transformation.getOperator
      .asInstanceOf[OneInputStreamOperator[Any, Any]]
    val keySelector = transformation.getStateKeySelector.asInstanceOf[KeySelector[Any, Any]]
    val keyType = transformation.getStateKeyType.asInstanceOf[TypeInformation[Any]]

    createHarnessTester(processOperator, keySelector, keyType)
      .asInstanceOf[KeyedOneInputStreamOperatorTestHarness[BaseRow, BaseRow, BaseRow]]
  }

  private def extractExpectedTransformation(
      t: Transformation[_],
      prefixOperatorName: String): OneInputTransformation[_, _] = {
    t match {
      case one: OneInputTransformation[_, _] =>
        if (one.getName.startsWith(prefixOperatorName)) {
          one
        } else {
          extractExpectedTransformation(one.getInput, prefixOperatorName)
        }
      case _ => throw new Exception(
        s"Can not find the expected $prefixOperatorName transformation")
    }
  }

  def dropWatermarks(elements: Array[AnyRef]): util.Collection[AnyRef] = {
    elements.filter(e => !e.isInstanceOf[Watermark]).toList
  }

  class TestTableConfig extends TableConfig {

    private var minIdleStateRetentionTime = 0L

    private var maxIdleStateRetentionTime = 0L

    override def getMinIdleStateRetentionTime: Long = minIdleStateRetentionTime

    override def getMaxIdleStateRetentionTime: Long = maxIdleStateRetentionTime

    override def setIdleStateRetentionTime(minTime: Time, maxTime: Time): Unit = {
      minIdleStateRetentionTime = minTime.toMilliseconds
      maxIdleStateRetentionTime = maxTime.toMilliseconds
    }
  }

  /**
   * Test class used to test min and max retention time.
   */
  class TestStreamQueryConfig(min: Time, max: Time) extends StreamQueryConfig {
    override def getMinIdleStateRetentionTime: Long = min.toMilliseconds
    override def getMaxIdleStateRetentionTime: Long = max.toMilliseconds
  }
}

object HarnessTestBase {

  @Parameterized.Parameters(name = "StateBackend={0}")
  def parameters(): util.Collection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](Array(HEAP_BACKEND), Array(ROCKSDB_BACKEND))
  }

  class TestingBaseRowKeySelector(
    private val selectorField: Int) extends KeySelector[BaseRow, JLong] {

    override def getKey(value: BaseRow): JLong = {
      value.getLong(selectorField)
    }
  }
}
