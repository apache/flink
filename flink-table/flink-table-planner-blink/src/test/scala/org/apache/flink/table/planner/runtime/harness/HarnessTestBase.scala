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
import org.apache.flink.table.api.TableConfig
import org.apache.flink.configuration.{CheckpointingOptions, Configuration}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, PartitionTransformation}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.util.{KeyedOneInputStreamOperatorTestHarness, OneInputStreamOperatorTestHarness}
import org.apache.flink.table.data.RowData
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
  : KeyedOneInputStreamOperatorTestHarness[RowData, RowData, RowData] = {

    val transformation = extractExpectedTransformation(
      ds.javaStream.getTransformation,
      prefixOperatorName)
    val processOperator = transformation.getOperator
      .asInstanceOf[OneInputStreamOperator[Any, Any]]
    val keySelector = transformation.getStateKeySelector.asInstanceOf[KeySelector[Any, Any]]
    val keyType = transformation.getStateKeyType.asInstanceOf[TypeInformation[Any]]

    createHarnessTester(processOperator, keySelector, keyType)
      .asInstanceOf[KeyedOneInputStreamOperatorTestHarness[RowData, RowData, RowData]]
  }

  def createHarnessTesterForNoState(
      ds: DataStream[_],
      prefixOperatorName: String)
  : OneInputStreamOperatorTestHarness[RowData, RowData] = {
    val transformation = extractExpectedTransformation(
      ds.javaStream.getTransformation,
      prefixOperatorName)
    val processOperator = transformation.getOperator
        .asInstanceOf[OneInputStreamOperator[Any, Any]]
    new OneInputStreamOperatorTestHarness(processOperator)
        .asInstanceOf[OneInputStreamOperatorTestHarness[RowData, RowData]]
  }

  private def extractExpectedTransformation(
      t: Transformation[_],
      prefixOperatorName: String): OneInputTransformation[_, _] = {
    t match {
      case one: OneInputTransformation[_, _] =>
        if (one.getName.startsWith(prefixOperatorName)) {
          one
        } else {
          extractExpectedTransformation(one.getInputs.get(0), prefixOperatorName)
        }
      case p: PartitionTransformation[_] =>
        extractExpectedTransformation(p.getInputs.get(0), prefixOperatorName)
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
}

object HarnessTestBase {

  @Parameterized.Parameters(name = "StateBackend={0}")
  def parameters(): util.Collection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](Array(HEAP_BACKEND), Array(ROCKSDB_BACKEND))
  }

  class TestingRowDataKeySelector(
    private val selectorField: Int) extends KeySelector[RowData, JLong] {

    override def getKey(value: RowData): JLong = {
      value.getLong(selectorField)
    }
  }
}
