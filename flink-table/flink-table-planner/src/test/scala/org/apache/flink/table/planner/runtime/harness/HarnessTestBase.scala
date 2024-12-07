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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.dag.Transformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.CheckpointStorage
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, PartitionTransformation}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.util.{KeyedOneInputStreamOperatorTestHarness, OneInputStreamOperatorTestHarness}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.JLong
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.{HEAP_BACKEND, ROCKSDB_BACKEND, StateBackendMode}
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters
import org.apache.flink.testutils.junit.utils.TempDirUtils

import java.util

import scala.collection.JavaConversions._

class HarnessTestBase(mode: StateBackendMode) extends StreamingTestBase {

  private val classLoader = Thread.currentThread.getContextClassLoader

  protected def getStateBackend: StateBackend = {
    mode match {
      case HEAP_BACKEND =>
        val conf = new Configuration()
        new HashMapStateBackend().configure(conf, classLoader)

      case ROCKSDB_BACKEND =>
        new EmbeddedRocksDBStateBackend()
    }
  }

  protected def getCheckpointStorage: CheckpointStorage = {
    mode match {
      case HEAP_BACKEND =>
        new JobManagerCheckpointStorage()

      case ROCKSDB_BACKEND =>
        new FileSystemCheckpointStorage(
          "file://" + TempDirUtils.newFolder(tempFolder).getAbsoluteFile)
    }
  }

  def createHarnessTester[IN, OUT, KEY](
      operator: OneInputStreamOperator[IN, OUT],
      keySelector: KeySelector[IN, KEY],
      keyType: TypeInformation[KEY]): KeyedOneInputStreamOperatorTestHarness[KEY, IN, OUT] = {
    val harness =
      new KeyedOneInputStreamOperatorTestHarness[KEY, IN, OUT](operator, keySelector, keyType)
    harness.setStateBackend(getStateBackend)
    harness.setCheckpointStorage(getCheckpointStorage)
    harness
  }

  def createHarnessTester(ds: DataStream[_], operatorNameIdentifier: String)
      : KeyedOneInputStreamOperatorTestHarness[RowData, RowData, RowData] = {

    val transformation =
      extractExpectedTransformation(ds.getTransformation, operatorNameIdentifier)
    val processOperator = transformation.getOperator
      .asInstanceOf[OneInputStreamOperator[Any, Any]]
    val keySelector = transformation.getStateKeySelector.asInstanceOf[KeySelector[Any, Any]]
    val keyType = transformation.getStateKeyType.asInstanceOf[TypeInformation[Any]]

    createHarnessTester(processOperator, keySelector, keyType)
      .asInstanceOf[KeyedOneInputStreamOperatorTestHarness[RowData, RowData, RowData]]
  }

  def createHarnessTesterForNoState(
      ds: DataStream[_],
      operatorNameIdentifier: String): OneInputStreamOperatorTestHarness[RowData, RowData] = {
    val transformation =
      extractExpectedTransformation(ds.getTransformation, operatorNameIdentifier)
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
        if (
          one.getName.contains(prefixOperatorName)
          || one.getDescription.contains(prefixOperatorName)
        ) {
          one
        } else {
          extractExpectedTransformation(one.getInputs.get(0), prefixOperatorName)
        }
      case p: PartitionTransformation[_] =>
        extractExpectedTransformation(p.getInputs.get(0), prefixOperatorName)
      case _ => throw new Exception(s"Can not find the expected $prefixOperatorName transformation")
    }
  }

  def dropWatermarks(elements: Array[AnyRef]): util.Collection[AnyRef] = {
    elements.filter(e => !e.isInstanceOf[Watermark]).toList
  }
}

object HarnessTestBase {

  @Parameters(name = "StateBackend={0}")
  def parameters(): util.Collection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](Array(HEAP_BACKEND), Array(ROCKSDB_BACKEND))
  }

  class TestingRowDataKeySelector(private val selectorField: Int)
    extends KeySelector[RowData, JLong] {

    override def getKey(value: RowData): JLong = {
      value.getLong(selectorField)
    }
  }
}
