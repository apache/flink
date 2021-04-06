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

package org.apache.flink.table.planner.runtime.utils

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.configuration.{CheckpointingOptions, Configuration}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.source.FromElementsFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.data.binary.BinaryRowData
import org.apache.flink.table.data.writer.BinaryRowWriter
import org.apache.flink.table.data.{RowData, StringData}
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.{HEAP_BACKEND, ROCKSDB_BACKEND, StateBackendMode}
import org.apache.flink.table.planner.utils.TableTestUtil
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType

import org.junit.runners.Parameterized
import org.junit.{After, Assert, Before}

import java.io.File
import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class StreamingWithStateTestBase(state: StateBackendMode) extends StreamingTestBase {

  enableObjectReuse = state match {
    case HEAP_BACKEND => false // TODO heap statebackend not support obj reuse now.
    case ROCKSDB_BACKEND => true
  }

  private val classLoader = Thread.currentThread.getContextClassLoader

  var baseCheckpointPath: File = _

  @Before
  override def before(): Unit = {
    super.before()
    // set state backend
    baseCheckpointPath = tempFolder.newFolder().getAbsoluteFile
    state match {
      case HEAP_BACKEND =>
        val conf = new Configuration()
        env.setStateBackend(new MemoryStateBackend(
          "file://" + baseCheckpointPath, null).configure(conf, classLoader))
      case ROCKSDB_BACKEND =>
        val conf = new Configuration()
        conf.setBoolean(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true)
        env.setStateBackend(new RocksDBStateBackend(
          "file://" + baseCheckpointPath).configure(conf, classLoader))
    }
    this.tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)
    FailingCollectionSource.failedBefore = true
  }

  @After
  override def after(): Unit = {
    super.after()
    Assert.assertTrue(FailingCollectionSource.failedBefore)
  }

  /**
    * Creates a BinaryRowData DataStream from the given non-empty [[Seq]].
    */
  def failingBinaryRowSource[T: TypeInformation](data: Seq[T]): DataStream[RowData] = {
    val typeInfo = implicitly[TypeInformation[_]].asInstanceOf[CompositeType[_]]
    val result = new mutable.MutableList[RowData]
    val reuse = new BinaryRowData(typeInfo.getArity)
    val writer = new BinaryRowWriter(reuse)
    data.foreach {
      case p: Product =>
        for (i <- 0 until typeInfo.getArity) {
          val fieldType = typeInfo.getTypeAt(i).asInstanceOf[TypeInformation[_]]
          fieldType match {
            case Types.INT => writer.writeInt(i, p.productElement(i).asInstanceOf[Int])
            case Types.LONG => writer.writeLong(i, p.productElement(i).asInstanceOf[Long])
            case Types.STRING => writer.writeString(i,
              StringData.fromString(p.productElement(i).asInstanceOf[String]))
            case Types.BOOLEAN => writer.writeBoolean(i, p.productElement(i).asInstanceOf[Boolean])
          }
        }
        writer.complete()
        result += reuse.copy()
      case _ => throw new UnsupportedOperationException
    }
    val newTypeInfo = InternalTypeInfo.of(
      TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType(typeInfo).asInstanceOf[RowType])
    failingDataSource(result)(newTypeInfo.asInstanceOf[TypeInformation[RowData]])
  }

  /**
    * Creates a DataStream from the given non-empty [[Seq]].
    */
  def failingDataSource[T: TypeInformation](data: Seq[T]): DataStream[T] = {
    env.enableCheckpointing(100, CheckpointingMode.EXACTLY_ONCE)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0))
    // reset failedBefore flag to false
    FailingCollectionSource.reset()

    require(data != null, "Data must not be null.")
    val typeInfo = implicitly[TypeInformation[T]]

    val collection = scala.collection.JavaConversions.asJavaCollection(data)
    // must not have null elements and mixed elements
    FromElementsFunction.checkCollection(data, typeInfo.getTypeClass)

    val function = new FailingCollectionSource[T](
      typeInfo.createSerializer(env.getConfig),
      collection,
      data.length / 2) // fail after half elements

    env.addSource(function)(typeInfo).setMaxParallelism(1)
  }

  private def mapStrEquals(str1: String, str2: String): Boolean = {
    val array1 = str1.toCharArray
    val array2 = str2.toCharArray
    if (array1.length != array2.length) {
      return false
    }
    val l = array1.length
    val leftBrace = "{".charAt(0)
    val rightBrace = "}".charAt(0)
    val equalsChar = "=".charAt(0)
    val lParenthesis = "(".charAt(0)
    val rParenthesis = ")".charAt(0)
    val dot = ",".charAt(0)
    val whiteSpace = " ".charAt(0)
    val map1 = Map[String, String]()
    val map2 = Map[String, String]()
    var idx = 0
    def findEquals(ss: CharSequence): Array[Int] = {
      val ret = new ArrayBuffer[Int]()
      (0 until ss.length) foreach (idx => if (ss.charAt(idx) == equalsChar) ret += idx)
      ret.toArray
    }

    def splitKV(ss: CharSequence, equalsIdx: Int): (String, String) = {
      // find right, if starts with '(' find until the ')', else until the ','
      var endFlag = false
      var curIdx = equalsIdx + 1
      var endChar = if (ss.charAt(curIdx) == lParenthesis) rParenthesis else dot
      var valueStr: CharSequence = null
      var keyStr: CharSequence = null
      while (curIdx < ss.length && !endFlag) {
        val curChar = ss.charAt(curIdx)
        if (curChar != endChar && curChar != rightBrace) {
          curIdx += 1
          if (curIdx == ss.length) {
            valueStr = ss.subSequence(equalsIdx + 1, curIdx)
          }
        } else {
          valueStr = ss.subSequence(equalsIdx + 1, curIdx)
          endFlag = true
        }
      }

      // find left, if starts with ')' find until the '(', else until the ' ,'
      endFlag = false
      curIdx = equalsIdx - 1
      endChar = if (ss.charAt(curIdx) == rParenthesis) lParenthesis else whiteSpace
      while (curIdx >= 0 && !endFlag) {
        val curChar = ss.charAt(curIdx)
        if (curChar != endChar && curChar != leftBrace) {
          curIdx -= 1
          if (curIdx == -1) {
            keyStr = ss.subSequence(0, equalsIdx)
          }
        } else {
          keyStr = ss.subSequence(curIdx, equalsIdx)
          endFlag = true
        }
      }
      require(keyStr != null)
      require(valueStr != null)
      (keyStr.toString, valueStr.toString)
    }

    def appendStrToMap(ss: CharSequence, m: Map[String, String]): Unit = {
      val equalsIdxs = findEquals(ss)
      equalsIdxs.foreach (idx => m + splitKV(ss, idx))
    }

    while (idx < l) {
      val char1 = array1(idx)
      val char2 = array2(idx)
      if (char1 != char2) {
        return false
      }

      if (char1 == leftBrace) {
        val rightBraceIdx = array1.subSequence(idx + 1, l).toString.indexOf(rightBrace)
        appendStrToMap(array1.subSequence(idx + 1, rightBraceIdx + idx + 2), map1)
        idx += rightBraceIdx
      } else {
        idx += 1
      }
    }
    map1.equals(map2)
  }

  def assertMapStrEquals(str1: String, str2: String): Unit = {
    if (!mapStrEquals(str1, str2)) {
      throw new AssertionError(s"Expected: $str1 \n Actual: $str2")
    }
  }
}

object StreamingWithStateTestBase {

  case class StateBackendMode(backend: String) {
    override def toString: String = backend.toString
  }

  val HEAP_BACKEND = StateBackendMode("HEAP")
  val ROCKSDB_BACKEND = StateBackendMode("ROCKSDB")

  @Parameterized.Parameters(name = "StateBackend={0}")
  def parameters(): util.Collection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](Array(HEAP_BACKEND), Array(ROCKSDB_BACKEND))
  }
}
