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

package org.apache.flink.state.api.scala

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.state.api.{WritableSavepoint, ExistingSavepoint => JExistingSavepoint}

import scala.reflect.ClassTag

class ExistingSavepoint(existingSavepoint: JExistingSavepoint)
  extends WritableSavepoint(existingSavepoint.metadata, existingSavepoint.stateBackend) {

  def readListState[T: TypeInformation : ClassTag](uid: String, name: String): DataSet[T] = {
    val dataSet = existingSavepoint.readListState(uid, name, implicitly[TypeInformation[T]])
    asScalaDataSet(dataSet)
  }

  def readListState[T: TypeInformation : ClassTag](uid: String, name: String, serializer: TypeSerializer[T]): DataSet[T] = {
    val dataSet = existingSavepoint.readListState(uid, name, implicitly[TypeInformation[T]], serializer)
    asScalaDataSet(dataSet)
  }

  def readUnionState[T: TypeInformation : ClassTag](uid: String, name: String): DataSet[T] = {
    val dataSet = existingSavepoint.readUnionState(uid, name, implicitly[TypeInformation[T]])
    asScalaDataSet(dataSet)
  }

  def readUnionState[T: TypeInformation : ClassTag](uid: String, name: String, serializer: TypeSerializer[T]): DataSet[T] = {
    val dataSet = existingSavepoint.readUnionState(uid, name, implicitly[TypeInformation[T]], serializer)
    asScalaDataSet(dataSet)
  }

  def readBroadcastState[K: TypeInformation, V: TypeInformation](uid: String, name: String): DataSet[JTuple2[K, V]] = {
    val dataSet = existingSavepoint.readBroadcastState(uid, name, implicitly[TypeInformation[K]], implicitly[TypeInformation[V]])
    asScalaDataSet(dataSet)
  }

  def readBroadcastState[K: TypeInformation, V: TypeInformation](uid: String, name: String, keySerializer: TypeSerializer[K],
                                                                 valueSerializer: TypeSerializer[V]): DataSet[JTuple2[K, V]] = {
    val dataSet = existingSavepoint.readBroadcastState(uid, name, implicitly[TypeInformation[K]], implicitly[TypeInformation[V]], keySerializer, valueSerializer)
    asScalaDataSet(dataSet)
  }

  def readKeyedState[K: TypeInformation, OUT: TypeInformation : ClassTag](uid: String, function: KeyedStateReaderFunction[K, OUT]): DataSet[OUT] = {
    val dataSet = existingSavepoint.readKeyedState(uid, function, implicitly[TypeInformation[K]], implicitly[TypeInformation[OUT]])
    asScalaDataSet(dataSet)
  }
}
