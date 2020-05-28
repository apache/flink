package org.apache.flink.state.api.scala

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.state.api.{WritableSavepoint, ExistingSavepoint => JExistingSavepoint}

class ExistingSavepoint(existingSavepoint: JExistingSavepoint)
  extends WritableSavepoint(existingSavepoint.metadata, existingSavepoint.stateBackend) {

  def readListState[T: TypeInformation](uid: String, name: String): DataSet[T] = {
    val dataSet = existingSavepoint.readListState(uid, name, implicitly[TypeInformation[T]])
    asScalaDataSet(dataSet)
  }

  def readListState[T: TypeInformation](uid: String, name: String, serializer: TypeSerializer[T]): DataSet[T] = {
    val dataSet = existingSavepoint.readListState(uid, name, implicitly[TypeInformation[T]], serializer)
    asScalaDataSet(dataSet)
  }

  def readUnionState[T: TypeInformation](uid: String, name: String): DataSet[T] = {
    val dataSet = existingSavepoint.readUnionState(uid, name, implicitly[TypeInformation[T]])
    asScalaDataSet(dataSet)
  }

  def readUnionState[T: TypeInformation](uid: String, name: String, serializer: TypeSerializer[T]): DataSet[T] = {
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

  def readKeyedState[K: TypeInformation, OUT: TypeInformation](uid: String, function: KeyedStateReaderFunction[K, OUT]): DataSet[OUT] = {
    val dataSet = existingSavepoint.readKeyedState(uid, function, implicitly[TypeInformation[K]], implicitly[TypeInformation[OUT]])
    asScalaDataSet(dataSet)
  }
}
