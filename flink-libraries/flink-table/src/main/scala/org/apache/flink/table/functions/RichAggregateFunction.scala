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

package org.apache.flink.table.functions

import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer

import scala.collection.mutable

/**
  * Rich variant of the [[AggregateFunction]]. It encapsulates access to the state.
  *
  */
abstract class RichAggregateFunction[T, ACC] extends AggregateFunction[T, ACC] {
  private var aggContext: AggregateContext = _
  private val descriptorMapping = mutable.Map[String, StateDescriptor[_, _]]()

  private[flink] def setAggregateContext(context : AggregateContext) = {
    this.aggContext = context
  }

  def registerValueState[K](name: String, typeClass: Class[K]): Unit = {
    descriptorMapping.put(name, new ValueStateDescriptor[K](name, typeClass))
  }

  def registerValueState[K](name: String, typeInfo: TypeInformation[K]): Unit = {
    descriptorMapping.put(name, new ValueStateDescriptor[K](name, typeInfo))
  }

  def registerValueState[K](name: String, typeSerializer: TypeSerializer[K]): Unit = {
    descriptorMapping.put(name, new ValueStateDescriptor[K](name, typeSerializer))
  }

  def registerListState[K](name: String, elementTypeClass: Class[K]): Unit = {
    descriptorMapping.put(name, new ListStateDescriptor[K](name, elementTypeClass))
  }

  def registerListState[K](name: String, elementTypeInfo: TypeInformation[K]): Unit = {
    descriptorMapping.put(name, new ListStateDescriptor[K](name, elementTypeInfo))
  }

  def registerListState[K](name: String, typeSerializer: TypeSerializer[K]): Unit = {
    descriptorMapping.put(name, new ListStateDescriptor[K](name, typeSerializer))
  }

  def registerMapState[UK, UV](name: String, keySerializer: TypeSerializer[UK],
  valueSerializer: TypeSerializer[UV]): Unit = {
    descriptorMapping.put(name, new MapStateDescriptor[UK, UV](name, keySerializer,
      valueSerializer))
  }

  def registerMapState[UK, UV](name: String, keyTypeInfo: TypeInformation[UK],
  valueTypeInfo: TypeInformation[UV]): Unit = {
    descriptorMapping.put(name, new MapStateDescriptor[UK, UV](name, keyTypeInfo, valueTypeInfo))
  }

  def registerMapState[UK, UV](name: String, keyClass: Class[UK], valueClass: Class[UV]): Unit = {
    descriptorMapping.put(name, new MapStateDescriptor[UK, UV](name, keyClass, valueClass))
  }

  def getValueByStateName[K](name: String): ValueState[K] = {
    aggContext.getState(descriptorMapping(name).asInstanceOf[ValueStateDescriptor[K]])
  }

  def getListByStateName[K](name: String): ListState[K] = {
    aggContext.getListState(descriptorMapping(name).asInstanceOf[ListStateDescriptor[K]])
  }

  def getMapByStateName[UK, UV](name: String): MapState[UK, UV] = {
    aggContext.getMapState(descriptorMapping(name).asInstanceOf[MapStateDescriptor[UK, UV]])
  }
}
