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

package org.apache.flink.table.descriptors

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.typeutils.TypeStringUtils

/**
  * Descriptor for a primitive type. Use internally only.
  */
class PrimitiveType[T] extends HierarchyDescriptor {

  var typeInformation: TypeInformation[T] = _
  var value: T = _

  def of(basicType: TypeInformation[T]): PrimitiveType[T] = {
    typeInformation = basicType
    this
  }

  def value(value: T): PrimitiveType[T] = {
    this.value = value
    this
  }

  override private[flink] def addProperties(properties: DescriptorProperties): Unit = {
    addPropertiesWithPrefix("", properties)
  }

  override private[flink] def addPropertiesWithPrefix(
      keyPrefix: String, properties: DescriptorProperties): Unit = {
    properties.putString(keyPrefix + "type", TypeStringUtils.writeTypeInfo(typeInformation))
    properties.putString(keyPrefix + "value", value.toString)
  }
}

object PrimitiveType {
  def apply[T]() = new PrimitiveType[T]()
}
