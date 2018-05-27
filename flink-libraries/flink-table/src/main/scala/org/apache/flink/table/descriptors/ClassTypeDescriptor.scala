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

import scala.collection.mutable.ArrayBuffer

/**
  * Descriptor for a class type.
  */
class ClassTypeDescriptor extends HierarchyDescriptor {

  private var className: String = _

  private val constructor: ArrayBuffer[Either[PrimitiveTypeDescriptor[_], ClassTypeDescriptor]] =
    ArrayBuffer()

  def setClassName(name: String): ClassTypeDescriptor = {
    this.className = name
    this
  }

  def addConstructorField(field: PrimitiveTypeDescriptor[_]): ClassTypeDescriptor = {
    constructor += Left(field)
    this
  }

  def addConstructorField(field: ClassTypeDescriptor): ClassTypeDescriptor = {
    constructor += Right(field)
    this
  }

  override private[flink] def addProperties(properties: DescriptorProperties): Unit =  {
    addPropertiesWithPrefix("", properties)
  }

  override private[flink] def addPropertiesWithPrefix(
      keyPrefix: String,
      properties: DescriptorProperties): Unit = {
    properties.putString(s"${keyPrefix}class", className)
    var i = 0
    while (i < constructor.size) {
      constructor(i) match {
        case Left(basicType) =>
          basicType.addPropertiesWithPrefix(s"${keyPrefix}constructor.$i.", properties)
        case Right(classType) =>
          classType.addPropertiesWithPrefix(s"${keyPrefix}constructor.$i.", properties)
      }
      i += 1
    }
  }
}

object ClassTypeDescriptor {
  def apply() = new ClassTypeDescriptor
}
