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

import scala.collection.mutable.ArrayBuffer

/**
  * Descriptor for a class type.
  *
  * @param className the full name of the class (e.g., java.lang.Integer)
  */
class ClassType(var className: Option[String] = None)
  extends HierarchyDescriptor {

  // the parameter is either a primitive type or a class type
  private val constructor: ArrayBuffer[Either[PrimitiveType[_], ClassType]] =
    ArrayBuffer()

  /**
    * Sets the class name for the descriptor.
    */
  def of(name: String): ClassType = {
    this.className = Option(name)
    this
  }

  /**
    * Adds the given string formatted value as a parameter, the type of which will be automatically
    * derived (e.g., "true" -> Boolean, "1" -> Integer, "2.0" -> Double and "abc" -> String).
    *
    */
  def strParam(valueStr: String): ClassType = {
    val typeString = PrimitiveTypeValidator.deriveTypeStrFromValueStr(valueStr)
    param(typeString, valueStr)
  }

  /**
    * Adds the given string formatted value as a parameter, the type of which will be decided by the
    * given type string (e.g., "DOUBLE", "VARCHAR").
    */
  def param(typeStr: String, valueStr: String): ClassType = {
    param(TypeStringUtils.readTypeInfo(typeStr), valueStr)
  }

  /**
    * Adds the give string formatted value as a parameter, the type of which will be defined by the
    * given type information.
    */
  def param[T](typeInfo: TypeInformation[T], valueStr: String): ClassType = {
    constructor += Left(
      new PrimitiveType[T]()
        .of(typeInfo)
        .value(PrimitiveTypeValidator.deriveTypeAndValueStr(typeInfo, valueStr)))
    this
  }

  /**
    * Adds the give value as a parameter, the type of which will be automatically derived.
    */
  def param[T](value: T): ClassType = {
    constructor += Left(
      new PrimitiveType[T]()
        .of(TypeInformation.of(value.getClass.asInstanceOf[Class[T]]))
        .value(value))
    this
  }

  /**
    * Adds a parameter defined by the given class type descriptor.
    */
  def param(field: ClassType): ClassType = {
    constructor += Right(field)
    this
  }

  override private[flink] def addProperties(properties: DescriptorProperties): Unit =  {
    addPropertiesWithPrefix("", properties)
  }

  override private[flink] def addPropertiesWithPrefix(
      keyPrefix: String,
      properties: DescriptorProperties): Unit = {
    className.foreach(properties.putString(s"$keyPrefix${ClassTypeValidator.CLASS}", _))
    var i = 0
    while (i < constructor.size) {
      constructor(i) match {
        case Left(basicType) =>
          basicType.addPropertiesWithPrefix(
            s"$keyPrefix${ClassTypeValidator.CONSTRUCTOR}.$i.",
            properties)
        case Right(classType) =>
          classType.addPropertiesWithPrefix(
            s"$keyPrefix${ClassTypeValidator.CONSTRUCTOR}.$i.",
            properties)
      }
      i += 1
    }
  }
}

object ClassType {
  def apply() = new ClassType
  def apply(className: String) = new ClassType(Option(className))
}
