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

import java.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.Types

import scala.collection.mutable.ArrayBuffer

/**
  * Descriptor for a class instance. A class instance is a Java/Scala object created from a class
  * with a public constructor (with or without parameters).
  */
class ClassInstance extends HierarchyDescriptor {

  private var className: Option[String] = None

  // the parameter is either a literal value or the instance of a class
  private val constructor: ArrayBuffer[Either[LiteralValue, ClassInstance]] = ArrayBuffer()

  /**
    * Sets the fully qualified class name for creating an instance.
    *
    * E.g. org.example.MyClass or org.example.MyClass$StaticInnerClass
    *
    * @param className fully qualified class name
    */
  def of(className: String): ClassInstance = {
    this.className = Option(className)
    this
  }

  /**
    * Adds a constructor parameter value of literal type. The type is automatically derived from
    * the value. Currently, this is supported for: BOOLEAN, INT, DOUBLE, and VARCHAR. Expression
    * values are not allowed.
    *
    * Examples:
    *   - "true", "false" -> BOOLEAN
    *   - "42", "-5" -> INT
    *   - "2.0", "1234.222" -> DOUBLE
    *   - VARCHAR otherwise
    *
    * For other types and explicit type declaration use [[parameter(String, String)]] or
    * [[parameter(TypeInformation, String)]].
    *
    */
  def parameterString(valueString: String): ClassInstance = {
    constructor += Left(new LiteralValue().value(valueString))
    this
  }

  /**
    * Adds a constructor parameter value of literal type. The type is explicitly defined using a
    * type string such as VARCHAR, FLOAT, BOOLEAN, INT, BIGINT, etc. The value is parsed
    * accordingly. Expression values are not allowed.
    *
    * @param typeString the type string that define how to parse the given value string
    * @param valueString the literal value to be parsed
    */
  def parameter(typeString: String, valueString: String): ClassInstance = {
    constructor += Left(new LiteralValue().of(typeString).value(valueString))
    this
  }

  /**
    * Adds a constructor parameter value of literal type. The type is explicitly defined using
    * type information. The value is parsed accordingly. Expression values are not allowed.
    *
    * @param typeInfo the type that define how to parse the given value string
    * @param valueString the literal value to be parsed
    */
  def parameter(typeInfo: TypeInformation[_], valueString: String): ClassInstance = {
    constructor += Left(new LiteralValue().of(typeInfo).value(valueString))
    this
  }

  /**
    * Adds a constructor parameter value of BOOLEAN type.
    *
    * @param value BOOLEAN value
    */
  def parameter(value: Boolean): ClassInstance = {
    constructor += Left(new LiteralValue().of(Types.BOOLEAN).value(value))
    this
  }

  /**
    * Adds a constructor parameter value of DOUBLE type.
    *
    * @param value DOUBLE value
    */
  def parameter(value: Double): ClassInstance = {
    constructor += Left(new LiteralValue().of(Types.DOUBLE).value(value))
    this
  }

  /**
    * Adds a constructor parameter value of FLOAT type.
    *
    * @param value FLOAT value
    */
  def parameter(value: Float): ClassInstance = {
    constructor += Left(new LiteralValue().of(Types.FLOAT).value(value))
    this
  }

  /**
    * Adds a constructor parameter value of INT type.
    *
    * @param value INT value
    */
  def parameter(value: Int): ClassInstance = {
    constructor += Left(new LiteralValue().of(Types.INT).value(value))
    this
  }

  /**
    * Adds a constructor parameter value of VARCHAR type.
    *
    * @param value VARCHAR value
    */
  def parameter(value: String): ClassInstance = {
    constructor += Left(new LiteralValue().of(Types.STRING).value(value))
    this
  }

  /**
    * Adds a constructor parameter value of BIGINT type.
    *
    * @param value BIGINT value
    */
  def parameter(value: Long): ClassInstance = {
    constructor += Left(new LiteralValue().of(Types.LONG).value(value))
    this
  }

  /**
    * Adds a constructor parameter value of TINYINT type.
    *
    * @param value TINYINT value
    */
  def parameter(value: Byte): ClassInstance = {
    constructor += Left(new LiteralValue().of(Types.BYTE).value(value))
    this
  }

  /**
    * Adds a constructor parameter value of SMALLINT type.
    *
    * @param value SMALLINT value
    */
  def parameter(value: Short): ClassInstance = {
    constructor += Left(new LiteralValue().of(Types.SHORT).value(value))
    this
  }

  /**
    * Adds a constructor parameter value of DECIMAL type.
    *
    * @param value DECIMAL value
    */
  def parameter(value: java.math.BigDecimal): ClassInstance = {
    constructor += Left(new LiteralValue().of(Types.DECIMAL).value(value))
    this
  }

  /**
    * Adds a constructor parameter value of a class instance (i.e. a Java object with a public
    * constructor).
    *
    * @param classInstance description of a class instance (i.e. a Java object with a public
    *                      constructor).
    */
  def parameter(classInstance: ClassInstance): ClassInstance = {
    constructor += Right(classInstance)
    this
  }

  /**
    * Converts this descriptor into a set of properties.
    */
  override def toProperties: util.Map[String, String] = {
    val properties = new DescriptorProperties()
    addPropertiesWithPrefix(HierarchyDescriptorValidator.EMPTY_PREFIX, properties)
    properties.asMap()
  }

  /**
    * Internal method for properties conversion.
    */
  override private[flink] def addPropertiesWithPrefix(
      keyPrefix: String,
      properties: DescriptorProperties): Unit = {
    className.foreach(properties.putString(s"$keyPrefix${ClassInstanceValidator.CLASS}", _))
    var i = 0
    while (i < constructor.size) {
      constructor(i) match {
        case Left(literalValue) =>
          literalValue.addPropertiesWithPrefix(
            s"$keyPrefix${ClassInstanceValidator.CONSTRUCTOR}.$i.",
            properties)
        case Right(classInstance) =>
          classInstance.addPropertiesWithPrefix(
            s"$keyPrefix${ClassInstanceValidator.CONSTRUCTOR}.$i.",
            properties)
      }
      i += 1
    }
  }
}

/**
  * Descriptor for a class instance. A class instance is a Java/Scala object created from a class
  * with a public constructor (with or without parameters).
  */
object ClassInstance {

  /**
    * Descriptor for a class instance. A class instance is a Java/Scala object created from a class
    * with a public constructor (with or without parameters).
    *
    * @deprecated Use `new ClassInstance()`.
    */
  @deprecated
  def apply() = new ClassInstance
}
