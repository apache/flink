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
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.typeutils.TypeStringUtils
import org.apache.flink.util.Preconditions

/**
  * Descriptor for a literal value. A literal value consists of a type and the actual value.
  * Expression values are not allowed.
  *
  * If no type is set, the type is automatically derived from the value. Currently,
  * this is supported for: BOOLEAN, INT, DOUBLE, and VARCHAR.
  *
  * Examples:
  *   - "true", "false" -> BOOLEAN
  *   - "42", "-5" -> INT
  *   - "2.0", "1234.222" -> DOUBLE
  *   - VARCHAR otherwise
  */
class LiteralValue extends HierarchyDescriptor {

  var typeInfo: Option[String] = None
  var value: Option[Any] = None

  /**
    * Type information of the literal value. E.g. Types.BOOLEAN.
    *
    * @param typeInfo type information describing the value
    */
  def of(typeInfo: TypeInformation[_]): LiteralValue = {
    Preconditions.checkNotNull("Type information must not be null.")
    this.typeInfo = Option(TypeStringUtils.writeTypeInfo(typeInfo))
    this
  }

  /**
    * Type string of the literal value. E.g. "BOOLEAN".
    *
    * @param typeString type string describing the value
    */
  def of(typeString: String): LiteralValue = {
    this.typeInfo = Option(typeString)
    this
  }

  /**
    * Literal BOOLEAN value.
    *
    * @param value literal BOOLEAN value
    */
  def value(value: Boolean): LiteralValue = {
    this.value = Option(value)
    this
  }

  /**
    * Literal INT value.
    *
    * @param value literal INT value
    */
  def value(value: Int): LiteralValue = {
    this.value = Option(value)
    this
  }

  /**
    * Literal DOUBLE value.
    *
    * @param value literal DOUBLE value
    */
  def value(value: Double): LiteralValue = {
    this.value = Option(value)
    this
  }

  /**
    * Literal FLOAT value.
    *
    * @param value literal FLOAT value
    */
  def value(value: Float): LiteralValue = {
    this.value = Option(value)
    this
  }

  /**
    * Literal value either for an explicit VARCHAR type or automatically derived type.
    *
    * If no type is set, the type is automatically derived from the value. Currently,
    * this is supported for: BOOLEAN, INT, DOUBLE, and VARCHAR.
    *
    * @param value literal value
    */
  def value(value: String): LiteralValue = {
    this.value = Option(value)
    this
  }

  /**
    * Literal BIGINT value.
    *
    * @param value literal BIGINT value
    */
  def value(value: Long): LiteralValue = {
    this.value = Option(value)
    this
  }

  /**
    * Literal TINYINT value.
    *
    * @param value literal TINYINT value
    */
  def value(value: Byte): LiteralValue = {
    this.value = Option(value)
    this
  }

  /**
    * Literal SMALLINT value.
    *
    * @param value literal SMALLINT value
    */
  def value(value: Short): LiteralValue = {
    this.value = Option(value)
    this
  }

  /**
    * Literal DECIMAL value.
    *
    * @param value literal DECIMAL value
    */
  def value(value: java.math.BigDecimal): LiteralValue = {
    this.value = Option(value)
    this
  }

  /**
    * Internal method for properties conversion.
    */
  override private[flink] def addProperties(properties: DescriptorProperties): Unit = {
    addPropertiesWithPrefix(HierarchyDescriptorValidator.EMPTY_PREFIX, properties)
  }

  /**
    * Internal method for properties conversion.
    */
  override private[flink] def addPropertiesWithPrefix(
      keyPrefix: String,
      properties: DescriptorProperties)
    : Unit = {

    typeInfo match {
      // explicit type
      case Some(ti) =>
        properties.putString(keyPrefix + "type", ti)
        value.foreach(v => properties.putString(keyPrefix + "value", String.valueOf(v)))
      // implicit type
      case None =>
        // do not allow values in top-level
        if (keyPrefix == HierarchyDescriptorValidator.EMPTY_PREFIX) {
          throw new ValidationException(
            "Literal values with implicit type must not exist in the top level of a hierarchy.")
        }
        value.foreach { v =>
          properties.putString(keyPrefix.substring(0, keyPrefix.length - 1), String.valueOf(v))
        }
    }
  }
}

/**
  * Descriptor for a literal value. A literal value consists of a type and the actual value.
  * Expression values are not allowed.
  *
  * If no type is set, the type is automatically derived from the value. Currently,
  * this is supported for: BOOLEAN, INT, DOUBLE, and VARCHAR.
  *
  * Examples:
  *   - "true", "false" -> BOOLEAN
  *   - "42", "-5" -> INT
  *   - "2.0", "1234.222" -> DOUBLE
  *   - VARCHAR otherwise
  */
object LiteralValue {

  /**
    * Descriptor for a literal value. A literal value consists of a type and the actual value.
    * Expression values are not allowed.
    *
    * If no type is set, the type is automatically derived from the value. Currently,
    * this is supported for: BOOLEAN, INT, DOUBLE, and VARCHAR.
    *
    * Examples:
    *   - "true", "false" -> BOOLEAN
    *   - "42", "-5" -> INT
    *   - "2.0", "1234.222" -> DOUBLE
    *   - VARCHAR otherwise
    */
  def apply() = new LiteralValue()
}
