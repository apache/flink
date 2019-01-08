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

import java.lang.{Boolean => JBoolean, Double => JDouble, Integer => JInt}

import org.apache.flink.table.api.{TableException, Types, ValidationException}

/**
  * Validator for [[LiteralValue]].
  */
class LiteralValueValidator(keyPrefix: String) extends HierarchyDescriptorValidator(keyPrefix) {

  /*
   * TODO The following types need to be supported next.
   * Types.SQL_DATE
   * Types.SQL_TIME
   * Types.SQL_TIMESTAMP
   * Types.PRIMITIVE_ARRAY
   * Types.OBJECT_ARRAY
   * Types.MAP
   * Types.MULTISET
   * null
   */

  override protected def validateWithPrefix(
      keyPrefix: String,
      properties: DescriptorProperties)
    : Unit = {

    val typeKey = s"$keyPrefix${LiteralValueValidator.TYPE}"

    properties.validateType(typeKey, isOptional = true, requireRow = false)

    // explicit type
    if (properties.containsKey(typeKey)) {
      val valueKey = s"$keyPrefix${LiteralValueValidator.VALUE}"
      val typeInfo = properties.getType(typeKey)
      typeInfo match {
        case Types.DECIMAL => properties.validateBigDecimal(valueKey, isOptional = false)
        case Types.BOOLEAN => properties.validateBoolean(valueKey, isOptional = false)
        case Types.BYTE => properties.validateByte(valueKey, isOptional = false)
        case Types.DOUBLE => properties.validateDouble(valueKey, isOptional = false)
        case Types.FLOAT => properties.validateFloat(valueKey, isOptional = false)
        case Types.INT => properties.validateInt(valueKey, isOptional = false)
        case Types.LONG => properties.validateLong(valueKey, isOptional = false)
        case Types.SHORT => properties.validateShort(valueKey, isOptional = false)
        case Types.STRING => properties.validateString(valueKey, isOptional = false)
        case _ => throw TableException(s"Unsupported type '$typeInfo'.")
      }
    }
    // implicit type
    else {
      // do not allow values in top-level
      if (keyPrefix == HierarchyDescriptorValidator.EMPTY_PREFIX) {
        throw new ValidationException(
          "Literal values with implicit type must not exist in the top level of a hierarchy.")
      }
      properties.validateString(keyPrefix.substring(0, keyPrefix.length - 1), isOptional = false)
    }
  }
}

object LiteralValueValidator {
  val TYPE = "type"
  val VALUE = "value"

  private val LITERAL_FALSE = "false"
  private val LITERAL_TRUE = "true"

  /**
    * Gets the value according to the type and value strings.
    *
    * @param keyPrefix the prefix of the literal type key
    * @param properties the descriptor properties
    * @return the derived value
    */
  def getValue(keyPrefix: String, properties: DescriptorProperties): Any = {
    val typeKey = s"$keyPrefix$TYPE"
    // explicit type
    if (properties.containsKey(typeKey)) {
      val valueKey = s"$keyPrefix$VALUE"
      val typeInfo = properties.getType(typeKey)
      typeInfo match {
        case Types.DECIMAL => properties.getBigDecimal(valueKey)
        case Types.BOOLEAN => properties.getBoolean(valueKey)
        case Types.BYTE => properties.getByte(valueKey)
        case Types.DOUBLE => properties.getDouble(valueKey)
        case Types.FLOAT => properties.getFloat(valueKey)
        case Types.INT => properties.getInt(valueKey)
        case Types.LONG => properties.getLong(valueKey)
        case Types.SHORT => properties.getShort(valueKey)
        case Types.STRING => properties.getString(valueKey)
        case _ => throw TableException(s"Unsupported type '${typeInfo.getTypeClass}'.")
      }
    }
    // implicit type
    else {
      deriveTypeStringFromValueString(
        properties.getString(keyPrefix.substring(0, keyPrefix.length - 1)))
    }
  }

  /**
    * Tries to derive a literal value from the given string value.
    * The derivation priority for the types are BOOLEAN, INT, DOUBLE, and VARCHAR.
    *
    * @param valueString the string formatted value
    * @return parsed value
    */
  def deriveTypeStringFromValueString(valueString: String): AnyRef = {
    if (valueString.equals(LITERAL_TRUE) || valueString.equals(LITERAL_FALSE)) {
      JBoolean.valueOf(valueString)
    } else {
      try {
        JInt.valueOf(valueString)
      } catch {
        case _: NumberFormatException =>
          try {
            JDouble.valueOf(valueString)
          } catch {
            case _: NumberFormatException =>
              valueString
          }
      }
    }
  }
}
