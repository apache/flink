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

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong, Short => JShort}
import java.math.{BigDecimal => JBigDecimal}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{TableException, Types}
import org.apache.flink.table.typeutils.TypeStringUtils

/**
  * Validator for [[PrimitiveType]].
  */
class PrimitiveTypeValidator extends HierarchyDescriptorValidator {

  /*
   * TODO The following types need to be supported next.
   * Types.SQL_DATE
   * Types.SQL_TIME
   * Types.SQL_TIMESTAMP
   * Types.INTERVAL_MONTHS
   * Types.INTERVAL_MILLIS
   * Types.PRIMITIVE_ARRAY
   * Types.OBJECT_ARRAY
   * Types.MAP
   * Types.MULTISET
   */

  override def validateWithPrefix(keyPrefix: String, properties: DescriptorProperties): Unit = {
    val typeKey = s"$keyPrefix${PrimitiveTypeValidator.PRIMITIVE_TYPE}"
    val valueKey = s"$keyPrefix${PrimitiveTypeValidator.PRIMITIVE_VALUE}"

    properties.validateType(typeKey, isOptional = false)

    val typeInfo: TypeInformation[_] =
      properties.getType(s"$keyPrefix${PrimitiveTypeValidator.PRIMITIVE_TYPE}")
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
      case _ => throw TableException(s"Unsupported type ${typeInfo.getTypeClass}.")
    }
  }
}

object PrimitiveTypeValidator {
  val PRIMITIVE_TYPE = "type"
  val PRIMITIVE_VALUE = "value"

  private val LITERAL_FALSE = "false"
  private val LITERAL_TRUE = "true"

  /**
    * Derives the value according to the type and value strings.
    *
    * @param keyPrefix the prefix of the primitive type key
    * @param properties the descriptor properties
    * @return the derived value
    */
  def derivePrimitiveValue(keyPrefix: String, properties: DescriptorProperties): Any = {
    val typeInfo =
      properties.getType(s"$keyPrefix$PRIMITIVE_TYPE")
    val valueKey = s"$keyPrefix$PRIMITIVE_VALUE"
    val value = typeInfo match {
      case Types.DECIMAL => properties.getBigDecimal(valueKey)
      case Types.BOOLEAN => properties.getBoolean(valueKey)
      case Types.BYTE => properties.getByte(valueKey)
      case Types.DOUBLE => properties.getDouble(valueKey)
      case Types.FLOAT => properties.getFloat(valueKey)
      case Types.INT => properties.getInt(valueKey)
      case Types.LONG => properties.getLong(valueKey)
      case Types.SHORT => properties.getShort(valueKey)
      case Types.STRING => properties.getString(valueKey)
      case _ => throw TableException(s"Unsupported type ${typeInfo.getTypeClass}.")
    }
    value
  }

  /**
    * Derives the actually value with the type information and string formatted value.
    */
  def deriveTypeAndValueStr[T](typeInfo: TypeInformation[T], valueStr: String): T = {
    typeInfo match {
      case Types.DECIMAL => new JBigDecimal(valueStr).asInstanceOf[T]
      case Types.BOOLEAN => JBoolean.parseBoolean(valueStr).asInstanceOf[T]
      case Types.BYTE => JByte.parseByte(valueStr).asInstanceOf[T]
      case Types.DOUBLE => JDouble.parseDouble(valueStr).asInstanceOf[T]
      case Types.FLOAT => JFloat.parseFloat(valueStr).asInstanceOf[T]
      case Types.INT => JInt.parseInt(valueStr).asInstanceOf[T]
      case Types.LONG => JLong.parseLong(valueStr).asInstanceOf[T]
      case Types.SHORT => JShort.parseShort(valueStr).asInstanceOf[T]
      case Types.STRING => valueStr.asInstanceOf[T]
      case _ => throw TableException(s"Unsupported type ${typeInfo.getTypeClass}.")
    }
  }

/**
  * Tries to derive the type string from the given string value.
  * The derive priority for the types are BOOLEAN, INT, DOUBLE, and VARCHAR.
    *
    * @param valueStr the string formatted value
    * @return the type string of the given value
    */
  def deriveTypeStrFromValueStr(valueStr: String): String = {
    if (valueStr.equals(LITERAL_TRUE) || valueStr.equals(LITERAL_FALSE)) {
      TypeStringUtils.BOOLEAN.key
    } else {
      try {
        valueStr.toInt
        TypeStringUtils.INT.key
      } catch {
        case _: NumberFormatException =>
          try {
            valueStr.toDouble
            TypeStringUtils.DOUBLE.key
          } catch {
            case _: NumberFormatException =>
              TypeStringUtils.STRING.key
          }
      }
    }
  }
}
