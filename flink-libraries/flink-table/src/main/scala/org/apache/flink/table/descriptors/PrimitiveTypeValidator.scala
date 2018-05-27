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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.table.api.TableException
import org.apache.flink.table.descriptors.PrimitiveTypeValidator.PRIMITIVE_VALUE
import scala.collection.JavaConversions._

/**
  * Validator for [[PrimitiveTypeDescriptor]].
  */
class PrimitiveTypeValidator extends HierarchyDescriptorValidator {
  override def validateWithPrefix(keyPrefix: String, properties: DescriptorProperties): Unit = {
    properties
      .validateType(s"$keyPrefix${PrimitiveTypeValidator.PRIMITIVE_TYPE}", isOptional = false)
    properties
      .validateString(s"$keyPrefix${PrimitiveTypeValidator.PRIMITIVE_VALUE}", isOptional = false, 1)
  }
}

object PrimitiveTypeValidator {
  val PRIMITIVE_TYPE = "type"
  val PRIMITIVE_VALUE = "value"

  def derivePrimitiveValue(keyPrefix: String, properties: DescriptorProperties): Any = {
    val typeInfo =
      properties.getType(s"$keyPrefix$PRIMITIVE_TYPE")
    val valueKey = s"$keyPrefix$PRIMITIVE_VALUE"
    val value = typeInfo match {
      case basicType: BasicTypeInfo[_] =>
        basicType match {
          case BasicTypeInfo.INT_TYPE_INFO =>
            properties.getInt(valueKey)
          case BasicTypeInfo.LONG_TYPE_INFO =>
            properties.getLong(valueKey)
          case BasicTypeInfo.DOUBLE_TYPE_INFO =>
            properties.getDouble(valueKey)
          case BasicTypeInfo.STRING_TYPE_INFO =>
            properties.getString(valueKey)
          case BasicTypeInfo.BOOLEAN_TYPE_INFO =>
            properties.getBoolean(valueKey)
          //TODO add more types
          case _ => throw TableException(s"Unsupported basic type ${basicType.getTypeClass}.")
        }
      case _ => throw TableException("Only primitive types are supported.")
    }
    value
  }
}






