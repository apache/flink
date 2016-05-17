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

package org.apache.flink.api.table

import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeSystem}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.ValueTypeInfo._
import org.apache.flink.api.table.FlinkTypeFactory.typeInfoToSqlTypeName
import org.apache.flink.api.table.plan.schema.GenericRelDataType
import org.apache.flink.api.table.typeutils.TypeCheckUtils.isSimple

import scala.collection.mutable

/**
  * Flink specific type factory that represents the interface between Flink's [[TypeInformation]]
  * and Calcite's [[RelDataType]].
  */
class FlinkTypeFactory(typeSystem: RelDataTypeSystem) extends JavaTypeFactoryImpl(typeSystem) {

  private val seenTypes = mutable.HashMap[TypeInformation[_], RelDataType]()

  def createTypeFromTypeInfo(typeInfo: TypeInformation[_]): RelDataType = {
    // simple type can be converted to SQL types and vice versa
    if (isSimple(typeInfo)) {
      createSqlType(typeInfoToSqlTypeName(typeInfo))
    }
    // advanced types require specific RelDataType
    // for storing the original TypeInformation
    else {
      seenTypes.getOrElseUpdate(typeInfo, canonize(createAdvancedType(typeInfo)))
    }
  }

  private def createAdvancedType(typeInfo: TypeInformation[_]): RelDataType = typeInfo match {
    // TODO add specific RelDataTypes
    // for PrimitiveArrayTypeInfo, ObjectArrayTypeInfo, CompositeType
    case ti: TypeInformation[_] =>
      new GenericRelDataType(typeInfo, getTypeSystem.asInstanceOf[FlinkTypeSystem])

    case ti@_ =>
      throw new TableException(s"Unsupported type information: $ti")
  }
}

object FlinkTypeFactory {

  private def typeInfoToSqlTypeName(typeInfo: TypeInformation[_]): SqlTypeName = typeInfo match {
      case BOOLEAN_TYPE_INFO => BOOLEAN
      case BYTE_TYPE_INFO => TINYINT
      case SHORT_TYPE_INFO => SMALLINT
      case INT_TYPE_INFO => INTEGER
      case LONG_TYPE_INFO => BIGINT
      case FLOAT_TYPE_INFO => FLOAT
      case DOUBLE_TYPE_INFO => DOUBLE
      case STRING_TYPE_INFO => VARCHAR
      case BIG_DEC_TYPE_INFO => DECIMAL

      // date/time types
      case SqlTimeTypeInfo.DATE => DATE
      case SqlTimeTypeInfo.TIME => TIME
      case SqlTimeTypeInfo.TIMESTAMP => TIMESTAMP

      case CHAR_TYPE_INFO | CHAR_VALUE_TYPE_INFO =>
        throw new TableException("Character type is not supported.")

      case _@t =>
        throw new TableException(s"Type is not supported: $t")
  }

  def toTypeInfo(relDataType: RelDataType): TypeInformation[_] = relDataType.getSqlTypeName match {
    case BOOLEAN => BOOLEAN_TYPE_INFO
    case TINYINT => BYTE_TYPE_INFO
    case SMALLINT => SHORT_TYPE_INFO
    case INTEGER => INT_TYPE_INFO
    case BIGINT => LONG_TYPE_INFO
    case FLOAT => FLOAT_TYPE_INFO
    case DOUBLE => DOUBLE_TYPE_INFO
    case VARCHAR | CHAR => STRING_TYPE_INFO
    case DECIMAL => BIG_DEC_TYPE_INFO

    // date/time types
    case DATE => SqlTimeTypeInfo.DATE
    case TIME => SqlTimeTypeInfo.TIME
    case TIMESTAMP => SqlTimeTypeInfo.TIMESTAMP
    case INTERVAL_DAY_TIME | INTERVAL_YEAR_MONTH =>
      throw new TableException("Intervals are not supported yet.")

    case NULL =>
      throw new TableException("Type NULL is not supported. " +
        "Null values must have a supported type.")

    // symbol for special flags e.g. TRIM's BOTH, LEADING, TRAILING
    // are represented as integer
    case SYMBOL => INT_TYPE_INFO

    // extract encapsulated TypeInformation
    case ANY if relDataType.isInstanceOf[GenericRelDataType] =>
      val genericRelDataType = relDataType.asInstanceOf[GenericRelDataType]
      genericRelDataType.typeInfo

    case _@t =>
      throw new TableException(s"Type is not supported: $t")
  }
}
