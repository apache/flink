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

package org.apache.flink.table.calcite

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.api.java.typeutils.ValueTypeInfo._
import org.apache.flink.table.`type`.DecimalType
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkTypeFactory.typeInfoToSqlTypeName
import org.apache.flink.table.typeutils._

import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.rel.`type`._
import org.apache.calcite.sql.SqlIntervalQualifier
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.parser.SqlParserPos

import java.util

import scala.collection.JavaConverters._

/**
  * Flink specific type factory that represents the interface between Flink's [[TypeInformation]]
  * and Calcite's [[RelDataType]].
  */
class FlinkTypeFactory(typeSystem: RelDataTypeSystem) extends JavaTypeFactoryImpl(typeSystem) {

  // NOTE: for future data types it might be necessary to
  // override more methods of RelDataTypeFactoryImpl

  def isAdvanced(dataType: TypeInformation[_]): Boolean = dataType match {
    case PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO => false
    case _: BasicTypeInfo[_] => false
    case _: SqlTimeTypeInfo[_] => false
    case _: TimeIntervalTypeInfo[_] => false
    case _ => true
  }

  def createTypeFromTypeInfo(
      typeInfo: TypeInformation[_],
      isNullable: Boolean): RelDataType = {

    // we cannot use seenTypes for simple types,
    // because time indicators and timestamps would be the same

    val relType = if (!isAdvanced(typeInfo)) {
      // simple types can be converted to SQL types and vice versa
      val sqlType = typeInfoToSqlTypeName(typeInfo)
      sqlType match {

        case INTERVAL_YEAR_MONTH =>
          createSqlIntervalType(
            new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO))

        case INTERVAL_DAY_SECOND =>
          createSqlIntervalType(
            new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.SECOND, SqlParserPos.ZERO))

        case _ =>
          createSqlType(sqlType)
      }
    } else {
      throw new TableException("Unsupported now")
    }

    createTypeWithNullability(relType, isNullable)
  }

  /**
    * Creates a struct type with the input fieldNames and input fieldTypes using FlinkTypeFactory
    *
    * @param fieldNames field names
    * @param fieldTypes field types, every element is Flink's [[TypeInformation]]
    * @return a struct type with the input fieldNames, input fieldTypes, and system fields
    */
  def buildLogicalRowType(
      fieldNames: Seq[String],
      fieldTypes: Seq[TypeInformation[_]]): RelDataType = {
    buildLogicalRowType(
      fieldNames,
      fieldTypes,
      fieldTypes.map(_ => true))
  }

  /**
    * Creates a struct type with the input fieldNames, input fieldTypes and input fieldNullables
    * using FlinkTypeFactory
    *
    * @param fieldNames     field names
    * @param fieldTypes     field types, every element is Flink's [[TypeInformation]]
    * @param fieldNullables field nullable properties
    * @return a struct type with the input fieldNames, input fieldTypes, and system fields
    */
  def buildLogicalRowType(
      fieldNames: Seq[String],
      fieldTypes: Seq[TypeInformation[_]],
      fieldNullables: Seq[Boolean]): RelDataType = {
    val logicalRowTypeBuilder = builder
    val fields = fieldNames.zip(fieldTypes).zip(fieldNullables)
    fields foreach {
      case ((fieldName, fieldType), fieldNullable) =>
        logicalRowTypeBuilder.add(fieldName, createTypeFromTypeInfo(fieldType, fieldNullable))
    }
    logicalRowTypeBuilder.build
  }

  // ----------------------------------------------------------------------------------------------

  override def getJavaClass(`type`: RelDataType): java.lang.reflect.Type = {
    if (`type`.getSqlTypeName == FLOAT) {
      if (`type`.isNullable) {
        classOf[java.lang.Float]
      } else {
        java.lang.Float.TYPE
      }
    } else {
      super.getJavaClass(`type`)
    }
  }

  override def createSqlType(typeName: SqlTypeName, precision: Int): RelDataType = {
    // it might happen that inferred VARCHAR types overflow as we set them to Int.MaxValue
    // Calcite will limit the length of the VARCHAR type to 65536.
    if (typeName == VARCHAR && precision < 0) {
      createSqlType(typeName, getTypeSystem.getDefaultPrecision(typeName))
    } else {
      super.createSqlType(typeName, precision)
    }
  }

  override def createSqlType(typeName: SqlTypeName): RelDataType = {
    if (typeName == DECIMAL) {
      // if we got here, the precision and scale are not specified, here we
      // keep precision/scale in sync with our type system's default value,
      // see DecimalType.USER_DEFAULT.
      createSqlType(typeName, DecimalType.USER_DEFAULT.precision(),
        DecimalType.USER_DEFAULT.scale())
    } else {
      super.createSqlType(typeName)
    }
  }

  override def createTypeWithNullability(
      relDataType: RelDataType,
      isNullable: Boolean): RelDataType = {

    // nullability change not necessary
    if (relDataType.isNullable == isNullable) {
      return canonize(relDataType)
    }

    // change nullability
    val newType = super.createTypeWithNullability(relDataType, isNullable)

    canonize(newType)
  }

  override def leastRestrictive(types: util.List[RelDataType]): RelDataType = {
    val type0 = types.get(0)
    if (type0.getSqlTypeName != null) {
      val resultType = resolveAllIdenticalTypes(types)
      if (resultType.isDefined) {
        // result type for identical types
        return resultType.get
      }
    }
    // fall back to super
    super.leastRestrictive(types)
  }

  private def resolveAllIdenticalTypes(types: util.List[RelDataType]): Option[RelDataType] = {
    val allTypes = types.asScala

    val head = allTypes.head
    // check if all types are the same
    if (allTypes.forall(_ == head)) {
      // types are the same, check nullability
      val nullable = allTypes
        .exists(sqlType => sqlType.isNullable || sqlType.getSqlTypeName == SqlTypeName.NULL)
      // return type with nullability
      Some(createTypeWithNullability(head, nullable))
    } else {
      // types are not all the same
      if (allTypes.exists(_.getSqlTypeName == SqlTypeName.ANY)) {
        // one of the type was ANY.
        // we cannot generate a common type if it differs from other types.
        throw new TableException("Generic ANY types must have a common type information.")
      } else {
        // cannot resolve a common type for different input types
        None
      }
    }
  }

}

object FlinkTypeFactory {

  private[flink] def typeInfoToSqlTypeName(typeInfo: TypeInformation[_]): SqlTypeName =
    typeInfo match {
      case BOOLEAN_TYPE_INFO => BOOLEAN
      case BYTE_TYPE_INFO => TINYINT
      case SHORT_TYPE_INFO => SMALLINT
      case INT_TYPE_INFO => INTEGER
      case LONG_TYPE_INFO => BIGINT
      case FLOAT_TYPE_INFO => FLOAT
      case DOUBLE_TYPE_INFO => DOUBLE
      case STRING_TYPE_INFO => VARCHAR

      // temporal types
      case SqlTimeTypeInfo.DATE => DATE
      case SqlTimeTypeInfo.TIME => TIME
      case SqlTimeTypeInfo.TIMESTAMP => TIMESTAMP
      case TimeIntervalTypeInfo.INTERVAL_MONTHS => INTERVAL_YEAR_MONTH
      case TimeIntervalTypeInfo.INTERVAL_MILLIS => INTERVAL_DAY_SECOND

      case PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO => VARBINARY

      case CHAR_TYPE_INFO | CHAR_VALUE_TYPE_INFO =>
        throw new TableException("Character type is not supported.")

      case _@t =>
        throw new TableException(s"Type is not supported: $t")
    }
}
