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

import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeSystem}
import org.apache.calcite.sql.SqlIntervalQualifier
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{NothingTypeInfo, PrimitiveArrayTypeInfo, SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.{ObjectArrayTypeInfo, RowTypeInfo}
import org.apache.flink.api.java.typeutils.ValueTypeInfo._
import org.apache.flink.table.api.TableException
import org.apache.flink.table.plan.schema.{CompositeRelDataType, GenericRelDataType}
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo
import org.apache.flink.table.typeutils.TypeCheckUtils.isSimple
import org.apache.flink.table.plan.schema.ArrayRelDataType
import org.apache.flink.table.calcite.FlinkTypeFactory.typeInfoToSqlTypeName
import org.apache.flink.types.Row

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
  * Flink specific type factory that represents the interface between Flink's [[TypeInformation]]
  * and Calcite's [[RelDataType]].
  */
class FlinkTypeFactory(typeSystem: RelDataTypeSystem) extends JavaTypeFactoryImpl(typeSystem) {

  // NOTE: for future data types it might be necessary to
  // override more methods of RelDataTypeFactoryImpl

  private val seenTypes = mutable.HashMap[TypeInformation[_], RelDataType]()

  def createTypeFromTypeInfo(typeInfo: TypeInformation[_]): RelDataType = {
    // simple type can be converted to SQL types and vice versa
    if (isSimple(typeInfo)) {
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
    }
    // advanced types require specific RelDataType
    // for storing the original TypeInformation
    else {
      seenTypes.getOrElseUpdate(typeInfo, canonize(createAdvancedType(typeInfo)))
    }
  }

  /**
    * Creates a struct type with the input fieldNames and input fieldTypes using FlinkTypeFactory
    *
    * @param fieldNames field names
    * @param fieldTypes field types, every element is Flink's [[TypeInformation]]
    * @return a struct type with the input fieldNames and input fieldTypes
    */
  def buildRowDataType(
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]])
    : RelDataType = {
    val rowDataTypeBuilder = builder
    fieldNames
      .zip(fieldTypes)
      .foreach { f =>
        rowDataTypeBuilder.add(f._1, createTypeFromTypeInfo(f._2)).nullable(true)
      }
    rowDataTypeBuilder.build
  }

  override def createSqlType(typeName: SqlTypeName, precision: Int): RelDataType = {
    // it might happen that inferred VARCHAR types overflow as we set them to Int.MaxValue
    // always set those to default value
    if (typeName == VARCHAR && precision < 0) {
      createSqlType(typeName, getTypeSystem.getDefaultPrecision(typeName))
    } else {
      super.createSqlType(typeName, precision)
    }
  }

  override def createArrayType(elementType: RelDataType, maxCardinality: Long): RelDataType =
    new ArrayRelDataType(
      ObjectArrayTypeInfo.getInfoFor(FlinkTypeFactory.toTypeInfo(elementType)),
      elementType,
      true)

  private def createAdvancedType(typeInfo: TypeInformation[_]): RelDataType = typeInfo match {
    case ct: CompositeType[_] =>
      new CompositeRelDataType(ct, this)

    case pa: PrimitiveArrayTypeInfo[_] =>
      new ArrayRelDataType(pa, createTypeFromTypeInfo(pa.getComponentType), false)

    case oa: ObjectArrayTypeInfo[_, _] =>
      new ArrayRelDataType(oa, createTypeFromTypeInfo(oa.getComponentInfo), true)

    case ti: TypeInformation[_] =>
      new GenericRelDataType(typeInfo, getTypeSystem.asInstanceOf[FlinkTypeSystem])

    case ti@_ =>
      throw TableException(s"Unsupported type information: $ti")
  }

  override def createTypeWithNullability(
      relDataType: RelDataType,
      nullable: Boolean)
    : RelDataType = relDataType match {
      case composite: CompositeRelDataType =>
        // at the moment we do not care about nullability
        canonize(composite)
      case array: ArrayRelDataType =>
        val elementType = createTypeWithNullability(array.getComponentType, nullable)
        canonize(new ArrayRelDataType(array.typeInfo, elementType, nullable))
      case _ =>
        super.createTypeWithNullability(relDataType, nullable)
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

      // temporal types
      case SqlTimeTypeInfo.DATE => DATE
      case SqlTimeTypeInfo.TIME => TIME
      case SqlTimeTypeInfo.TIMESTAMP => TIMESTAMP
      case TimeIntervalTypeInfo.INTERVAL_MONTHS => INTERVAL_YEAR_MONTH
      case TimeIntervalTypeInfo.INTERVAL_MILLIS => INTERVAL_DAY_SECOND

      case CHAR_TYPE_INFO | CHAR_VALUE_TYPE_INFO =>
        throw TableException("Character type is not supported.")

      case _@t =>
        throw TableException(s"Type is not supported: $t")
  }

  /**
    * Converts a Calcite logical record into a Flink type information.
    */
  def toInternalRowTypeInfo(logicalRowType: RelDataType): TypeInformation[Row] = {
    // convert to type information
    val logicalFieldTypes = logicalRowType.getFieldList.asScala map { relDataType =>
      FlinkTypeFactory.toTypeInfo(relDataType.getType)
    }
    // field names
    val logicalFieldNames = logicalRowType.getFieldNames.asScala
    new RowTypeInfo(logicalFieldTypes.toArray, logicalFieldNames.toArray)
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

    // temporal types
    case DATE => SqlTimeTypeInfo.DATE
    case TIME => SqlTimeTypeInfo.TIME
    case TIMESTAMP => SqlTimeTypeInfo.TIMESTAMP
    case typeName if YEAR_INTERVAL_TYPES.contains(typeName) => TimeIntervalTypeInfo.INTERVAL_MONTHS
    case typeName if DAY_INTERVAL_TYPES.contains(typeName) => TimeIntervalTypeInfo.INTERVAL_MILLIS

    case NULL =>
      throw TableException("Type NULL is not supported. Null values must have a supported type.")

    // symbol for special flags e.g. TRIM's BOTH, LEADING, TRAILING
    // are represented as integer
    case SYMBOL => INT_TYPE_INFO

    // extract encapsulated TypeInformation
    case ANY if relDataType.isInstanceOf[GenericRelDataType] =>
      val genericRelDataType = relDataType.asInstanceOf[GenericRelDataType]
      genericRelDataType.typeInfo

    case ROW if relDataType.isInstanceOf[CompositeRelDataType] =>
      val compositeRelDataType = relDataType.asInstanceOf[CompositeRelDataType]
      compositeRelDataType.compositeType

    // ROW and CURSOR for UDTF case, whose type info will never be used, just a placeholder
    case ROW | CURSOR => new NothingTypeInfo

    case ARRAY if relDataType.isInstanceOf[ArrayRelDataType] =>
      val arrayRelDataType = relDataType.asInstanceOf[ArrayRelDataType]
      arrayRelDataType.typeInfo

    case _@t =>
      throw TableException(s"Type is not supported: $t")
  }
}
