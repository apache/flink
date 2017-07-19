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
import org.apache.calcite.rel.`type`._
import org.apache.calcite.sql.SqlIntervalQualifier
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.`type`.{BasicSqlType, SqlTypeName}
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.ValueTypeInfo._
import org.apache.flink.api.java.typeutils.{MapTypeInfo, ObjectArrayTypeInfo, RowTypeInfo}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkTypeFactory.typeInfoToSqlTypeName
import org.apache.flink.table.plan.schema._
import org.apache.flink.table.typeutils.TypeCheckUtils.isSimple
import org.apache.flink.table.typeutils.{TimeIndicatorTypeInfo, TimeIntervalTypeInfo}
import org.apache.flink.types.Row

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Flink specific type factory that represents the interface between Flink's [[TypeInformation]]
  * and Calcite's [[RelDataType]].
  */
class FlinkTypeFactory(typeSystem: RelDataTypeSystem) extends JavaTypeFactoryImpl(typeSystem) {

  // NOTE: for future data types it might be necessary to
  // override more methods of RelDataTypeFactoryImpl

  private val seenTypes = mutable.HashMap[TypeInformation[_], RelDataType]()

  def createTypeFromTypeInfo(typeInfo: TypeInformation[_]): RelDataType =
    createTypeFromTypeInfo(typeInfo, nullable = false)

  def createTypeFromTypeInfo(typeInfo: TypeInformation[_], nullable: Boolean): RelDataType = {
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

        case TIMESTAMP if typeInfo.isInstanceOf[TimeIndicatorTypeInfo] =>
          if (typeInfo.asInstanceOf[TimeIndicatorTypeInfo].isEventTime) {
            createRowtimeIndicatorType()
          } else {
            createProctimeIndicatorType()
          }

        case _ =>
          createTypeWithNullability(createSqlType(sqlType), nullable)
      }
    }
    // advanced types require specific RelDataType
    // for storing the original TypeInformation
    else {
      seenTypes.getOrElseUpdate(typeInfo, canonize(createAdvancedType(typeInfo)))
    }
  }

  /**
    * Creates a indicator type for processing-time, but with similar properties as SQL timestamp.
    */
  def createProctimeIndicatorType(): RelDataType = {
    val originalType = createTypeFromTypeInfo(SqlTimeTypeInfo.TIMESTAMP)
    canonize(
      new TimeIndicatorRelDataType(
        getTypeSystem,
        originalType.asInstanceOf[BasicSqlType],
        isEventTime = false)
    )
  }

  /**
    * Creates a indicator type for event-time, but with similar properties as SQL timestamp.
    */
  def createRowtimeIndicatorType(): RelDataType = {
    val originalType = createTypeFromTypeInfo(SqlTimeTypeInfo.TIMESTAMP)
    canonize(
      new TimeIndicatorRelDataType(
        getTypeSystem,
        originalType.asInstanceOf[BasicSqlType],
        isEventTime = true)
    )
  }

  /**
    * Creates a struct type with the input fieldNames and input fieldTypes using FlinkTypeFactory
    *
    * @param fieldNames field names
    * @param fieldTypes field types, every element is Flink's [[TypeInformation]]
    * @param rowtime optional system field to indicate event-time; the index determines the index
    *                in the final record. If the index is smaller than the number of specified
    *                fields, it shifts all following fields.
    * @param proctime optional system field to indicate processing-time; the index determines the
    *                 index in the final record. If the index is smaller than the number of
    *                 specified fields, it shifts all following fields.
    * @return a struct type with the input fieldNames, input fieldTypes, and system fields
    */
  def buildLogicalRowType(
      fieldNames: Seq[String],
      fieldTypes: Seq[TypeInformation[_]],
      rowtime: Option[(Int, String)],
      proctime: Option[(Int, String)])
    : RelDataType = {
    val logicalRowTypeBuilder = builder

    val fields = fieldNames.zip(fieldTypes)

    var totalNumberOfFields = fields.length
    if (rowtime.isDefined) {
      totalNumberOfFields += 1
    }
    if (proctime.isDefined) {
      totalNumberOfFields += 1
    }

    var addedTimeAttributes = 0
    for (i <- 0 until totalNumberOfFields) {
      if (rowtime.isDefined && rowtime.get._1 == i) {
        logicalRowTypeBuilder.add(rowtime.get._2, createRowtimeIndicatorType())
        addedTimeAttributes += 1
      } else if (proctime.isDefined && proctime.get._1 == i) {
        logicalRowTypeBuilder.add(proctime.get._2, createProctimeIndicatorType())
        addedTimeAttributes += 1
      } else {
        val field = fields(i - addedTimeAttributes)
        logicalRowTypeBuilder.add(field._1, createTypeFromTypeInfo(field._2)).nullable(true)
      }
    }

    logicalRowTypeBuilder.build
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

    case ba: BasicArrayTypeInfo[_, _] =>
      new ArrayRelDataType(ba, createTypeFromTypeInfo(ba.getComponentInfo), true)

    case oa: ObjectArrayTypeInfo[_, _] =>
      new ArrayRelDataType(oa, createTypeFromTypeInfo(oa.getComponentInfo), true)

    case mp: MapTypeInfo[_, _] =>
      new MapRelDataType(mp, createTypeFromTypeInfo(mp.getKeyTypeInfo),
        createTypeFromTypeInfo(mp.getValueTypeInfo), true)

    case ti: TypeInformation[_] =>
      createTypeWithNullability(
        new GenericRelDataType(ti, getTypeSystem.asInstanceOf[FlinkTypeSystem]),
        nullable = true
      )

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
  @deprecated("Use the RowSchema class instead because it handles both logical and physical rows.")
  def toInternalRowTypeInfo(logicalRowType: RelDataType): TypeInformation[Row] = {
    // convert to type information
    val logicalFieldTypes = logicalRowType.getFieldList.asScala map { relDataType =>
      FlinkTypeFactory.toTypeInfo(relDataType.getType)
    }
    // field names
    val logicalFieldNames = logicalRowType.getFieldNames.asScala
    new RowTypeInfo(logicalFieldTypes.toArray, logicalFieldNames.toArray)
  }

  def isProctimeIndicatorType(relDataType: RelDataType): Boolean = relDataType match {
    case ti: TimeIndicatorRelDataType if !ti.isEventTime => true
    case _ => false
  }

  def isProctimeIndicatorType(typeInfo: TypeInformation[_]): Boolean = typeInfo match {
    case ti: TimeIndicatorTypeInfo if !ti.isEventTime => true
    case _ => false
  }

  def isRowtimeIndicatorType(relDataType: RelDataType): Boolean = relDataType match {
    case ti: TimeIndicatorRelDataType if ti.isEventTime => true
    case _ => false
  }

  def isRowtimeIndicatorType(typeInfo: TypeInformation[_]): Boolean = typeInfo match {
    case ti: TimeIndicatorTypeInfo if ti.isEventTime => true
    case _ => false
  }

  def isTimeIndicatorType(relDataType: RelDataType): Boolean = relDataType match {
    case ti: TimeIndicatorRelDataType => true
    case _ => false
  }

  def isTimeIndicatorType(typeInfo: TypeInformation[_]): Boolean = typeInfo match {
    case ti: TimeIndicatorTypeInfo => true
    case _ => false
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

    // time indicators
    case TIMESTAMP if relDataType.isInstanceOf[TimeIndicatorRelDataType] =>
      val indicator = relDataType.asInstanceOf[TimeIndicatorRelDataType]
      if (indicator.isEventTime) {
        TimeIndicatorTypeInfo.ROWTIME_INDICATOR
      } else {
        TimeIndicatorTypeInfo.PROCTIME_INDICATOR
      }

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

    case MAP if relDataType.isInstanceOf[MapRelDataType] =>
      val mapRelDataType = relDataType.asInstanceOf[MapRelDataType]
      mapRelDataType.typeInfo

    case _@t =>
      throw TableException(s"Type is not supported: $t")
  }
}
