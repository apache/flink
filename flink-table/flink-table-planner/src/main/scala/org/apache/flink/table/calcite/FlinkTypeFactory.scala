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

import java.util
import java.nio.charset.Charset
import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.rel.`type`._
import org.apache.calcite.sql.SqlIntervalQualifier
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.`type`.{BasicSqlType, SqlTypeName}
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.util.ConversionUtil
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.ValueTypeInfo._
import org.apache.flink.api.java.typeutils.{MapTypeInfo, MultisetTypeInfo, ObjectArrayTypeInfo, RowTypeInfo}
import org.apache.flink.table.api.{TableException, TableSchema}
import org.apache.flink.table.calcite.FlinkTypeFactory.typeInfoToSqlTypeName
import org.apache.flink.table.plan.schema._
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.{LogicalType, TimestampKind, TimestampType}
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.table.typeutils.TypeCheckUtils.isSimple
import org.apache.flink.table.typeutils.{TimeIndicatorTypeInfo, TimeIntervalTypeInfo}
import org.apache.flink.types.Row

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Flink specific type factory that represents the interface between Flink's [[TypeInformation]]
  * and Calcite's [[RelDataType]].
  */
class FlinkTypeFactory(typeSystem: RelDataTypeSystem)
  extends JavaTypeFactoryImpl(typeSystem)
  with ExtendedRelTypeFactory {

  // NOTE: for future data types it might be necessary to
  // override more methods of RelDataTypeFactoryImpl

  def createTypeFromTypeInfo(
      typeInfo: TypeInformation[_],
      isNullable: Boolean)
    : RelDataType = {

    val relType = if (isSimple(typeInfo)) {
      // simple types can be converted to SQL types and vice versa
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
          createSqlType(sqlType)
      }
    } else {
      createAdvancedType(typeInfo, isNullable)
    }

    createTypeWithNullability(relType, isNullable)
  }

  /**
    * Creates a indicator type for processing-time, but with similar properties as SQL timestamp.
    */
  def createProctimeIndicatorType(): RelDataType = {
    val originalType = createTypeFromTypeInfo(SqlTimeTypeInfo.TIMESTAMP, isNullable = false)
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
    val originalType = createTypeFromTypeInfo(SqlTimeTypeInfo.TIMESTAMP, isNullable = false)
    canonize(
      new TimeIndicatorRelDataType(
        getTypeSystem,
        originalType.asInstanceOf[BasicSqlType],
        isEventTime = true)
    )
  }

  /**
    * Creates types that create custom [[RelDataType]]s that wrap Flink's [[TypeInformation]].
    */
  private def createAdvancedType(
      typeInfo: TypeInformation[_],
      isNullable: Boolean): RelDataType = {

    val relType = typeInfo match {

      case ct: CompositeType[_] =>
        new CompositeRelDataType(ct, isNullable, this)

      case pa: PrimitiveArrayTypeInfo[_] =>
        new ArrayRelDataType(
          pa,
          createTypeFromTypeInfo(pa.getComponentType, isNullable = false),
          isNullable)

      case ba: BasicArrayTypeInfo[_, _] =>
        new ArrayRelDataType(
          ba,
          createTypeFromTypeInfo(ba.getComponentInfo, isNullable = true),
          isNullable)

      case oa: ObjectArrayTypeInfo[_, _] =>
        new ArrayRelDataType(
          oa,
          createTypeFromTypeInfo(oa.getComponentInfo, isNullable = true),
          isNullable)

      case mts: MultisetTypeInfo[_] =>
        new MultisetRelDataType(
          mts,
          createTypeFromTypeInfo(mts.getElementTypeInfo, isNullable = true),
          isNullable
        )

      case mp: MapTypeInfo[_, _] =>
        new MapRelDataType(
          mp,
          createTypeFromTypeInfo(mp.getKeyTypeInfo, isNullable = true),
          createTypeFromTypeInfo(mp.getValueTypeInfo, isNullable = true),
          isNullable)

      case ti: TypeInformation[_] =>
        new GenericRelDataType(
          ti,
          isNullable,
          getTypeSystem.asInstanceOf[FlinkTypeSystem])

      case ti@_ =>
        throw new TableException(s"Unsupported type information: $ti")
    }

    canonize(relType)
  }

  /**
    * Creates a struct type with the input fieldNames and input fieldTypes using FlinkTypeFactory
    *
    * @param tableSchema schema to convert to Calcite's specific one
    * @return a struct type with the input fieldNames, input fieldTypes, and system fields
    */
  def buildLogicalRowType(tableSchema: TableSchema): RelDataType = {
    buildLogicalRowType(tableSchema.getFieldNames, tableSchema.getFieldDataTypes)
  }

  /**
   * Creates a struct type with the input fieldNames and input fieldTypes using FlinkTypeFactory
   *
   * @param fieldNames field names
   * @param fieldTypes field types, every element is Flink's [[DataType]]
   * @return a struct type with the input fieldNames, input fieldTypes, and system fields
   */
  def buildLogicalRowType(
    fieldNames: Array[String],
    fieldTypes: Array[DataType])
  : RelDataType = {
    val logicalRowTypeBuilder = builder

    val fields = fieldNames.zip(fieldTypes)
    fields.foreach(f => {
      // time indicators are not nullable
      val logicalType = f._2.getLogicalType
      val nullable  = if (FlinkTypeFactory.isTimeIndicatorType(logicalType)) {
        false
      } else {
        logicalType.isNullable
      }

      logicalRowTypeBuilder.add(f._1,
        createTypeFromTypeInfo(TypeConversions.fromDataTypeToLegacyInfo(f._2), nullable))
    })

    logicalRowTypeBuilder.build
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
      fieldTypes: Seq[TypeInformation[_]])
    : RelDataType = {
    val logicalRowTypeBuilder = builder

    val fields = fieldNames.zip(fieldTypes)
    fields.foreach(f => {
      // time indicators are not nullable
      val nullable = !FlinkTypeFactory.isTimeIndicatorType(f._2)
      logicalRowTypeBuilder.add(f._1, createTypeFromTypeInfo(f._2, nullable))
    })

    logicalRowTypeBuilder.build
  }

  // ----------------------------------------------------------------------------------------------

  override def createSqlType(typeName: SqlTypeName, precision: Int): RelDataType = {
    // it might happen that inferred VARCHAR types overflow as we set them to Int.MaxValue
    // Calcite will limit the length of the VARCHAR type to 65536.
    if (typeName == VARCHAR && precision < 0) {
      createSqlType(typeName, getTypeSystem.getDefaultPrecision(typeName))
    } else {
      super.createSqlType(typeName, precision)
    }
  }

  override def createArrayType(elementType: RelDataType, maxCardinality: Long): RelDataType = {
    val relType = new ArrayRelDataType(
      ObjectArrayTypeInfo.getInfoFor(FlinkTypeFactory.toTypeInfo(elementType)),
      elementType,
      isNullable = false)

    canonize(relType)
  }

  override def createMapType(keyType: RelDataType, valueType: RelDataType): RelDataType = {
    val relType = new MapRelDataType(
      new MapTypeInfo(
        FlinkTypeFactory.toTypeInfo(keyType),
        FlinkTypeFactory.toTypeInfo(valueType)),
      keyType,
      valueType,
      isNullable = false)
    this.canonize(relType)
  }

  override def createMultisetType(elementType: RelDataType, maxCardinality: Long): RelDataType = {
    val relType = new MultisetRelDataType(
      MultisetTypeInfo.getInfoFor(FlinkTypeFactory.toTypeInfo(elementType)),
      elementType,
      isNullable = false)
    canonize(relType)
  }

  override def createRawType(className: String, serializerString: String): RelDataType = {
    throw new TableException("RAW types are only supported in the Blink planner.")
  }

  override def createTypeWithNullability(
      relDataType: RelDataType,
      isNullable: Boolean): RelDataType = {

    // nullability change not necessary
    if (relDataType.isNullable == isNullable) {
      return canonize(relDataType)
    }

    // change nullability
    val newType = relDataType match {

      case composite: CompositeRelDataType =>
        new CompositeRelDataType(composite.compositeType, isNullable, this)

      case array: ArrayRelDataType =>
        new ArrayRelDataType(array.typeInfo, array.getComponentType, isNullable)

      case map: MapRelDataType =>
        new MapRelDataType(map.typeInfo, map.keyType, map.valueType, isNullable)

      case multiSet: MultisetRelDataType =>
        new MultisetRelDataType(multiSet.typeInfo, multiSet.getComponentType, isNullable)

      case generic: GenericRelDataType =>
        new GenericRelDataType(generic.typeInfo, isNullable, typeSystem)

      case timeIndicator: TimeIndicatorRelDataType =>
        timeIndicator

      case _ =>
        super.createTypeWithNullability(relDataType, isNullable)
    }

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
        // one of the type was RAW.
        // we cannot generate a common type if it differs from other types.
        throw new TableException("Generic RAW types must have a common type information.")
      } else {
        // cannot resolve a common type for different input types
        None
      }
    }
  }

  override def getDefaultCharset: Charset = {
    Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME)
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
        throw new TableException("Character type is not supported.")

      case _@t =>
        throw new TableException(s"Type is not supported: $t")
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

  def isTimeIndicatorType(t: LogicalType): Boolean = t match {
    case t: TimestampType
      if t.getKind == TimestampKind.ROWTIME || t.getKind == TimestampKind.PROCTIME => true
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
      throw new TableException(
        "Type NULL is not supported. Null values must have a supported type.")

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

    case ROW if relDataType.isInstanceOf[RelRecordType] =>
      val relRecordType = relDataType.asInstanceOf[RelRecordType]
      new RowSchema(relRecordType).typeInfo

    // CURSOR for UDTF case, whose type info will never be used, just a placeholder
    case CURSOR => new NothingTypeInfo

    case ARRAY if relDataType.isInstanceOf[ArrayRelDataType] =>
      val arrayRelDataType = relDataType.asInstanceOf[ArrayRelDataType]
      arrayRelDataType.typeInfo

    case MAP if relDataType.isInstanceOf[MapRelDataType] =>
      val mapRelDataType = relDataType.asInstanceOf[MapRelDataType]
      mapRelDataType.typeInfo

    case MULTISET if relDataType.isInstanceOf[MultisetRelDataType] =>
      val multisetRelDataType = relDataType.asInstanceOf[MultisetRelDataType]
      multisetRelDataType.typeInfo

    case _@t =>
      throw new TableException(s"Type is not supported: $t")
  }
}
