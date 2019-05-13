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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.`type`.TypeConverters.createInternalTypeFromTypeInfo
import org.apache.flink.table.`type`._
import org.apache.flink.table.api.{TableException, TableSchema}
import org.apache.flink.table.plan.schema.{GenericRelDataType, _}
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo

import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.rel.`type`._
import org.apache.calcite.sql.SqlIntervalQualifier
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.`type`.{BasicSqlType, SqlTypeName, SqlTypeUtil}
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.util.ConversionUtil

import java.nio.charset.Charset
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Flink specific type factory that represents the interface between Flink's [[InternalType]]
  * and Calcite's [[RelDataType]].
  */
class FlinkTypeFactory(typeSystem: RelDataTypeSystem) extends JavaTypeFactoryImpl(typeSystem) {

  // NOTE: for future data types it might be necessary to
  // override more methods of RelDataTypeFactoryImpl

  private val seenTypes = mutable.HashMap[(InternalType, Boolean), RelDataType]()

  def createTypeFromInternalType(
      tp: InternalType,
      isNullable: Boolean): RelDataType = {

    val relType = seenTypes.get((tp, isNullable)) match {
      case Some(retType: RelDataType) => retType
      case None =>
        val refType = tp match {
          case InternalTypes.BOOLEAN => createSqlType(BOOLEAN)
          case InternalTypes.BYTE => createSqlType(TINYINT)
          case InternalTypes.SHORT => createSqlType(SMALLINT)
          case InternalTypes.INT => createSqlType(INTEGER)
          case InternalTypes.LONG => createSqlType(BIGINT)
          case InternalTypes.FLOAT => createSqlType(FLOAT)
          case InternalTypes.DOUBLE => createSqlType(DOUBLE)
          case InternalTypes.STRING => createSqlType(VARCHAR)

          // temporal types
          case InternalTypes.DATE => createSqlType(DATE)
          case InternalTypes.TIME => createSqlType(TIME)
          case InternalTypes.TIMESTAMP => createSqlType(TIMESTAMP)
          case InternalTypes.PROCTIME_INDICATOR => createProctimeIndicatorType()
          case InternalTypes.ROWTIME_INDICATOR => createRowtimeIndicatorType()

          // interval types
          case InternalTypes.INTERVAL_MONTHS =>
            createSqlIntervalType(
              new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO))
          case InternalTypes.INTERVAL_MILLIS =>
            createSqlIntervalType(
              new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.SECOND, SqlParserPos.ZERO))

          case InternalTypes.BINARY => createSqlType(VARBINARY)

          case decimal: DecimalType =>
            createSqlType(DECIMAL, decimal.precision(), decimal.scale())

          case rowType: RowType => new RowRelDataType(rowType, isNullable, this)

          case arrayType: ArrayType => new ArrayRelDataType(arrayType,
            createTypeFromInternalType(arrayType.getElementType, isNullable = true), isNullable)

          case mapType: MapType => new MapRelDataType(
            mapType,
            createTypeFromInternalType(mapType.getKeyType, isNullable = true),
            createTypeFromInternalType(mapType.getValueType, isNullable = true),
            isNullable)

          case multisetType: MultisetType => new MultisetRelDataType(
            multisetType,
            createTypeFromInternalType(multisetType.getElementType, isNullable = true),
            isNullable)

          case generic: GenericType[_] =>
            new GenericRelDataType(generic, isNullable, getTypeSystem)

          case _@t =>
            throw new TableException(s"Type is not supported: $t")
        }
        seenTypes.put((tp, isNullable), refType)
        refType
    }

    createTypeWithNullability(relType, isNullable)
  }

  /**
    * Creates a indicator type for processing-time, but with similar properties as SQL timestamp.
    */
  def createProctimeIndicatorType(): RelDataType = {
    val originalType = createTypeFromInternalType(InternalTypes.TIMESTAMP, isNullable = false)
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
    val originalType = createTypeFromInternalType(InternalTypes.TIMESTAMP, isNullable = false)
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
    * @param fieldTypes field types, every element is Flink's [[InternalType]]
    * @return a struct type with the input fieldNames, input fieldTypes, and system fields
    */
  def buildLogicalRowType(
      fieldNames: Seq[String],
      fieldTypes: Seq[InternalType]): RelDataType = {
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
    * @param fieldTypes     field types, every element is Flink's [[InternalType]]
    * @param fieldNullables field nullable properties
    * @return a struct type with the input fieldNames, input fieldTypes, and system fields
    */
  def buildLogicalRowType(
      fieldNames: Seq[String],
      fieldTypes: Seq[InternalType],
      fieldNullables: Seq[Boolean]): RelDataType = {
    val logicalRowTypeBuilder = builder
    val fields = fieldNames.zip(fieldTypes).zip(fieldNullables)
    fields foreach {
      case ((fieldName, fieldType), fieldNullable) =>
        logicalRowTypeBuilder.add(fieldName, createTypeFromInternalType(fieldType, fieldNullable))
    }
    logicalRowTypeBuilder.build
  }

  /**
    * Created a struct type with the input table schema using FlinkTypeFactory
    * @param tableSchema  the table schema
    * @return a struct type with the input fieldNames, input fieldTypes, and system fields
    */
  def buildLogicalRowType(tableSchema: TableSchema, isStreaming: Option[Boolean]): RelDataType = {
    buildRelDataType(
      tableSchema.getFieldNames.toSeq,
      tableSchema.getFieldTypes map {
        case TimeIndicatorTypeInfo.PROCTIME_INDICATOR
          if isStreaming.isDefined && !isStreaming.get =>
          InternalTypes.TIMESTAMP
        case TimeIndicatorTypeInfo.ROWTIME_INDICATOR
          if isStreaming.isDefined && !isStreaming.get =>
          InternalTypes.TIMESTAMP
        case tpe: TypeInformation[_] => createInternalTypeFromTypeInfo(tpe)
      })
  }

  def buildRelDataType(
      fieldNames: Seq[String],
      fieldTypes: Seq[InternalType]): RelDataType = {
    buildRelDataType(
      fieldNames,
      fieldTypes,
      fieldTypes.map(!FlinkTypeFactory.isTimeIndicatorType(_)))
  }

  def buildRelDataType(
      fieldNames: Seq[String],
      fieldTypes: Seq[InternalType],
      fieldNullables: Seq[Boolean]): RelDataType = {
    val b = builder
    val fields = fieldNames.zip(fieldTypes).zip(fieldNullables)
    fields foreach {
      case ((fieldName, fieldType), fieldNullable) =>
        if (FlinkTypeFactory.isTimeIndicatorType(fieldType) && fieldNullable) {
          throw new TableException(
            s"$fieldName can not be nullable because it is TimeIndicatorType!")
        }
        b.add(fieldName, createTypeFromInternalType(fieldType, fieldNullable))
    }
    b.build
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


  override def createArrayType(elementType: RelDataType, maxCardinality: Long): RelDataType = {
    val arrayType = InternalTypes.createArrayType(FlinkTypeFactory.toInternalType(elementType))
    val relType = new ArrayRelDataType(
      arrayType,
      elementType,
      isNullable = false)
    canonize(relType)
  }

  override def createMapType(keyType: RelDataType, valueType: RelDataType): RelDataType = {
    val internalKeyType = FlinkTypeFactory.toInternalType(keyType)
    val internalValueType = FlinkTypeFactory.toInternalType(valueType)
    val internalMapType = InternalTypes.createMapType(internalKeyType, internalValueType)
    val relType = new MapRelDataType(
      internalMapType,
      keyType,
      valueType,
      isNullable = false)
    canonize(relType)
  }

  override def createMultisetType(elementType: RelDataType, maxCardinality: Long): RelDataType = {
    val internalElementType = FlinkTypeFactory.toInternalType(elementType)
    val relType = new MultisetRelDataType(
      InternalTypes.createMultisetType(internalElementType),
      elementType,
      isNullable = false)
    canonize(relType)
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
    val newType = relDataType match {
      case array: ArrayRelDataType =>
        new ArrayRelDataType(array.arrayType, array.getComponentType, isNullable)

      case map: MapRelDataType =>
        new MapRelDataType(map.mapType, map.keyType, map.valueType, isNullable)

      case multiSet: MultisetRelDataType =>
        new MultisetRelDataType(multiSet.multisetType, multiSet.getComponentType, isNullable)

      case generic: GenericRelDataType =>
        new GenericRelDataType(generic.genericType, isNullable, typeSystem)

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
        // one of the type was ANY.
        // we cannot generate a common type if it differs from other types.
        throw new TableException("Generic ANY types must have a common type information.")
      } else {
        // cannot resolve a common type for different input types
        None
      }
    }
  }

  override def getDefaultCharset: Charset = {
    Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME)
  }

  /**
    * Calcite's default impl for division is apparently borrowed from T-SQL,
    * but the details are a little different, e.g. when Decimal(34,0)/Decimal(10,0)
    * To avoid confusion, follow the exact T-SQL behavior.
    * Note that for (+-*), Calcite is also different from T-SQL;
    * however, Calcite conforms to SQL2003 while T-SQL does not.
    * therefore we keep Calcite's behavior on (+-*).
    */
  override def createDecimalQuotient(type1: RelDataType, type2: RelDataType): RelDataType = {
    if (SqlTypeUtil.isExactNumeric(type1) && SqlTypeUtil.isExactNumeric(type2) &&
        (SqlTypeUtil.isDecimal(type1) || SqlTypeUtil.isDecimal(type2))) {
      val result = DecimalType.inferDivisionType(
        type1.getPrecision, type1.getScale,
        type2.getPrecision, type2.getScale)
      createSqlType(SqlTypeName.DECIMAL, result.precision, result.scale)
    } else {
      null
    }
  }
}

object FlinkTypeFactory {

  def isTimeIndicatorType(t: InternalType): Boolean = t match {
    case InternalTypes.ROWTIME_INDICATOR | InternalTypes.PROCTIME_INDICATOR => true
    case _ => false
  }

  def isTimeIndicatorType(relDataType: RelDataType): Boolean = relDataType match {
    case _: TimeIndicatorRelDataType => true
    case _ => false
  }

  def isRowtimeIndicatorType(relDataType: RelDataType): Boolean = relDataType match {
    case ti: TimeIndicatorRelDataType if ti.isEventTime => true
    case _ => false
  }

  def isProctimeIndicatorType(relDataType: RelDataType): Boolean = relDataType match {
    case ti: TimeIndicatorRelDataType if !ti.isEventTime => true
    case _ => false
  }

  def toInternalType(relDataType: RelDataType): InternalType = relDataType.getSqlTypeName match {
    case BOOLEAN => InternalTypes.BOOLEAN
    case TINYINT => InternalTypes.BYTE
    case SMALLINT => InternalTypes.SHORT
    case INTEGER => InternalTypes.INT
    case BIGINT => InternalTypes.LONG
    case FLOAT => InternalTypes.FLOAT
    case DOUBLE => InternalTypes.DOUBLE
    case VARCHAR | CHAR => InternalTypes.STRING
    case VARBINARY | BINARY => InternalTypes.BINARY
    case DECIMAL => InternalTypes.createDecimalType(relDataType.getPrecision, relDataType.getScale)

    // time indicators
    case TIMESTAMP if relDataType.isInstanceOf[TimeIndicatorRelDataType] =>
      val indicator = relDataType.asInstanceOf[TimeIndicatorRelDataType]
      if (indicator.isEventTime) {
        InternalTypes.ROWTIME_INDICATOR
      } else {
        InternalTypes.PROCTIME_INDICATOR
      }

    // temporal types
    case DATE => InternalTypes.DATE
    case TIME => InternalTypes.TIME
    case TIMESTAMP => InternalTypes.TIMESTAMP
    case typeName if YEAR_INTERVAL_TYPES.contains(typeName) => InternalTypes.INTERVAL_MONTHS
    case typeName if DAY_INTERVAL_TYPES.contains(typeName) => InternalTypes.INTERVAL_MILLIS

    case NULL =>
      throw new TableException(
        "Type NULL is not supported. Null values must have a supported type.")

    // symbol for special flags e.g. TRIM's BOTH, LEADING, TRAILING
    // are represented as Enum
    case SYMBOL => InternalTypes.createGenericType(classOf[Enum[_]])

    // extract encapsulated Type
    case ANY if relDataType.isInstanceOf[GenericRelDataType] =>
      val genericRelDataType = relDataType.asInstanceOf[GenericRelDataType]
      genericRelDataType.genericType

    case ROW if relDataType.isInstanceOf[RowRelDataType] =>
      val compositeRelDataType = relDataType.asInstanceOf[RowRelDataType]
      compositeRelDataType.rowType

    case ROW if relDataType.isInstanceOf[RelRecordType] =>
      val relRecordType = relDataType.asInstanceOf[RelRecordType]
      new RowSchema(relRecordType).internalType

    case MULTISET if relDataType.isInstanceOf[MultisetRelDataType] =>
      val multisetRelDataType = relDataType.asInstanceOf[MultisetRelDataType]
      multisetRelDataType.multisetType

    case ARRAY if relDataType.isInstanceOf[ArrayRelDataType] =>
      val arrayRelDataType = relDataType.asInstanceOf[ArrayRelDataType]
      arrayRelDataType.arrayType

    case MAP if relDataType.isInstanceOf[MapRelDataType] =>
      val mapRelDataType = relDataType.asInstanceOf[MapRelDataType]
      mapRelDataType.mapType

    case _@t =>
      throw new TableException(s"Type is not supported: $t")
  }

  def toInternalRowType(logicalRowType: RelDataType): RowType = {
    // convert to InternalType
    val logicalFieldTypes = logicalRowType.getFieldList.asScala map {
      relDataType => FlinkTypeFactory.toInternalType(relDataType.getType)
    }
    // field names
    val logicalFieldNames = logicalRowType.getFieldNames.asScala
    new RowType(logicalFieldTypes.toArray[InternalType], logicalFieldNames.toArray)
  }
}
