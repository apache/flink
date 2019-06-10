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

import org.apache.flink.api.common.typeinfo.NothingTypeInfo
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.table.api.{DataTypes, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory.toLogicalType
import org.apache.flink.table.plan.schema.{GenericRelDataType, _}
import org.apache.flink.table.types.logical._
import org.apache.flink.types.Nothing
import org.apache.flink.util.Preconditions.checkArgument

import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`._
import org.apache.calcite.sql.SqlIntervalQualifier
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.`type`.{BasicSqlType, MapSqlType, SqlTypeName, SqlTypeUtil}
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.util.ConversionUtil

import java.nio.charset.Charset
import java.util

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Flink specific type factory that represents the interface between Flink's [[LogicalType]]
  * and Calcite's [[RelDataType]].
  */
class FlinkTypeFactory(typeSystem: RelDataTypeSystem) extends JavaTypeFactoryImpl(typeSystem) {

  private val seenTypes = mutable.HashMap[LogicalType, RelDataType]()

  /**
    * Create a calcite field type in table schema from [[LogicalType]]. It use
    * PEEK_FIELDS_NO_EXPAND when type is a nested struct type (Flink [[RowType]]).
    *
    * @param t flink logical type.
    * @return calcite [[RelDataType]].
    */
  def createFieldTypeFromLogicalType(t: LogicalType): RelDataType = {
    def newRelDataType(): RelDataType = t.getTypeRoot match {
      case LogicalTypeRoot.BOOLEAN => createSqlType(BOOLEAN)
      case LogicalTypeRoot.TINYINT => createSqlType(TINYINT)
      case LogicalTypeRoot.SMALLINT => createSqlType(SMALLINT)
      case LogicalTypeRoot.INTEGER => createSqlType(INTEGER)
      case LogicalTypeRoot.BIGINT => createSqlType(BIGINT)
      case LogicalTypeRoot.FLOAT => createSqlType(FLOAT)
      case LogicalTypeRoot.DOUBLE => createSqlType(DOUBLE)
      case LogicalTypeRoot.VARCHAR =>
        createSqlType(VARCHAR, t.asInstanceOf[VarCharType].getLength)

      // temporal types
      case LogicalTypeRoot.DATE => createSqlType(DATE)
      case LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE => createSqlType(TIME)

      // interval types
      case LogicalTypeRoot.INTERVAL_YEAR_MONTH =>
        createSqlIntervalType(
          new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO))
      case LogicalTypeRoot.INTERVAL_DAY_TIME =>
        createSqlIntervalType(
          new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.SECOND, SqlParserPos.ZERO))

      case LogicalTypeRoot.VARBINARY =>
        createSqlType(VARBINARY, t.asInstanceOf[VarBinaryType].getLength)

      case LogicalTypeRoot.DECIMAL =>
        val decimalType = t.asInstanceOf[DecimalType]
        createSqlType(DECIMAL, decimalType.getPrecision, decimalType.getScale)

      case LogicalTypeRoot.ROW =>
        val rowType = t.asInstanceOf[RowType]
        buildStructType(
          rowType.getFieldNames,
          rowType.getChildren,
          // fields are not expanded in "SELECT *"
          StructKind.PEEK_FIELDS_NO_EXPAND)

      case LogicalTypeRoot.ARRAY =>
        val arrayType = t.asInstanceOf[ArrayType]
        createArrayType(createFieldTypeFromLogicalType(arrayType.getElementType), -1)

      case LogicalTypeRoot.MAP =>
        val mapType = t.asInstanceOf[MapType]
        createMapType(
          createFieldTypeFromLogicalType(mapType.getKeyType),
          createFieldTypeFromLogicalType(mapType.getValueType))

      case LogicalTypeRoot.MULTISET =>
        val multisetType = t.asInstanceOf[MultisetType]
        createMultisetType(createFieldTypeFromLogicalType(multisetType.getElementType), -1)

      case LogicalTypeRoot.ANY =>
        new GenericRelDataType(
          t.asInstanceOf[TypeInformationAnyType[_]],
          true,
          getTypeSystem)

      case _@t =>
        throw new TableException(s"Type is not supported: $t")
    }

    // Kind in TimestampType do not affect the hashcode and equals, So we can't put it to seenTypes
    val relType = t.getTypeRoot match {
      case LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE =>
        val timestampType = t.asInstanceOf[TimestampType]
        timestampType.getKind match {
          case TimestampKind.PROCTIME => createProctimeIndicatorType()
          case TimestampKind.ROWTIME => createRowtimeIndicatorType()
          case TimestampKind.REGULAR => createSqlType(TIMESTAMP)
        }
      case _ =>
        seenTypes.get(t) match {
          case Some(retType: RelDataType) => retType
          case None =>
            val refType = newRelDataType()
            seenTypes.put(t, refType)
            refType
        }
    }

    createTypeWithNullability(relType, t.isNullable)
  }

  /**
    * Creates a indicator type for processing-time, but with similar properties as SQL timestamp.
    */
  def createProctimeIndicatorType(): RelDataType = {
    val originalType = createFieldTypeFromLogicalType(new TimestampType(3))
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
    val originalType = createFieldTypeFromLogicalType(new TimestampType(3))
    canonize(
      new TimeIndicatorRelDataType(
        getTypeSystem,
        originalType.asInstanceOf[BasicSqlType],
        isEventTime = true)
    )
  }

  /**
    * Creates a table row type with the input fieldNames and input fieldTypes using
    * FlinkTypeFactory. Table row type is table schema for Calcite RelNode. See getRowType of
    * [[RelNode]]. Use FULLY_QUALIFIED to let each field must be referenced explicitly.
    *
    * @param fieldNames field names
    * @param fieldTypes field types, every element is Flink's [[LogicalType]]
    * @return a table row type with the input fieldNames, input fieldTypes.
    */
  def buildRelNodeRowType(
      fieldNames: Seq[String],
      fieldTypes: Seq[LogicalType]): RelDataType = {
    buildStructType(fieldNames, fieldTypes, StructKind.FULLY_QUALIFIED)
  }

  /**
    * Creates a struct type with the input fieldNames and input fieldTypes using FlinkTypeFactory.
    *
    * @param fieldNames field names
    * @param fieldTypes field types, every element is Flink's [[LogicalType]].
    * @param structKind Name resolution policy. See more information in [[StructKind]].
    * @return a struct type with the input fieldNames, input fieldTypes.
    */
  private def buildStructType(
      fieldNames: Seq[String],
      fieldTypes: Seq[LogicalType],
      structKind: StructKind): RelDataType = {
    val b = builder
    b.kind(structKind)
    val fields = fieldNames.zip(fieldTypes)
    fields foreach {
      case (fieldName, fieldType) =>
        b.add(fieldName, createFieldTypeFromLogicalType(fieldType))
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
    // Just validate type, make sure there is a failure in validate phase.
    toLogicalType(elementType)
    super.createArrayType(elementType, maxCardinality)
  }

  override def createMapType(keyType: RelDataType, valueType: RelDataType): RelDataType = {
    // Just validate type, make sure there is a failure in validate phase.
    toLogicalType(keyType)
    toLogicalType(valueType)
    super.createMapType(keyType, valueType)
  }

  override def createMultisetType(elementType: RelDataType, maxCardinality: Long): RelDataType = {
    // Just validate type, make sure there is a failure in validate phase.
    toLogicalType(elementType)
    super.createMultisetType(elementType, maxCardinality)
  }

  override def createSqlType(typeName: SqlTypeName): RelDataType = {
    if (typeName == DECIMAL) {
      // if we got here, the precision and scale are not specified, here we
      // keep precision/scale in sync with our type system's default value,
      // see DecimalType.USER_DEFAULT.
      createSqlType(typeName, DecimalType.DEFAULT_PRECISION, DecimalType.DEFAULT_SCALE)
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
      val result = FlinkTypeSystem.inferDivisionType(
        type1.getPrecision, type1.getScale,
        type2.getPrecision, type2.getScale)
      createSqlType(SqlTypeName.DECIMAL, result.getPrecision, result.getScale)
    } else {
      null
    }
  }
}

object FlinkTypeFactory {

  def isTimeIndicatorType(t: LogicalType): Boolean = t match {
    case t: TimestampType
      if t.getKind == TimestampKind.ROWTIME || t.getKind == TimestampKind.PROCTIME => true
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

  def toLogicalType(relDataType: RelDataType): LogicalType = {
    val logicalType = relDataType.getSqlTypeName match {
      case BOOLEAN => new BooleanType()
      case TINYINT => new TinyIntType()
      case SMALLINT => new SmallIntType()
      case INTEGER => new IntType()
      case BIGINT => new BigIntType()
      case FLOAT => new FloatType()
      case DOUBLE => new DoubleType()
      case VARCHAR | CHAR =>
        // TODO we use VarCharType to support sql CHAR, VarCharType don't support 0 length
        new VarCharType(
          if (relDataType.getPrecision == 0) VarCharType.MAX_LENGTH else relDataType.getPrecision)
      case VARBINARY | BINARY =>
        // TODO we use VarBinaryType to support sql BINARY, VarBinaryType don't support 0 length
        new VarBinaryType(
          if (relDataType.getPrecision == 0) VarBinaryType.MAX_LENGTH else relDataType.getPrecision)
      case DECIMAL => new DecimalType(relDataType.getPrecision, relDataType.getScale)

      // time indicators
      case TIMESTAMP if relDataType.isInstanceOf[TimeIndicatorRelDataType] =>
        val indicator = relDataType.asInstanceOf[TimeIndicatorRelDataType]
        if (indicator.isEventTime) {
          new TimestampType(true, TimestampKind.ROWTIME, 3)
        } else {
          new TimestampType(true, TimestampKind.PROCTIME, 3)
        }

      // temporal types
      case DATE => new DateType()
      case TIME => new TimeType()
      case TIMESTAMP => new TimestampType(3)
      case typeName if YEAR_INTERVAL_TYPES.contains(typeName) =>
        DataTypes.INTERVAL(DataTypes.MONTH).getLogicalType
      case typeName if DAY_INTERVAL_TYPES.contains(typeName) =>
        DataTypes.INTERVAL(DataTypes.SECOND(3)).getLogicalType

      case NULL =>
        throw new TableException(
          "Type NULL is not supported. Null values must have a supported type.")

      // symbol for special flags e.g. TRIM's BOTH, LEADING, TRAILING
      // are represented as Enum
      case SYMBOL => new TypeInformationAnyType[Enum[_]](
        TypeExtractor.createTypeInfo(classOf[Enum[_]]))

      // extract encapsulated Type
      case ANY if relDataType.isInstanceOf[GenericRelDataType] =>
        val genericRelDataType = relDataType.asInstanceOf[GenericRelDataType]
        genericRelDataType.genericType

      case ROW if relDataType.isInstanceOf[RelRecordType] =>
        val recordType = relDataType.asInstanceOf[RelRecordType]
        RowType.of(
          recordType.getFieldList.map(_.getType).map(toLogicalType).toArray,
          recordType.getFieldNames.toSeq.toArray)

      case ROW if relDataType.isInstanceOf[RelRecordType] =>
        toLogicalRowType(relDataType.asInstanceOf[RelRecordType])

      case MULTISET => new MultisetType(toLogicalType(relDataType.getComponentType))

      case ARRAY => new ArrayType(toLogicalType(relDataType.getComponentType))

      case MAP if relDataType.isInstanceOf[MapSqlType] =>
        val mapRelDataType = relDataType.asInstanceOf[MapSqlType]
        new MapType(
          toLogicalType(mapRelDataType.getKeyType),
          toLogicalType(mapRelDataType.getValueType))

      // CURSOR for UDTF case, whose type info will never be used, just a placeholder
      case CURSOR => new TypeInformationAnyType[Nothing](new NothingTypeInfo)

      case _@t =>
        throw new TableException(s"Type is not supported: $t")
    }
    logicalType.copy(relDataType.isNullable)
  }

  def toLogicalRowType(relType: RelDataType): RowType = {
    checkArgument(relType.isStruct)
    RowType.of(
      relType.getFieldList.asScala
          .map(fieldType => toLogicalType(fieldType.getType))
          .toArray[LogicalType],
      relType.getFieldNames.asScala.toArray)
  }
}
