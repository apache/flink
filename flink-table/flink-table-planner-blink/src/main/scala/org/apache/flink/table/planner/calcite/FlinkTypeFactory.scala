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

package org.apache.flink.table.planner.calcite

import java.nio.charset.Charset
import java.util

import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`._
import org.apache.calcite.sql.SqlIntervalQualifier
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.`type`.{BasicSqlType, MapSqlType, SqlTypeName}
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.util.ConversionUtil
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, NothingTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.table.api.{DataTypes, TableException, TableSchema, ValidationException}
import org.apache.flink.table.calcite.ExtendedRelTypeFactory
import org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType
import org.apache.flink.table.planner.plan.schema.{GenericRelDataType, _}
import org.apache.flink.table.runtime.types.{LogicalTypeDataTypeConverter, PlannerTypeUtils}
import org.apache.flink.table.types.logical._
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
import org.apache.flink.types.Nothing
import org.apache.flink.util.Preconditions.checkArgument

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Flink specific type factory that represents the interface between Flink's [[LogicalType]]
  * and Calcite's [[RelDataType]].
  */
class FlinkTypeFactory(typeSystem: RelDataTypeSystem)
  extends JavaTypeFactoryImpl(typeSystem)
  with ExtendedRelTypeFactory {

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
      case LogicalTypeRoot.NULL => createSqlType(NULL)
      case LogicalTypeRoot.BOOLEAN => createSqlType(BOOLEAN)
      case LogicalTypeRoot.TINYINT => createSqlType(TINYINT)
      case LogicalTypeRoot.SMALLINT => createSqlType(SMALLINT)
      case LogicalTypeRoot.INTEGER => createSqlType(INTEGER)
      case LogicalTypeRoot.BIGINT => createSqlType(BIGINT)
      case LogicalTypeRoot.FLOAT => createSqlType(FLOAT)
      case LogicalTypeRoot.DOUBLE => createSqlType(DOUBLE)
      case LogicalTypeRoot.VARCHAR => createSqlType(VARCHAR, t.asInstanceOf[VarCharType].getLength)
      case LogicalTypeRoot.CHAR => createSqlType(CHAR, t.asInstanceOf[CharType].getLength)

      // temporal types
      case LogicalTypeRoot.DATE => createSqlType(DATE)
      case LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE => createSqlType(TIME)
      case LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        val lzTs = t.asInstanceOf[LocalZonedTimestampType]
        createSqlType(TIMESTAMP_WITH_LOCAL_TIME_ZONE, lzTs.getPrecision)

      // interval types
      case LogicalTypeRoot.INTERVAL_YEAR_MONTH =>
        createSqlIntervalType(
          new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO))
      case LogicalTypeRoot.INTERVAL_DAY_TIME =>
        createSqlIntervalType(
          new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.SECOND, SqlParserPos.ZERO))

      case LogicalTypeRoot.BINARY => createSqlType(BINARY, t.asInstanceOf[BinaryType].getLength)
      case LogicalTypeRoot.VARBINARY =>
        createSqlType(VARBINARY, t.asInstanceOf[VarBinaryType].getLength)

      case LogicalTypeRoot.DECIMAL =>
        t match {
          case decimalType: DecimalType =>
            createSqlType(DECIMAL, decimalType.getPrecision, decimalType.getScale)
          case legacyType: LegacyTypeInformationType[_]
              if legacyType.getTypeInformation == BasicTypeInfo.BIG_DEC_TYPE_INFO =>
            createSqlType(DECIMAL, 38, 18)
        }

      case LogicalTypeRoot.ROW =>
        val rowType = t.asInstanceOf[RowType]
        buildStructType(
          rowType.getFieldNames,
          rowType.getChildren,
          // fields are not expanded in "SELECT *"
          StructKind.PEEK_FIELDS_NO_EXPAND)

      case LogicalTypeRoot.STRUCTURED_TYPE =>
        t match {
          case structuredType: StructuredType => StructuredRelDataType.create(this, structuredType)
          case legacyTypeInformationType: LegacyTypeInformationType[_] =>
            createFieldTypeFromLogicalType(
            PlannerTypeUtils.removeLegacyTypes(legacyTypeInformationType))
        }

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

      case LogicalTypeRoot.RAW =>
        t match {
          case rawType: RawType[_] =>
            new RawRelDataType(rawType)
          case genericType: TypeInformationRawType[_] =>
            new GenericRelDataType(genericType, true, getTypeSystem)
          case legacyType: LegacyTypeInformationType[_] =>
            createFieldTypeFromLogicalType(PlannerTypeUtils.removeLegacyTypes(legacyType))
        }

      case LogicalTypeRoot.SYMBOL =>
        createSqlType(SqlTypeName.SYMBOL)

      case _@t =>
        throw new TableException(s"Type is not supported: $t")
    }

    // Kind in TimestampType do not affect the hashcode and equals, So we can't put it to seenTypes
    val relType = t.getTypeRoot match {
      case LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE =>
        val timestampType = t.asInstanceOf[TimestampType]
        timestampType.getKind match {
          case TimestampKind.PROCTIME => createProctimeIndicatorType(true)
          case TimestampKind.ROWTIME => createRowtimeIndicatorType(true)
          case TimestampKind.REGULAR => createSqlType(TIMESTAMP, timestampType.getPrecision)
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
  def createProctimeIndicatorType(isNullable: Boolean): RelDataType = {
    val originalType = createFieldTypeFromLogicalType(new TimestampType(isNullable, 3))
    canonize(new TimeIndicatorRelDataType(
      getTypeSystem,
      originalType.asInstanceOf[BasicSqlType],
      isNullable,
      isEventTime = false))
  }

  /**
    * Creates a indicator type for event-time, but with similar properties as SQL timestamp.
    */
  def createRowtimeIndicatorType(isNullable: Boolean): RelDataType = {
    val originalType = createFieldTypeFromLogicalType(new TimestampType(isNullable, 3))
    canonize(new TimeIndicatorRelDataType(
      getTypeSystem,
      originalType.asInstanceOf[BasicSqlType],
      isNullable,
      isEventTime = true))
  }

  /**
    * Creates a struct type with the input fieldNames and input fieldTypes using FlinkTypeFactory
    *
    * @param tableSchema schema to convert to Calcite's specific one
    * @return a struct type with the input fieldNames, input fieldTypes, and system fields
    */
  def buildRelNodeRowType(tableSchema: TableSchema): RelDataType = {
    buildRelNodeRowType(
      tableSchema.getFieldNames,
      tableSchema.getFieldDataTypes.map(_.getLogicalType))
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
    * Creates a table row type with the input fieldNames and input fieldTypes using
    * FlinkTypeFactory. Table row type is table schema for Calcite RelNode. See getRowType of
    * [[RelNode]]. Use FULLY_QUALIFIED to let each field must be referenced explicitly.
    */
  def buildRelNodeRowType(rowType: RowType): RelDataType = {
    val fields = rowType.getFields
    buildStructType(fields.map(_.getName), fields.map(_.getType), StructKind.FULLY_QUALIFIED)
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
        val fieldRelDataType = createFieldTypeFromLogicalType(fieldType)
        checkForNullType(fieldRelDataType)
        b.add(fieldName, fieldRelDataType)
    }
    b.build
  }

  /**
   * Returns a projected [[RelDataType]] of the structure type.
   */
  def projectStructType(relType: RelDataType, selectedFields: Array[Int]): RelDataType = {
    this.createStructType(
        selectedFields
          .map(idx => relType.getFieldList.get(idx))
          .toList
          .asJava)
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
    checkForNullType(elementType)
    toLogicalType(elementType)
    super.createArrayType(elementType, maxCardinality)
  }

  override def createMapType(keyType: RelDataType, valueType: RelDataType): RelDataType = {
    // Just validate type, make sure there is a failure in validate phase.
    checkForNullType(keyType, valueType)
    toLogicalType(keyType)
    toLogicalType(valueType)
    super.createMapType(keyType, valueType)
  }

  override def createMultisetType(elementType: RelDataType, maxCardinality: Long): RelDataType = {
    // Just validate type, make sure there is a failure in validate phase.
    checkForNullType(elementType)
    toLogicalType(elementType)
    super.createMultisetType(elementType, maxCardinality)
  }

  override def createRawType(className: String, serializerString: String): RelDataType = {
    val rawType = RawType.restore(
      FlinkTypeFactory.getClass.getClassLoader, // temporary solution until FLINK-15635 is fixed
      className,
      serializerString)
    val rawRelDataType = createFieldTypeFromLogicalType(rawType)
    canonize(rawRelDataType)
  }

  override def createSqlType(typeName: SqlTypeName): RelDataType = {
    if (typeName == DECIMAL) {
      // if we got here, the precision and scale are not specified, here we
      // keep precision/scale in sync with our type system's default value,
      // see DecimalType.USER_DEFAULT.
      createSqlType(typeName, DecimalType.DEFAULT_PRECISION, DecimalType.DEFAULT_SCALE)
    } else if (typeName == COLUMN_LIST) {
      // we don't support column lists and translate them into the unknown type,
      // this makes it possible to ignore them in the validator and fall back to regular row types
      // see also SqlFunction#deriveType
      createUnknownType()
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
      case raw: RawRelDataType =>
        raw.createWithNullability(isNullable)

      case structured: StructuredRelDataType =>
        structured.createWithNullability(isNullable)

      case generic: GenericRelDataType =>
        new GenericRelDataType(generic.genericType, isNullable, typeSystem)

      case it: TimeIndicatorRelDataType =>
        new TimeIndicatorRelDataType(it.typeSystem, it.originalType, isNullable, it.isEventTime)

      // for nested rows we keep the nullability property,
      // top-level rows fall back to Calcite's default handling
      case rt: RelRecordType if rt.getStructKind == StructKind.PEEK_FIELDS_NO_EXPAND =>
        new RelRecordType(rt.getStructKind, rt.getFieldList, isNullable);

      case _ =>
        super.createTypeWithNullability(relDataType, isNullable)
    }

    canonize(newType)
  }

  override def leastRestrictive(types: util.List[RelDataType]): RelDataType = {
    val leastRestrictive = resolveAllIdenticalTypes(types)
      .getOrElse(super.leastRestrictive(types))
    // NULL is reserved for untyped literals only
    if (leastRestrictive == null || leastRestrictive.getSqlTypeName == NULL) {
      null
    } else {
      leastRestrictive
    }
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

  /**
   * This is a safety check in case the null type ends up in the type factory for other use cases
   * than untyped NULL literals.
   */
  private def checkForNullType(childTypes: RelDataType*): Unit = {
    childTypes.foreach { t =>
      if (t.getSqlTypeName == NULL) {
        throw new ValidationException(
          "The null type is reserved for representing untyped NULL literals. It should not be " +
            "used in constructed types. Please cast NULL literals to a more explicit type.")
      }
    }
  }
}

object FlinkTypeFactory {
  val INSTANCE = new FlinkTypeFactory(new FlinkTypeSystem)

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

  @Deprecated
  def isProctimeIndicatorType(typeInfo: TypeInformation[_]): Boolean = typeInfo match {
    case ti: TimeIndicatorTypeInfo if !ti.isEventTime => true
    case _ => false
  }

  @Deprecated
  def isRowtimeIndicatorType(typeInfo: TypeInformation[_]): Boolean = typeInfo match {
    case ti: TimeIndicatorTypeInfo if ti.isEventTime => true
    case _ => false
  }

  @Deprecated
  def isTimeIndicatorType(typeInfo: TypeInformation[_]): Boolean = typeInfo match {
    case ti: TimeIndicatorTypeInfo => true
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
      case CHAR =>
        if (relDataType.getPrecision == 0) {
          CharType.ofEmptyLiteral
        } else {
          new CharType(relDataType.getPrecision)
        }
      case VARCHAR =>
        if (relDataType.getPrecision == 0) {
          VarCharType.ofEmptyLiteral
        } else {
          new VarCharType(relDataType.getPrecision)
        }
      case BINARY =>
        if (relDataType.getPrecision == 0) {
          BinaryType.ofEmptyLiteral
        } else {
          new BinaryType(relDataType.getPrecision)
        }
      case VARBINARY =>
        if (relDataType.getPrecision == 0) {
          VarBinaryType.ofEmptyLiteral
        } else {
          new VarBinaryType(relDataType.getPrecision)
        }
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
      case TIME =>
        if (relDataType.getPrecision > 3) {
          throw new TableException(
            s"TIME precision is not supported: ${relDataType.getPrecision}")
        }
        // blink runner support precision 3, but for consistent with flink runner, we set to 0.
        new TimeType()
      case TIMESTAMP =>
        new TimestampType(relDataType.getPrecision)
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        new LocalZonedTimestampType(relDataType.getPrecision)
      case typeName if YEAR_INTERVAL_TYPES.contains(typeName) =>
        DataTypes.INTERVAL(DataTypes.MONTH).getLogicalType
      case typeName if DAY_INTERVAL_TYPES.contains(typeName) =>
        if (relDataType.getPrecision > 3) {
          throw new TableException(
            s"DAY_INTERVAL_TYPES precision is not supported: ${relDataType.getPrecision}")
        }
        DataTypes.INTERVAL(DataTypes.SECOND(3)).getLogicalType

      case NULL =>
        new NullType()

      // symbol for special flags e.g. TRIM's BOTH, LEADING, TRAILING
      // are represented as Enum
      case SYMBOL => new TypeInformationRawType[Enum[_]](
        TypeExtractor.createTypeInfo(classOf[Enum[_]]))

      // extract encapsulated Type
      case ANY if relDataType.isInstanceOf[GenericRelDataType] =>
        val genericRelDataType = relDataType.asInstanceOf[GenericRelDataType]
        genericRelDataType.genericType

      case ROW if relDataType.isInstanceOf[RelRecordType] =>
        toLogicalRowType(relDataType)

      case STRUCTURED if relDataType.isInstanceOf[StructuredRelDataType] =>
        relDataType.asInstanceOf[StructuredRelDataType].getStructuredType

      case MULTISET => new MultisetType(toLogicalType(relDataType.getComponentType))

      case ARRAY => new ArrayType(toLogicalType(relDataType.getComponentType))

      case MAP if relDataType.isInstanceOf[MapSqlType] =>
        val mapRelDataType = relDataType.asInstanceOf[MapSqlType]
        new MapType(
          toLogicalType(mapRelDataType.getKeyType),
          toLogicalType(mapRelDataType.getValueType))

      // CURSOR for UDTF case, whose type info will never be used, just a placeholder
      case CURSOR => new TypeInformationRawType[Nothing](new NothingTypeInfo)

      case OTHER if relDataType.isInstanceOf[RawRelDataType] =>
        relDataType.asInstanceOf[RawRelDataType].getRawType

      case _@t =>
        throw new TableException(s"Type is not supported: $t")
    }
    logicalType.copy(relDataType.isNullable)
  }

  def toTableSchema(relDataType: RelDataType): TableSchema = {
    val fieldNames = relDataType.getFieldNames.toArray(new Array[String](0))
    val fieldTypes = relDataType.getFieldList
      .asScala
      .map(field =>
        LogicalTypeDataTypeConverter.fromLogicalTypeToDataType(
          FlinkTypeFactory.toLogicalType(field.getType))
      ).toArray
    TableSchema.builder.fields(fieldNames, fieldTypes).build
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
