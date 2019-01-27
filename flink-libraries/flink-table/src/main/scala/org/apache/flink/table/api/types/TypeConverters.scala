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

package org.apache.flink.table.api.types

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{BOOLEAN_TYPE_INFO, BYTE_TYPE_INFO, CHAR_TYPE_INFO, DOUBLE_TYPE_INFO, FLOAT_TYPE_INFO, INT_TYPE_INFO, LONG_TYPE_INFO, SHORT_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, BasicTypeInfo, BigDecimalTypeInfo, PrimitiveArrayTypeInfo, SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.{MapTypeInfo, MultisetTypeInfo, ObjectArrayTypeInfo, PojoField, PojoTypeInfo, RowTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.table.api.TableException
import org.apache.flink.table.typeutils.{BaseArrayTypeInfo, BaseMapTypeInfo, BaseRowTypeInfo, BinaryStringTypeInfo, DecimalTypeInfo, RowIntervalTypeInfo, TimeIndicatorTypeInfo, TimeIntervalTypeInfo}

/**
  * Type Converters:
  * [[InternalType]] <=> [[TypeInformation]].
  * [[DataType]] <=> [[TypeInformation]].
  */
object TypeConverters {

  /**
    * Create a [[InternalType]] from a [[TypeInformation]].
    * If you want to convert to [[DataType]], just use [[TypeInfoWrappedDataType]] to wrap it.
    *
    * <p>Note: Information may be lost. For example, after Pojo is converted to InternalType,
    * we no longer know that it is a Pojo and only think it is a Row.
    *
    * <p>Eg:
    * [[BasicTypeInfo#STRING_TYPE_INFO]] => [[DataTypes.STRING]].
    * [[BasicTypeInfo#BIG_DEC_TYPE_INFO]] => [[DecimalType]].
    * [[RowTypeInfo]] => [[RowType]].
    * [[PojoTypeInfo]] (CompositeType) => [[RowType]].
    * [[TupleTypeInfo]] (CompositeType) => [[RowType]].
    */
  def createInternalTypeFromTypeInfo(typeInfo: TypeInformation[_])
  : InternalType = typeInfo match {
    // built-in composite type info. (Need to be converted to RowType)
    case rt: RowTypeInfo =>
      new RowType(
        rt.getFieldTypes.map(new TypeInfoWrappedDataType(_)).toArray[DataType],
        rt.getFieldNames)

    case tt: TupleTypeInfo[_] =>
      new RowType(
        (0 until tt.getArity).map(tt.getTypeAt)
            .map(new TypeInfoWrappedDataType(_))
            .toArray[DataType],
        tt.getFieldNames)

    case pt: PojoTypeInfo[_] =>
      val fields = (0 until pt.getArity).map(pt.getPojoFieldAt)
      new RowType(
        fields.map{(field: PojoField) =>
          new TypeInfoWrappedDataType(field.getTypeInformation)}.toArray[DataType],
        fields.map{(field: PojoField) => field.getField.getName}.toArray)

    case cs: CaseClassTypeInfo[_] => new RowType(
      (0 until cs.getArity).map(cs.getTypeAt).map(new TypeInfoWrappedDataType(_)).toArray[DataType],
      cs.fieldNames.toArray)

    //primitive types
    case BOOLEAN_TYPE_INFO => DataTypes.BOOLEAN
    case BYTE_TYPE_INFO => DataTypes.BYTE
    case SHORT_TYPE_INFO => DataTypes.SHORT
    case INT_TYPE_INFO => DataTypes.INT
    case LONG_TYPE_INFO => DataTypes.LONG
    case FLOAT_TYPE_INFO => DataTypes.FLOAT
    case DOUBLE_TYPE_INFO => DataTypes.DOUBLE
    case CHAR_TYPE_INFO => DataTypes.CHAR

    case STRING_TYPE_INFO | BinaryStringTypeInfo.INSTANCE => DataTypes.STRING
    case dt: BigDecimalTypeInfo => DataTypes.createDecimalType(dt.precision, dt.scale)
    case dt: DecimalTypeInfo => DataTypes.createDecimalType(dt.precision, dt.scale)
    case BYTE_PRIMITIVE_ARRAY_TYPE_INFO => DataTypes.BYTE_ARRAY

    // temporal types
    case TimeIntervalTypeInfo.INTERVAL_MONTHS => DataTypes.INTERVAL_MONTHS
    case TimeIntervalTypeInfo.INTERVAL_MILLIS => DataTypes.INTERVAL_MILLIS
    case RowIntervalTypeInfo.INTERVAL_ROWS => DataTypes.INTERVAL_ROWS

    // time indicators
    case SqlTimeTypeInfo.TIMESTAMP if typeInfo.isInstanceOf[TimeIndicatorTypeInfo] =>
      val indicator = typeInfo.asInstanceOf[TimeIndicatorTypeInfo]
      if (indicator.isEventTime) {
        DataTypes.ROWTIME_INDICATOR
      } else {
        DataTypes.PROCTIME_INDICATOR
      }

    case SqlTimeTypeInfo.DATE => DataTypes.DATE
    case SqlTimeTypeInfo.TIME => DataTypes.TIME
    case SqlTimeTypeInfo.TIMESTAMP => DataTypes.TIMESTAMP

    // arrays and map types
    case pa: PrimitiveArrayTypeInfo[_] =>
      DataTypes.createPrimitiveArrayType(new TypeInfoWrappedDataType(pa.getComponentType))

    case pa: PrimitiveArrayTypeInfo[_] =>
      DataTypes.createPrimitiveArrayType(new TypeInfoWrappedDataType(pa.getComponentType))

    case ba: BasicArrayTypeInfo[_, _] =>
      DataTypes.createArrayType(new TypeInfoWrappedDataType(ba.getComponentInfo))

    case oa: ObjectArrayTypeInfo[_, _] =>
      DataTypes.createArrayType(new TypeInfoWrappedDataType(oa.getComponentInfo))

    case pa: BaseArrayTypeInfo =>
      new ArrayType(pa.getEleType, pa.isPrimitive)

    case mp: MultisetTypeInfo[_] =>
      DataTypes.createMultisetType(new TypeInfoWrappedDataType(mp.getElementTypeInfo))

    case mp: MapTypeInfo[_, _] =>
      DataTypes.createMapType(
        new TypeInfoWrappedDataType(mp.getKeyTypeInfo),
        new TypeInfoWrappedDataType(mp.getValueTypeInfo))

    case mp: BaseMapTypeInfo =>
      DataTypes.createMapType(mp.getKeyType, mp.getValueType)

    case br: BaseRowTypeInfo =>
      new RowType(
        br.getFieldTypes.map(new TypeInfoWrappedDataType(_)).toArray[DataType],
        br.getFieldNames)

    // unknown type info, treat as generic.
    case _ => DataTypes.createGenericType(typeInfo)
  }

  /**
    * Create a TypeInformation from DataType.
    *
    * <p>eg:
    * [[DataTypes.STRING]] => [[BasicTypeInfo.STRING_TYPE_INFO]].
    * [[RowType]] => [[RowTypeInfo]].
    */
  def createExternalTypeInfoFromDataType(t: DataType): TypeInformation[_] = {
    if (t == null) {
      return null
    }
    t match {
      //primitive types
      case DataTypes.BOOLEAN => BOOLEAN_TYPE_INFO
      case DataTypes.BYTE => BYTE_TYPE_INFO
      case DataTypes.SHORT => SHORT_TYPE_INFO
      case DataTypes.INT => INT_TYPE_INFO
      case DataTypes.LONG =>  LONG_TYPE_INFO
      case DataTypes.FLOAT => FLOAT_TYPE_INFO
      case DataTypes.DOUBLE => DOUBLE_TYPE_INFO
      case DataTypes.CHAR => CHAR_TYPE_INFO

      case _: StringType => STRING_TYPE_INFO
      case dt: DecimalType => BigDecimalTypeInfo.of(dt.precision, dt.scale);
      case DataTypes.BYTE_ARRAY => BYTE_PRIMITIVE_ARRAY_TYPE_INFO

      // temporal types
      case DataTypes.INTERVAL_MONTHS => TimeIntervalTypeInfo.INTERVAL_MONTHS
      case DataTypes.INTERVAL_MILLIS => TimeIntervalTypeInfo.INTERVAL_MILLIS
      case DataTypes.ROWTIME_INDICATOR => TimeIndicatorTypeInfo.ROWTIME_INDICATOR
      case DataTypes.PROCTIME_INDICATOR => TimeIndicatorTypeInfo.PROCTIME_INDICATOR
      case DataTypes.INTERVAL_ROWS => RowIntervalTypeInfo.INTERVAL_ROWS

      case DataTypes.DATE => SqlTimeTypeInfo.DATE
      case DataTypes.TIME => SqlTimeTypeInfo.TIME
      case DataTypes.TIMESTAMP => SqlTimeTypeInfo.TIMESTAMP

      // arrays and map types
      case at: ArrayType if at.isPrimitive => at.getElementInternalType match {
        case DataTypes.BOOLEAN => PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO
        case DataTypes.SHORT => PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO
        case DataTypes.INT => PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO
        case DataTypes.LONG =>  PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO
        case DataTypes.FLOAT => PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO
        case DataTypes.DOUBLE => PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO
        case DataTypes.CHAR => PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO
      }

      case at: ArrayType => at.getElementInternalType match {
        case DataTypes.BOOLEAN => BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO
        case DataTypes.SHORT => BasicArrayTypeInfo.SHORT_ARRAY_TYPE_INFO
        case DataTypes.INT => BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO
        case DataTypes.LONG =>  BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO
        case DataTypes.FLOAT => BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO
        case DataTypes.DOUBLE => BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO
        case DataTypes.CHAR => BasicArrayTypeInfo.CHAR_ARRAY_TYPE_INFO
        case DataTypes.STRING => BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO

        // object
        case _ => ObjectArrayTypeInfo.getInfoFor(
          createExternalTypeInfoFromDataType(at.getElementType))
      }

      case mp: MultisetType => new MultisetTypeInfo(
        createExternalTypeInfoFromDataType(mp.getKeyType))

      case mp: MapType =>
        new MapTypeInfo(
          createExternalTypeInfoFromDataType(mp.getKeyType),
          createExternalTypeInfoFromDataType(mp.getValueType))

      // composite types
      case br: RowType =>
        new RowTypeInfo(br.getFieldTypes.map(
          createExternalTypeInfoFromDataType), br.getFieldNames)

      case gt: GenericType[_] => gt.getTypeInfo

      case et: TypeInfoWrappedDataType => et.getTypeInfo

      case _ =>
        throw new TableException(s"Type is not supported: $t")
    }
  }

  /**
    * Create a internal [[TypeInformation]] from a [[DataType]].
    *
    * <p>eg:
    * [[DataTypes.STRING]] => [[BinaryStringTypeInfo]].
    * [[RowType]] => [[BaseRowTypeInfo]].
    */
  def createInternalTypeInfoFromDataType(t: DataType): TypeInformation[_] = {
    if (t == null) {
      return null
    }
    t match {
      //primitive types
      case DataTypes.BOOLEAN => BOOLEAN_TYPE_INFO
      case DataTypes.BYTE => BYTE_TYPE_INFO
      case DataTypes.SHORT => SHORT_TYPE_INFO
      case DataTypes.INT => INT_TYPE_INFO
      case DataTypes.LONG =>  LONG_TYPE_INFO
      case DataTypes.FLOAT => FLOAT_TYPE_INFO
      case DataTypes.DOUBLE => DOUBLE_TYPE_INFO
      case DataTypes.CHAR => CHAR_TYPE_INFO

      case _: StringType => BinaryStringTypeInfo.INSTANCE
      case dt: DecimalType => DecimalTypeInfo.of(dt.precision, dt.scale);
      case DataTypes.BYTE_ARRAY => BYTE_PRIMITIVE_ARRAY_TYPE_INFO

      // temporal types
      case DataTypes.INTERVAL_MONTHS => TimeIntervalTypeInfo.INTERVAL_MONTHS
      case DataTypes.INTERVAL_MILLIS => TimeIntervalTypeInfo.INTERVAL_MILLIS
      case DataTypes.ROWTIME_INDICATOR => TimeIndicatorTypeInfo.ROWTIME_INDICATOR
      case DataTypes.PROCTIME_INDICATOR => TimeIndicatorTypeInfo.PROCTIME_INDICATOR
      case DataTypes.INTERVAL_ROWS => RowIntervalTypeInfo.INTERVAL_ROWS

      case DataTypes.DATE => INT_TYPE_INFO
      case DataTypes.TIME => INT_TYPE_INFO
      case DataTypes.TIMESTAMP => LONG_TYPE_INFO

      // arrays and map types
      case at: ArrayType => new BaseArrayTypeInfo(at.isPrimitive, at.getElementType)
      case mp: MapType => new BaseMapTypeInfo(mp.getKeyType, mp.getValueType)

      // composite types
      case br: RowType => new BaseRowTypeInfo(
        br.getFieldInternalTypes.map(createExternalTypeInfoFromDataType), br.getFieldNames)

      case gt: GenericType[_] => gt.getTypeInfo

      case et: TypeInfoWrappedDataType => createInternalTypeInfoFromDataType(et.toInternalType)

      case _ =>
        throw new TableException(s"Type is not supported: $t")
    }
  }

  def createExternalTypeInfoFromDataTypes(types: Array[DataType]): Array[TypeInformation[_]] =
    types.map(createExternalTypeInfoFromDataType)

  def toBaseRowTypeInfo(t: RowType): BaseRowTypeInfo = {
    new BaseRowTypeInfo(
      t.getFieldInternalTypes.map(createExternalTypeInfoFromDataType),
      t.getFieldNames)
  }
}
