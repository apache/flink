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

package org.apache.flink.table.typeutils

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
import org.apache.flink.api.common.typeinfo.{AtomicType => AtomicTypeInfo, _}
import org.apache.flink.api.common.typeutils.{TypeComparator, TypeSerializer}
import org.apache.flink.api.java.typeutils.{PojoField, _}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.table.api.Types._
import org.apache.flink.table.api.types._

import scala.collection.mutable

object TypeUtils {

  def getExternalClassForType(t: DataType): Class[_] =
    TypeConverters.createExternalTypeInfoFromDataType(t).getTypeClass

  def getInternalClassForType(t: DataType): Class[_] =
    TypeConverters.createInternalTypeInfoFromDataType(t).getTypeClass

  def getPrimitiveInternalClassForType(t: DataType): Class[_] = {
    val internalType = t.toInternalType
    internalType match {
      case DataTypes.INT | _: DateType | DataTypes.TIME | DataTypes.INTERVAL_MONTHS => classOf[Int]
      case DataTypes.LONG | _: TimestampType | DataTypes.INTERVAL_MILLIS => classOf[Long]
      case DataTypes.SHORT => classOf[Short]
      case DataTypes.BYTE => classOf[Byte]
      case DataTypes.FLOAT => classOf[Float]
      case DataTypes.DOUBLE => classOf[Double]
      case DataTypes.BOOLEAN => classOf[Boolean]
      case DataTypes.CHAR => classOf[Char]
      case _ => getInternalClassForType(t)
    }
  }

  def isPrimitive(dataType: TypeInformation[_]): Boolean = {
    dataType match {
      case BOOLEAN => true
      case BYTE => true
      case SHORT => true
      case INT => true
      case LONG => true
      case FLOAT => true
      case DOUBLE => true
      case _ => false
    }
  }

  def isPrimitive(dataType: DataType): Boolean = {
    dataType match {
      case DataTypes.BOOLEAN => true
      case DataTypes.BYTE => true
      case DataTypes.SHORT => true
      case DataTypes.INT => true
      case DataTypes.LONG => true
      case DataTypes.FLOAT => true
      case DataTypes.DOUBLE => true
      case wt: TypeInfoWrappedDataType => isPrimitive(wt.getTypeInfo)
      case _ => false
    }
  }

  def isInternalCompositeType(t: TypeInformation[_]): Boolean = {
    t match {
      case _: BaseRowTypeInfo |
           _: RowTypeInfo |
           _: PojoTypeInfo[_] |
           _: TupleTypeInfo[_] |
           _: CaseClassTypeInfo[_] =>
        true
      case _ => false
    }
  }

  def isInternalArrayType(t: TypeInformation[_]): Boolean = {
    t match {
      case _: PrimitiveArrayTypeInfo[_] |
           _: BasicArrayTypeInfo[_, _] |
           _: ObjectArrayTypeInfo[_, _] if t != BYTE_PRIMITIVE_ARRAY_TYPE_INFO =>
        true
      case _ => false
    }
  }

  def getBaseArraySerializer(t: InternalType): BaseArraySerializer = {
    t match {
      case a: ArrayType => new BaseArraySerializer(a.isPrimitive, a.getElementInternalType)
    }
  }

  def getArrayElementType(t: TypeInformation[_]): TypeInformation[_] = {
    t match {
      case a: PrimitiveArrayTypeInfo[_] => a.getComponentType
      case a: BasicArrayTypeInfo[_, _] => a.getComponentInfo
      case a: ObjectArrayTypeInfo[_, _] => a.getComponentInfo
    }
  }

  def getCompositeTypes(t: TypeInformation[_]): Array[TypeInformation[_]] = {
    t match {
      case c: TupleTypeInfoBase[_] => (0 until t.getArity).map(c.getTypeAt).toArray
      case p: PojoTypeInfo[_] => (0 until p.getArity).map(p.getTypeAt).toArray
      case _ => Array(t)
    }
  }

  def flattenComparatorAndSerializer(
      arity: Int,
      keys: Array[Int],
      orders: Array[Boolean],
      types: Array[InternalType]): (Array[TypeComparator[_]], Array[TypeSerializer[_]]) = {
    val fieldComparators = new mutable.ArrayBuffer[TypeComparator[_]]()
    for (i <- keys.indices) {
      fieldComparators += createInternalComparator(types(keys(i)), orders(i))
    }
    (fieldComparators.toArray, fieldComparators.indices.map(index =>
      DataTypes.createInternalSerializer(types(keys(index)))).toArray)
  }

  def flattenComparatorAndSerializer(
      arity: Int,
      keys: Array[Int],
      orders: Array[Boolean],
      types: Array[TypeInformation[_]]): (Array[TypeComparator[_]], Array[TypeSerializer[_]]) =
    flattenComparatorAndSerializer(arity, keys, orders,
      types.map(TypeConverters.createInternalTypeFromTypeInfo))

  def createInternalComparator(t: TypeInformation[_], order: Boolean)
    : TypeComparator[_] = t match {
    case rt: BaseRowTypeInfo => new BaseRowComparator(rt.getFieldTypes, order)
    case rt: RowTypeInfo => new BaseRowComparator(rt.getFieldTypes, order)
    case pj: PojoTypeInfo[_] => new BaseRowComparator((0 until pj.getArity).map(
      pj.getPojoFieldAt).map{field: PojoField => field.getTypeInformation}.toArray, order)
    case tt: TupleTypeInfo[_] => new BaseRowComparator(
      (0 until tt.getArity).map(tt.getTypeAt).toArray, order)
    case cc: CaseClassTypeInfo[_] => new BaseRowComparator(
      (0 until cc.getArity).map(cc.getTypeAt).toArray, order)
    case STRING_TYPE_INFO => new BinaryStringComparator(order)
    case SqlTimeTypeInfo.TIMESTAMP => createInternalComparator(LONG_TYPE_INFO, order)
    case SqlTimeTypeInfo.DATE => createInternalComparator(INT_TYPE_INFO, order)
    case SqlTimeTypeInfo.TIME => createInternalComparator(INT_TYPE_INFO, order)
    case d: BigDecimalTypeInfo => new DecimalComparator(order, d.precision(), d.scale())
    case at: AtomicTypeInfo[_] => at.createComparator(order, new ExecutionConfig)
  }

  def createInternalComparator(t: InternalType, order: Boolean): TypeComparator[_] =
    createInternalComparator(TypeConverters.createExternalTypeInfoFromDataType(t), order)

  def isGeneric(tp: DataType): Boolean = tp match {
    case _: GenericType[_] => true
    case wt: TypeInfoWrappedDataType =>
      wt.getTypeInfo match {
        // TODO special to trust it.
        case _: BinaryStringTypeInfo | _: DecimalTypeInfo => false
        case _ => isGeneric(wt.toInternalType)
      }
    case _ => false
  }

  def shouldAutoCastTo(t: PrimitiveType, castTo: PrimitiveType): Boolean = {
    t match {
      case DataTypes.BYTE => castTo match {
        case DataTypes.SHORT | DataTypes.INT | DataTypes.LONG |
             DataTypes.FLOAT | DataTypes.DOUBLE | DataTypes.CHAR => true
        case _ => false
      }
      case DataTypes.SHORT => castTo match {
        case DataTypes.INT | DataTypes.LONG |
             DataTypes.FLOAT | DataTypes.DOUBLE | DataTypes.CHAR => true
        case _ => false
      }
      case DataTypes.INT => castTo match {
        case DataTypes.LONG | DataTypes.FLOAT | DataTypes.DOUBLE | DataTypes.CHAR => true
        case _ => false
      }
      case DataTypes.LONG => castTo match {
        case DataTypes.FLOAT | DataTypes.DOUBLE | DataTypes.CHAR => true
        case _ => false
      }
      case DataTypes.FLOAT => castTo == DataTypes.DOUBLE
      case _ => false
    }
  }

  def getArity(t: InternalType): Int = {
    t match {
      case rowType: RowType => rowType.getArity
      case _ => 1
    }
  }
}
