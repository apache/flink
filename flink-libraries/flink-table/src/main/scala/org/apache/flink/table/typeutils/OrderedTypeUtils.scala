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

import org.apache.flink.api.common.functions.Comparator
import org.apache.flink.api.common.typeinfo.{TypeInformation, _}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
import org.apache.flink.table.api.dataview.Order
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.types.{DataTypes, _}
import org.apache.flink.table.typeutils.ordered.OrderedBasicTypeInfo._
import org.apache.flink.table.typeutils.ordered.{OrderedBigDecTypeInfo, OrderedDecTypeInfo}
import org.apache.flink.table.typeutils.ordered.OrderedPrimitiveArrayTypeInfo._
import org.apache.flink.table.typeutils.ordered.OrderedSqlTimeTypeInfo._

import SortedMapViewTypeInfo.{ByteArrayComparator, ComparableComparator}

object OrderedTypeUtils {

  def createComparatorFromDataType(t: DataType, ord: Order): Comparator[_] = {
    t match {
      case DataTypes.BOOLEAN | DataTypes.BYTE | DataTypes.SHORT | DataTypes.INT |
           DataTypes.LONG | DataTypes.FLOAT | DataTypes.DOUBLE | DataTypes.CHAR |
           DataTypes.STRING | DataTypes.DATE | DataTypes.TIME | DataTypes.TIMESTAMP |
           DataTypes.INTERVAL_MONTHS | DataTypes.INTERVAL_MILLIS | DataTypes.ROWTIME_INDICATOR |
           DataTypes.PROCTIME_INDICATOR | _: DecimalType =>
        new ComparableComparator[Any](ord == Order.ASCENDING)
      case t: TypeInfoWrappedDataType if classOf[Comparable[_]]
          .isAssignableFrom(t.getTypeInfo.getTypeClass) =>
        new ComparableComparator[Any](ord == Order.ASCENDING)

      case DataTypes.BYTE_ARRAY => new ByteArrayComparator(ord == Order.ASCENDING)

      case _ =>
        throw new TableException(s"Type is not supported as the sort key: $t")
    }
  }

  /**
    * Create a TypeInformation from DataType for binary sortable type that is
    * the serialization format maintain the sort order of the original values.
    */
  def createOrderedTypeInfoFromDataType(
      t: DataType,
      ord: Order): TypeInformation[_] = {
    if (ord == Order.ASCENDING) {
      t match {
        //primitive types
        case DataTypes.BOOLEAN => ASC_BOOLEAN_TYPE_INFO
        case DataTypes.BYTE => ASC_BYTE_TYPE_INFO
        case DataTypes.SHORT => ASC_SHORT_TYPE_INFO
        case DataTypes.INT => ASC_INT_TYPE_INFO
        case DataTypes.LONG => ASC_LONG_TYPE_INFO
        case DataTypes.FLOAT => ASC_FLOAT_TYPE_INFO
        case DataTypes.DOUBLE => ASC_DOUBLE_TYPE_INFO
        case DataTypes.CHAR => ASC_CHAR_TYPE_INFO

        case DataTypes.STRING => ASC_STRING_TYPE_INFO
        case DataTypes.BYTE_ARRAY => ASC_BYTE_PRIMITIVE_ARRAY_TYPE_INFO

        // temporal types
        case DataTypes.INTERVAL_MONTHS => ASC_INT_TYPE_INFO
        case DataTypes.INTERVAL_MILLIS => ASC_LONG_TYPE_INFO
        case DataTypes.ROWTIME_INDICATOR => ASC_TIMESTAMP
        case DataTypes.PROCTIME_INDICATOR => ASC_TIMESTAMP

        case DataTypes.DATE => ASC_DATE
        case DataTypes.TIME => ASC_TIME
        case DataTypes.TIMESTAMP => ASC_TIMESTAMP

        case dt: DecimalType => OrderedBigDecTypeInfo.of(dt.precision, dt.scale, true);
        case et: TypeInfoWrappedDataType => createOrderedTypeInfoFromTypeInfo(et.getTypeInfo, ord)

        case _ =>
          throw new TableException(s"Type is not supported as the sort key: $t")
      }
    } else {
      t match {
        //primitive types
        case DataTypes.BOOLEAN => DESC_BOOLEAN_TYPE_INFO
        case DataTypes.BYTE => DESC_BYTE_TYPE_INFO
        case DataTypes.SHORT => DESC_SHORT_TYPE_INFO
        case DataTypes.INT => DESC_INT_TYPE_INFO
        case DataTypes.LONG => DESC_LONG_TYPE_INFO
        case DataTypes.FLOAT => DESC_FLOAT_TYPE_INFO
        case DataTypes.DOUBLE => DESC_DOUBLE_TYPE_INFO
        case DataTypes.CHAR => DESC_CHAR_TYPE_INFO

        case DataTypes.STRING => DESC_STRING_TYPE_INFO
        case DataTypes.BYTE_ARRAY => DESC_BYTE_PRIMITIVE_ARRAY_TYPE_INFO

        // temporal types
        case DataTypes.INTERVAL_MONTHS => DESC_INT_TYPE_INFO
        case DataTypes.INTERVAL_MILLIS => DESC_LONG_TYPE_INFO
        case DataTypes.ROWTIME_INDICATOR => DESC_TIMESTAMP
        case DataTypes.PROCTIME_INDICATOR => DESC_TIMESTAMP

        case DataTypes.DATE => DESC_DATE
        case DataTypes.TIME => DESC_TIME
        case DataTypes.TIMESTAMP => DESC_TIMESTAMP

        case dt: DecimalType => OrderedBigDecTypeInfo.of(dt.precision, dt.scale, false);
        case et: TypeInfoWrappedDataType => createOrderedTypeInfoFromTypeInfo(et.getTypeInfo, ord)

        case _ =>
          throw new TableException(s"Type is not supported as the sort key: $t")
      }
    }
  }

  private def createOrderedTypeInfoFromTypeInfo(
      typeInfo: TypeInformation[_],
      ord: Order): TypeInformation[_] = {
    if (ord == Order.ASCENDING) {
      typeInfo match {
        case BOOLEAN_TYPE_INFO => ASC_BOOLEAN_TYPE_INFO
        case BYTE_TYPE_INFO => ASC_BYTE_TYPE_INFO
        case SHORT_TYPE_INFO => ASC_SHORT_TYPE_INFO
        case INT_TYPE_INFO => ASC_INT_TYPE_INFO
        case LONG_TYPE_INFO => ASC_LONG_TYPE_INFO
        case FLOAT_TYPE_INFO => ASC_FLOAT_TYPE_INFO
        case DOUBLE_TYPE_INFO => ASC_DOUBLE_TYPE_INFO
        case CHAR_TYPE_INFO => ASC_CHAR_TYPE_INFO

        case STRING_TYPE_INFO => ASC_STRING_TYPE_INFO
        case _: BinaryStringTypeInfo => ASC_BINARY_STRING_TYPE_INFO
        case BYTE_PRIMITIVE_ARRAY_TYPE_INFO => ASC_BYTE_PRIMITIVE_ARRAY_TYPE_INFO

        // temporal types
        case TimeIntervalTypeInfo.INTERVAL_MONTHS => ASC_INT_TYPE_INFO
        case TimeIntervalTypeInfo.INTERVAL_MILLIS => ASC_LONG_TYPE_INFO
        case TimeIndicatorTypeInfo.ROWTIME_INDICATOR => ASC_TIMESTAMP
        case TimeIndicatorTypeInfo.PROCTIME_INDICATOR => ASC_TIMESTAMP

        case SqlTimeTypeInfo.DATE => ASC_DATE
        case SqlTimeTypeInfo.TIME => ASC_TIME
        case SqlTimeTypeInfo.TIMESTAMP => ASC_TIMESTAMP

        case dt: DecimalTypeInfo => OrderedDecTypeInfo.of(dt.precision, dt.scale, true);
        case BIG_INT_TYPE_INFO => ASC_BIG_INT_TYPE_INFO

        case _ =>
          throw new TableException(s"Type is not supported as the sort key: $typeInfo")
      }
    } else {
      typeInfo match {
        case BOOLEAN_TYPE_INFO => DESC_BOOLEAN_TYPE_INFO
        case BYTE_TYPE_INFO => DESC_BYTE_TYPE_INFO
        case SHORT_TYPE_INFO => DESC_SHORT_TYPE_INFO
        case INT_TYPE_INFO => DESC_INT_TYPE_INFO
        case LONG_TYPE_INFO => DESC_LONG_TYPE_INFO
        case FLOAT_TYPE_INFO => DESC_FLOAT_TYPE_INFO
        case DOUBLE_TYPE_INFO => DESC_DOUBLE_TYPE_INFO
        case CHAR_TYPE_INFO => DESC_CHAR_TYPE_INFO

        case STRING_TYPE_INFO => DESC_STRING_TYPE_INFO
        case _: BinaryStringTypeInfo => DESC_BINARY_STRING_TYPE_INFO
        case BYTE_PRIMITIVE_ARRAY_TYPE_INFO => DESC_BYTE_PRIMITIVE_ARRAY_TYPE_INFO

        // temporal types
        case TimeIntervalTypeInfo.INTERVAL_MONTHS => DESC_INT_TYPE_INFO
        case TimeIntervalTypeInfo.INTERVAL_MILLIS => DESC_LONG_TYPE_INFO
        case TimeIndicatorTypeInfo.ROWTIME_INDICATOR => DESC_TIMESTAMP
        case TimeIndicatorTypeInfo.PROCTIME_INDICATOR => DESC_TIMESTAMP

        case SqlTimeTypeInfo.DATE => DESC_DATE
        case SqlTimeTypeInfo.TIME => DESC_TIME
        case SqlTimeTypeInfo.TIMESTAMP => DESC_TIMESTAMP

        case dt: DecimalTypeInfo => OrderedDecTypeInfo.of(dt.precision, dt.scale, false);
        case BIG_INT_TYPE_INFO => DESC_BIG_INT_TYPE_INFO

        case _ =>
          throw new TableException(s"Type is not supported as the sort key: $typeInfo")
      }
    }
  }
}
