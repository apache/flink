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

package org.apache.flink.table.plan.schema

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.Statistic
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.flink.api.common.typeinfo.{AtomicType, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo

abstract class FlinkTable[T](
    val typeInfo: TypeInformation[T],
    val fieldIndexes: Array[Int],
    val fieldNames: Array[String],
    val statistic: FlinkStatistic)
  extends AbstractTable {

  if (fieldIndexes.length != fieldNames.length) {
    throw new TableException(
      s"Number of field names and field indexes must be equal.\n" +
        s"Number of names is ${fieldNames.length}, number of indexes is ${fieldIndexes.length}.\n" +
        s"List of column names: ${fieldNames.mkString("[", ", ", "]")}.\n" +
        s"List of column indexes: ${fieldIndexes.mkString("[", ", ", "]")}.")
  }

  // check uniqueness of field names
  if (fieldNames.length != fieldNames.toSet.size) {
    val duplicateFields = fieldNames
      // count occurences of field names
      .groupBy(identity).mapValues(_.length)
      // filter for occurences > 1 and map to field name
      .filter(g => g._2 > 1).keys

    throw new TableException(
      s"Field names must be unique.\n" +
        s"List of duplicate fields: ${duplicateFields.mkString("[", ", ", "]")}.\n" +
        s"List of all fields: ${fieldNames.mkString("[", ", ", "]")}.")
  }

  val fieldTypes: Array[TypeInformation[_]] =
    typeInfo match {
      case cType: CompositeType[_] =>
        // it is ok to leave out fields
        if (fieldIndexes.count(_ >= 0) > cType.getArity) {
          throw new TableException(
          s"Arity of type (" + cType.getFieldNames.deep + ") " +
            "must not be greater than number of field names " + fieldNames.deep + ".")
        }
        fieldIndexes.map {
          case TimeIndicatorTypeInfo.ROWTIME_MARKER => TimeIndicatorTypeInfo.ROWTIME_INDICATOR
          case TimeIndicatorTypeInfo.PROCTIME_MARKER => TimeIndicatorTypeInfo.PROCTIME_INDICATOR
          case i => cType.getTypeAt(i).asInstanceOf[TypeInformation[_]]}
      case aType: AtomicType[_] =>
        if (fieldIndexes.length != 1 || fieldIndexes(0) != 0) {
          throw new TableException(
            "Non-composite input type may have only a single field and its index must be 0.")
        }
        Array(aType)
    }

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    val flinkTypeFactory = typeFactory.asInstanceOf[FlinkTypeFactory]
    flinkTypeFactory.buildLogicalRowType(fieldNames, fieldTypes)
  }

  /**
    * Returns statistics of current table
    *
    * @return statistics of current table
    */
  override def getStatistic: Statistic = statistic

}
