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

import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging
import org.apache.flink.table.types.logical.{DecimalType, LocalZonedTimestampType, TimestampType}

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, RelDataTypeFactoryImpl, RelDataTypeSystemImpl}
import org.apache.calcite.sql.`type`.{SqlTypeName, SqlTypeUtil}

/**
  * Custom type system for Flink.
  */
class FlinkTypeSystem extends RelDataTypeSystemImpl {

  // set the maximum precision of a NUMERIC or DECIMAL type to DecimalType.MAX_PRECISION.
  override def getMaxNumericPrecision: Int = DecimalType.MAX_PRECISION

  // the max scale can't be greater than precision
  override def getMaxNumericScale: Int = DecimalType.MAX_PRECISION

  override def getDefaultPrecision(typeName: SqlTypeName): Int = typeName match {

    // Calcite will limit the length of the VARCHAR field to 65536
    case SqlTypeName.VARCHAR | SqlTypeName.VARBINARY =>
      Int.MaxValue

    // by default we support timestamp with microseconds precision (Timestamp(6))
    case SqlTypeName.TIMESTAMP =>
      TimestampType.DEFAULT_PRECISION

    // by default we support timestamp with local time zone with microseconds precision
    // Timestamp(6) with local time zone
    case SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
      LocalZonedTimestampType.DEFAULT_PRECISION

    case _ =>
      super.getDefaultPrecision(typeName)
  }

  override def getMaxPrecision(typeName: SqlTypeName): Int = typeName match {
    case SqlTypeName.VARCHAR | SqlTypeName.CHAR | SqlTypeName.VARBINARY | SqlTypeName.BINARY =>
      Int.MaxValue

    // The maximum precision of TIMESTAMP is 3 in Calcite,
    // change it to 9 to support nanoseconds precision
    case SqlTypeName.TIMESTAMP => TimestampType.MAX_PRECISION

    // The maximum precision of TIMESTAMP_WITH_LOCAL_TIME_ZONE is 3 in Calcite,
    // change it to 9 to support nanoseconds precision
    case SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE => LocalZonedTimestampType.MAX_PRECISION

    case _ =>
      super.getMaxPrecision(typeName)
  }

  // when union a number of CHAR types of different lengths, we should cast to a VARCHAR
  // this fixes the problem of CASE WHEN with different length string literals but get wrong
  // result with additional space suffix
  override def shouldConvertRaggedUnionTypesToVarying(): Boolean = true

  override def deriveAvgAggType(
      typeFactory: RelDataTypeFactory,
      argRelDataType: RelDataType)
    : RelDataType = {
    val argType = FlinkTypeFactory.toLogicalType(argRelDataType)
    val resultType = LogicalTypeMerging.findAvgAggType(argType)
    unwrapTypeFactory(typeFactory).createFieldTypeFromLogicalType(resultType)
  }

  override def deriveSumType(
      typeFactory: RelDataTypeFactory,
      argRelDataType: RelDataType)
    : RelDataType = {
    val argType = FlinkTypeFactory.toLogicalType(argRelDataType)
    val resultType = LogicalTypeMerging.findSumAggType(argType)
    unwrapTypeFactory(typeFactory).createFieldTypeFromLogicalType(resultType)
  }

  override def deriveDecimalPlusType(
      typeFactory: RelDataTypeFactory,
      type1: RelDataType,
      type2: RelDataType): RelDataType = {
    deriveDecimalType(typeFactory, type1, type2,
      (p1, s1, p2, s2) => LogicalTypeMerging.findAdditionDecimalType(p1, s1, p2, s2))
  }

  override def deriveDecimalModType(
      typeFactory: RelDataTypeFactory,
      type1: RelDataType,
      type2: RelDataType): RelDataType = {
    deriveDecimalType(typeFactory, type1, type2,
      (p1, s1, p2, s2) => {
        if (s1 == 0 && s2 == 0) {
          return type2
        }
        LogicalTypeMerging.findModuloDecimalType(p1, s1, p2, s2)
      })
  }

  override def deriveDecimalDivideType(
      typeFactory: RelDataTypeFactory,
      type1: RelDataType,
      type2: RelDataType): RelDataType = {
    deriveDecimalType(typeFactory, type1, type2,
      (p1, s1, p2, s2) => LogicalTypeMerging.findDivisionDecimalType(p1, s1, p2, s2))
  }

  override def deriveDecimalMultiplyType(
      typeFactory: RelDataTypeFactory,
      type1: RelDataType,
      type2: RelDataType): RelDataType = {
    deriveDecimalType(typeFactory, type1, type2,
      (p1, s1, p2, s2) => LogicalTypeMerging.findMultiplicationDecimalType(p1, s1, p2, s2))
  }

  /**
   * Use derivation from [[LogicalTypeMerging]] to derive decimal type.
   */
  private def deriveDecimalType(
      typeFactory: RelDataTypeFactory,
      type1: RelDataType,
      type2: RelDataType,
      deriveImpl: (Int, Int, Int, Int) => DecimalType): RelDataType = {
    if (SqlTypeUtil.isExactNumeric(type1) && SqlTypeUtil.isExactNumeric(type2) &&
        (SqlTypeUtil.isDecimal(type1) || SqlTypeUtil.isDecimal(type2))) {
      val decType1 = adjustType(typeFactory, type1)
      val decType2 = adjustType(typeFactory, type2)
      val result = deriveImpl(
        decType1.getPrecision, decType1.getScale, decType2.getPrecision, decType2.getScale)
      typeFactory.createSqlType(SqlTypeName.DECIMAL, result.getPrecision, result.getScale)
    } else {
      null
    }
  }

  /**
   * Java numeric will always have invalid precision/scale,
   * use its default decimal precision/scale instead.
   */
  private def adjustType(
      typeFactory: RelDataTypeFactory,
      relDataType: RelDataType): RelDataType = {
    if (RelDataTypeFactoryImpl.isJavaType(relDataType)) {
      typeFactory.decimalOf(relDataType)
    } else {
      relDataType
    }
  }
}

object FlinkTypeSystem {

  val DECIMAL_SYSTEM_DEFAULT = new DecimalType(DecimalType.MAX_PRECISION, 18)
}
