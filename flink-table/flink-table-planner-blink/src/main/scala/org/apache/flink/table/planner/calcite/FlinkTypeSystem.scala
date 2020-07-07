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

import org.apache.flink.table.runtime.typeutils.TypeCheckUtils
import org.apache.flink.table.types.logical.{DecimalType, LocalZonedTimestampType, LogicalType, TimestampType}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, RelDataTypeSystemImpl}
import org.apache.calcite.sql.`type`.{SqlTypeName, SqlTypeUtil}
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging

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
      typeFactory: RelDataTypeFactory, argType: RelDataType): RelDataType = {
    val argTypeInfo = FlinkTypeFactory.toLogicalType(argType)
    val avgType = FlinkTypeSystem.deriveAvgAggType(argTypeInfo)
    typeFactory.asInstanceOf[FlinkTypeFactory].createFieldTypeFromLogicalType(
      avgType.copy(argType.isNullable))
  }

  override def deriveSumType(
      typeFactory: RelDataTypeFactory, argType: RelDataType): RelDataType = {
    val argTypeInfo = FlinkTypeFactory.toLogicalType(argType)
    val sumType = FlinkTypeSystem.deriveSumType(argTypeInfo)
    typeFactory.asInstanceOf[FlinkTypeFactory].createFieldTypeFromLogicalType(
      sumType.copy(argType.isNullable))
  }

  /**
    * Calcite's default impl for division is apparently borrowed from T-SQL,
    * but the details are a little different, e.g. when Decimal(34,0)/Decimal(10,0)
    * To avoid confusion, follow the exact T-SQL behavior.
    * Note that for (+-*), Calcite is also different from T-SQL;
    * however, Calcite conforms to SQL2003 while T-SQL does not.
    * therefore we keep Calcite's behavior on (+-*).
    */
  override def deriveDecimalDivideType(
      typeFactory: RelDataTypeFactory,
      type1: RelDataType,
      type2: RelDataType): RelDataType = {
    if (SqlTypeUtil.isExactNumeric(type1) && SqlTypeUtil.isExactNumeric(type2) &&
      (SqlTypeUtil.isDecimal(type1) || SqlTypeUtil.isDecimal(type2))) {
      val result = LogicalTypeMerging.findDivisionDecimalType(
        type1.getPrecision, type1.getScale,
        type2.getPrecision, type2.getScale)
      typeFactory.createSqlType(SqlTypeName.DECIMAL, result.getPrecision, result.getScale)
    } else {
      null
    }
  }
}

object FlinkTypeSystem {

  def deriveAvgAggType(argType: LogicalType): LogicalType = argType match {
    case dt: DecimalType =>
      val result = inferAggAvgType(dt.getScale)
      new DecimalType(result.getPrecision, result.getScale)
    case nt if TypeCheckUtils.isNumeric(nt) => nt
    case _ =>
      throw new RuntimeException("Unsupported argType for AVG(): " + argType)
  }

  def deriveSumType(argType: LogicalType): LogicalType = argType match {
    case dt: DecimalType =>
      val result = inferAggSumType(dt.getScale())
      new DecimalType(result.getPrecision(), result.getScale())
    case nt if TypeCheckUtils.isNumeric(nt) =>
      argType
    case _ =>
      throw new RuntimeException("Unsupported argType for SUM(): " + argType)
  }

  def inferIntDivType(precision1: Int, scale1: Int, scale2: Int): DecimalType = {
    val p = Math.min(38, precision1 - scale1 + scale2)
    new DecimalType(p, 0)
  }

  /**
    * https://docs.microsoft.com/en-us/sql/t-sql/functions/sum-transact-sql.
    */
  def inferAggSumType(scale: Int) = new DecimalType(38, scale)

  /**
    * https://docs.microsoft.com/en-us/sql/t-sql/functions/avg-transact-sql
    * however, we count by LONG, therefore divide by Decimal(20,0),
    * but the end result is actually the same, which is Decimal(38, max(6,s)).
    */
  def inferAggAvgType(scale: Int): DecimalType =
    LogicalTypeMerging.findDivisionDecimalType(38, scale, 20, 0)

  val DECIMAL_SYSTEM_DEFAULT = new DecimalType(DecimalType.MAX_PRECISION, 18)
}
