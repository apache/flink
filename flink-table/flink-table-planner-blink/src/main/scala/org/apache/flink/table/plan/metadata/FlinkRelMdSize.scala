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

package org.apache.flink.table.plan.metadata

import org.apache.flink.table.api.TableException
import org.apache.flink.table.plan.schema.FlinkRelOptTable

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.util.{BuiltInMethod, Util}

import java.lang.Double
import java.util.{List => JList}

import scala.collection.JavaConversions._

/**
  * FlinkRelMdSize supplies a implementation of
  * [[RelMetadataQuery#getAverageRowSize]] and
  * [[RelMetadataQuery#getAverageColumnSizes]] for the standard logical algebra.
  *
  * <p>Different from Calcite's default implementation, [[FlinkRelMdSize]] throws
  * [[RelMdMethodNotImplementedException]] to disable the implementation of averageColumnSizes
  * on [[RelNode]] and requires to provide implementation on each kind of RelNode.
  *
  * <p>When add a new kind of RelNode, the author maybe forget to implement the logic for
  * the new rel, and get the unexpected result from the method on [[RelNode]]. So this handler
  * will force the author to implement the logic for new rel to avoid the unexpected result.
  */
class FlinkRelMdSize private extends MetadataHandler[BuiltInMetadata.Size] {

  def getDef: MetadataDef[BuiltInMetadata.Size] = BuiltInMetadata.Size.DEF

  // ----- averageRowSize -----------------------------------------------------

  def averageRowSize(rel: RelNode, mq: RelMetadataQuery): Double = {
    val averageColumnSizes = mq.getAverageColumnSizes(rel)
    if (averageColumnSizes == null) {
      FlinkRelMdSize.estimateRowSize(rel.getRowType)
    } else {
      val fields = rel.getRowType.getFieldList
      val columnSizes = averageColumnSizes.zip(fields).map {
        case (columnSize, field) =>
          if (columnSize == null) {
            FlinkRelMdSize.averageTypeValueSize(field.getType)
          } else {
            columnSize
          }
      }
      columnSizes.foldLeft(0D)(_ + _)
    }
  }

  // ----- averageColumnSizes -------------------------------------------------

  def averageColumnSizes(rel: TableScan, mq: RelMetadataQuery): JList[Double] = {
    val statistic = rel.getTable.asInstanceOf[FlinkRelOptTable].getFlinkStatistic
    rel.getRowType.getFieldList.map {
      field =>
        val colStats = statistic.getColumnStats(field.getName)
        if (colStats != null && colStats.getAvgLen != null) {
          colStats.getAvgLen
        } else {
          FlinkRelMdSize.averageTypeValueSize(field.getType)
        }
    }
  }

  def averageColumnSizes(subset: RelSubset, mq: RelMetadataQuery): JList[Double] = {
    val rel = Util.first(subset.getBest, subset.getOriginal)
    mq.getAverageColumnSizes(rel)
  }


  /**
    * Throws [[RelMdMethodNotImplementedException]] to
    * force implement [[averageColumnSizes]] logic on each kind of RelNode.
    */
  def averageColumnSizes(rel: RelNode, mq: RelMetadataQuery): JList[Double] = {
    throw RelMdMethodNotImplementedException(
      "averageColumnSizes", getClass.getSimpleName, rel.getRelTypeName)
  }

}

object FlinkRelMdSize {

  private val INSTANCE = new FlinkRelMdSize

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    INSTANCE,
    BuiltInMethod.AVERAGE_COLUMN_SIZES.method,
    BuiltInMethod.AVERAGE_ROW_SIZE.method)

  def averageTypeValueSize(t: RelDataType): Double = t.getSqlTypeName match {
    case SqlTypeName.ROW =>
      estimateRowSize(t)
    case SqlTypeName.ARRAY =>
      // 16 is an arbitrary estimate
      averageTypeValueSize(t.getComponentType) * 16
    case SqlTypeName.MAP =>
      // 16 is an arbitrary estimate
      (averageTypeValueSize(t.getKeyType) + averageTypeValueSize(t.getValueType)) * 16
    case SqlTypeName.MULTISET =>
      // 16 is an arbitrary estimate
      (averageTypeValueSize(t.getComponentType) + averageTypeValueSize(SqlTypeName.INTEGER)) * 16
    case _ => averageTypeValueSize(t.getSqlTypeName)
  }

  private def estimateRowSize(rowType: RelDataType): Double = {
    val fieldList = rowType.getFieldList
    fieldList.map(_.getType).foldLeft(0.0) {
      (s, t) =>
        s + averageTypeValueSize(t)
    }
  }

  def averageTypeValueSize(sqlType: SqlTypeName): Double = sqlType match {
    case SqlTypeName.TINYINT => 1D
    case SqlTypeName.SMALLINT => 2D
    case SqlTypeName.INTEGER => 4D
    case SqlTypeName.BIGINT => 8D
    case SqlTypeName.BOOLEAN => 1D
    case SqlTypeName.FLOAT => 4D
    case SqlTypeName.DOUBLE => 8D
    case SqlTypeName.VARCHAR => 12D
    case SqlTypeName.CHAR => 1D
    case SqlTypeName.DECIMAL => 12D
    case typeName if SqlTypeName.YEAR_INTERVAL_TYPES.contains(typeName) => 8D
    case typeName if SqlTypeName.DAY_INTERVAL_TYPES.contains(typeName) => 4D
    // TODO after time/date => int, timestamp => long, this estimate value should update
    case SqlTypeName.TIME | SqlTypeName.TIMESTAMP | SqlTypeName.DATE => 12D
    case SqlTypeName.ANY => 128D // 128 is an arbitrary estimate
    case SqlTypeName.BINARY | SqlTypeName.VARBINARY => 16D // 16 is an arbitrary estimate
    case _ => throw new TableException(s"Unsupported data type encountered: $sqlType")
  }

}
