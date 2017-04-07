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
package org.apache.flink.table.runtime.aggregate

import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.types.Row
import org.apache.calcite.rel.`type`._
import org.apache.calcite.rel.logical.LogicalSort
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.functions.AggregateFunction
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.flink.table.api.TableException
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.flink.table.functions.Accumulator
import java.util.{ List => JList, ArrayList }
import org.apache.flink.api.common.typeinfo.{ SqlTimeTypeInfo, TypeInformation }
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.functions.aggfunctions.RowSortAggFunction
import java.sql.Timestamp
import org.apache.calcite.rel.RelFieldCollation
import org.apache.calcite.rel.RelFieldCollation.Direction


/**
 * Class represents a collection of helper methods to build the sort logic.
 * It encapsulates as well implementation for ordering and generic interfaces
 */

object SortUtil {

  /**
   * Function creates [org.apache.flink.streaming.api.functions.ProcessFunction] for sorting 
   * elements based on proctime and potentially other fields
   * @param calcSort Sort logical object
   * @param inputType input row type
   * @return org.apache.flink.streaming.api.functions.ProcessFunction
   */
  private[flink] def createProcTimeSortFunction(
    calcSort: LogicalSort,
    inputType: RelDataType): ProcessFunction[Row, Row] = {

    val keySortFields = getSortFieldIndexList(calcSort)
    val keySortDirections = getSortFieldDirectionList(calcSort)
    val sortAggregates = createSortAggregation(inputType, keySortFields,keySortDirections, false)

    val aggType = createSingleAccumulatorRowType(sortAggregates)
    
   new ProcTimeUnboundedSortProcessFunction(
      sortAggregates,
      inputType.getFieldCount,
      aggType)

  }

  
   /**
   * Function creates a sorting aggregation object 
   * elements based on proctime and potentially other fields
   * @param inputType input row type
   * @param keyIndex the indexes of the fields on which the sorting is done. 
   * @param keySortDirections the directions of the sorts for each field. 
   * First is expected to be the time  
   * @return SortAggregationFunction
   */
  private def createSortAggregation(
    inputType: RelDataType,
    keyIndex: Array[Int],
    keySortDirections: Array[Direction],
    retraction: Boolean): MultiOutputAggregateFunction[_] = {

    val orderings = createOrderingComparison(inputType, keyIndex)

    val sortKeyType = toKeySortInternalRowTypeInfo(inputType, keyIndex).asInstanceOf[RowTypeInfo]

    // get the output types
    val rowTypeInfo = FlinkTypeFactory.toInternalRowTypeInfo(inputType).asInstanceOf[RowTypeInfo]

    val sortAggFunc = new RowSortAggFunction(keyIndex,
        keySortDirections, orderings, rowTypeInfo, sortKeyType)

    sortAggFunc

  }
  
   /**
   * Function creates a typed based comparison objects 
   * @param inputType input row type
   * @param keyIndex the indexes of the fields on which the sorting is done. 
   * First is expected to be the time  
   * @return Array of ordering objects
   */
  
  def createOrderingComparison(inputType: RelDataType,
    keyIndex: Array[Int]): Array[UntypedOrdering] = {

    var i = 0
    val orderings = new Array[UntypedOrdering](keyIndex.size)

    while (i < keyIndex.size) {
      val sqlTypeName = inputType.getFieldList.get(keyIndex(i)).getType.getSqlTypeName

      orderings(i) = sqlTypeName match {
        case TINYINT =>
          new ByteOrdering()
        case SMALLINT =>
          new SmallOrdering()
        case INTEGER =>
          new IntOrdering()
        case BIGINT =>
          new LongOrdering()
        case FLOAT =>
          new FloatOrdering()
        case DOUBLE =>
          new DoubleOrdering()
        case DECIMAL =>
          new DecimalOrdering()
        case VARCHAR | CHAR =>
          new StringOrdering()
        //should be updated when times are merged in master branch based on their types
        case TIMESTAMP =>
          new TimestampOrdering()
        case sqlType: SqlTypeName =>
          throw new TableException("Sort aggregate does no support type:" + sqlType)
      }
      i += 1
    }
    orderings
  }
  
  /**
   * Function creates a type for sort aggregation 
   * @param sort input row type
   * @param keyIndex the indexes of the fields on which the sorting is done. 
   * First is expected to be the time  
   * @return org.apache.flink.streaming.api.functions.ProcessFunction
   */
  private def createSingleAccumulatorRowType(
      sortAggregate: MultiOutputAggregateFunction[_]): RowTypeInfo = {
    
    val accType = sortAggregate.getAccumulatorType
    new RowTypeInfo(accType)
  }

  /**
   * Extracts and converts a Calcite logical record into a Flink type information
   * by selecting certain only a subset of the fields
   */
  def toKeySortInternalRowTypeInfo(logicalRowType: RelDataType,
    keyIndexes: Array[Int]): TypeInformation[Row] = {
    var i = 0
    val fieldList = logicalRowType.getFieldList
    val logicalFieldTypes = new Array[TypeInformation[_]](keyIndexes.size)
    val logicalFieldNames = new Array[String](keyIndexes.size)

    while (i < keyIndexes.size) {
      logicalFieldTypes(i) = (FlinkTypeFactory.toTypeInfo(
        logicalRowType.getFieldList.get(i).getType))
      logicalFieldNames(i) = (logicalRowType.getFieldNames.get(i))
      i += 1
    }

    new RowTypeInfo(logicalFieldTypes.toArray, logicalFieldNames.toArray)

  }

  /**
   * Function returns the array of indexes for the fields on which the sort is done
   * @param calcSort The LogicalSort object
   * @return [Array[Int]]
   */
  def getSortFieldIndexList(calcSort: LogicalSort): Array[Int] = {
    val keyFields = calcSort.collation.getFieldCollations
    var i = 0
    val keySort = new Array[Int](keyFields.size())
    while (i < keyFields.size()) {
      keySort(i) = keyFields.get(i).getFieldIndex
      i += 1
    }
    keySort
  }
  
    /**
   * Function returns the array of sort direction for the sort fields 
   * @param calcSort The LogicalSort object
   * @return [Array[Int]]
   */
  def getSortFieldDirectionList(calcSort: LogicalSort): Array[Direction] = {
    val keyFields = calcSort.collation.getFieldCollations
    var i = 0
    val keySortDirection = new Array[Direction](keyFields.size())
    while (i < keyFields.size()) {
      keySortDirection(i) = getDirection(calcSort,i)
      i += 1
    }
    keySortDirection
  }

  /**
   * Function returns the time type in order clause. Expectation is that if exists if is the
   * primary sort field
   * @param calcSort The LogicalSort object
   * @param rowType The data type of the input
   * @return [Array[Int]]
   */
  def getTimeType(calcSort: LogicalSort, rowType: RelDataType): RelDataType = {

    //need to identify time between others order fields
    //
    val ind = calcSort.getCollationList.get(0).getFieldCollations.get(0).getFieldIndex
    rowType.getFieldList.get(ind).getValue
  }

  /**
   * Function returns the direction type of the time in order clause. 
   * @param calcSort The LogicalSort object
   * @return [Array[Int]]
   */
  def getTimeDirection(calcSort: LogicalSort):Direction = {
    calcSort.getCollationList.get(0).getFieldCollations.get(0).direction
  }
  
   /**
   * Function returns the direction type of the field in order clause. 
   * @param calcSort The LogicalSort object
   * @return [Array[Int]]
   */
  def getDirection(calcSort: LogicalSort, sortField:Int):Direction = {
    
    calcSort.getCollationList.get(0).getFieldCollations.get(sortField).direction match {
      case Direction.ASCENDING => Direction.ASCENDING
      case Direction.DESCENDING => Direction.DESCENDING
      case _ =>  throw new TableException("SQL/Table does not support such sorting")
    }
    
  }
  
}


/**
 * Untyped interface for defining comparison method that can be override by typed implementations
 * Each typed implementation will cast the generic type to the implicit ordering type used 
 */

trait UntypedOrdering extends Serializable{
  def compare(x: Any, y: Any): Int

}

class LongOrdering(implicit ord: Ordering[Long]) extends UntypedOrdering {

  override def compare(x: Any, y: Any): Int = {
    val xL = x.asInstanceOf[Long]
    val yL = y.asInstanceOf[Long]
    ord.compare(xL, yL)
  }
}

class IntOrdering(implicit ord: Ordering[Int]) extends UntypedOrdering {

  override def compare(x: Any, y: Any): Int = {
    val xI = x.asInstanceOf[Int]
    val yI = y.asInstanceOf[Int]
    ord.compare(xI, yI)
  }
}

class FloatOrdering(implicit ord: Ordering[Float]) extends UntypedOrdering {

  override def compare(x: Any, y: Any): Int = {
    val xF = x.asInstanceOf[Float]
    val yF = y.asInstanceOf[Float]
    ord.compare(xF, yF)
  }
}

class DoubleOrdering(implicit ord: Ordering[Double]) extends UntypedOrdering {

  override def compare(x: Any, y: Any): Int = {
    val xD = x.asInstanceOf[Double]
    val yD = y.asInstanceOf[Double]
    ord.compare(xD, yD)
  }
}

class DecimalOrdering(implicit ord: Ordering[BigDecimal]) extends UntypedOrdering {

  override def compare(x: Any, y: Any): Int = {
    val xBD = x.asInstanceOf[BigDecimal]
    val yBD = y.asInstanceOf[BigDecimal]
    ord.compare(xBD, yBD)
  }
}

class ByteOrdering(implicit ord: Ordering[Byte]) extends UntypedOrdering {

  override def compare(x: Any, y: Any): Int = {
    val xB = x.asInstanceOf[Byte]
    val yB = y.asInstanceOf[Byte]
    ord.compare(xB, yB)
  }
}

class SmallOrdering(implicit ord: Ordering[Short]) extends UntypedOrdering {

  override def compare(x: Any, y: Any): Int = {
    val xS = x.asInstanceOf[Short]
    val yS = y.asInstanceOf[Short]
    ord.compare(xS, yS)
  }
}

class StringOrdering(implicit ord: Ordering[String]) extends UntypedOrdering {

  override def compare(x: Any, y: Any): Int = {
    val xS = x.asInstanceOf[String]
    val yS = y.asInstanceOf[String]
    ord.compare(xS, yS)
  }
}

/**
 * Ordering object for timestamps. As there is no implicit Ordering for [java.sql.Timestamp]
 * we need to compare based on the Long value of the timestamp
 */
class TimestampOrdering(implicit ord: Ordering[Long]) extends UntypedOrdering {

  override def compare(x: Any, y: Any): Int = {
    val xTs = x.asInstanceOf[Timestamp]
    val yTs = y.asInstanceOf[Timestamp]
    ord.compare(xTs.getTime, yTs.getTime)
  }
}



/**
 * Called every time when a multi-output aggregation result should be materialized.
 * The returned values could be either an early and incomplete result
 * (periodically emitted as data arrive) or the final result of the
 * aggregation.
 *
 * @param accumulator the accumulator which contains the current
 *                    aggregated results
 * @return the aggregation result
 */
abstract class MultiOutputAggregateFunction[T] extends AggregateFunction[T] {

  def getValues(accumulator: Accumulator): JList[T]
}
