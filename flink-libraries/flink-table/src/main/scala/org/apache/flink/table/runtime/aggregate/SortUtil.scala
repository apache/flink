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
import java.sql.Timestamp
import org.apache.calcite.rel.RelFieldCollation
import org.apache.calcite.rel.RelFieldCollation.Direction
import java.util.Comparator
import org.apache.flink.api.common.typeutils.TypeComparator
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import java.lang.{Byte=>JByte,Integer=>JInt,Long=>JLong,Double=>JDouble,Short=>JShort,String=>JString,Float=>JFloat}
import java.math.{BigDecimal=>JBigDecimal}
import org.apache.flink.api.common.functions.MapFunction

/**
 * Class represents a collection of helper methods to build the sort logic.
 * It encapsulates as well the implementation for ordering and generic interfaces
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

    val inputRowType = FlinkTypeFactory.toInternalRowTypeInfo(inputType).asInstanceOf[RowTypeInfo]
    
    val orderings = createOrderingComparison(inputType, keySortFields, keySortDirections)
    
    //drop time from comparison
    val keyIndexesNoTime = keySortFields.slice(1, keySortFields.size)
    val orderingsNoTime = orderings.slice(1, keySortFields.size)
    
    val rowComparator = createRowSortComparator(keyIndexesNoTime,orderingsNoTime)
    
    new ProcTimeBoundedSortProcessFunction(
      inputType.getFieldCount,
      inputRowType,
      rowComparator)

  }

   /**
   * Function creates a row comparator for the sorting fields based on
   * [java.util.Comparator] objects derived from [org.apache.flink.api.common.TypeInfo]
   * @param keyIndex the indexes of the fields on which the sorting is done. 
   * First is expected to be the time  
   * @param orderings the [UntypedOrdering] objects 
   * @return Array of ordering objects
   */
  def createRowSortComparator(keyIndex: Array[Int],
    orderings:Array[UntypedOrdering]): Comparator[Row] = {
          
    new SortRowComparator(orderings,keyIndex)
  }
  
   /**
   * Function creates comparison objects with embeded type casting 
   * @param inputType input row type
   * @param keyIndex the indexes of the fields on which the sorting is done. 
   * First is expected to be the time  
   * @return Array of ordering objects
   */
  def createOrderingComparison(inputType: RelDataType,
    keyIndex: Array[Int],
    orderDirection: Array[Direction]): Array[UntypedOrdering] = {

    var i = 0
    val orderings = new Array[UntypedOrdering](keyIndex.size)

    while (i < keyIndex.size) {
      val sqlTypeName = inputType.getFieldList.get(keyIndex(i)).getType.getSqlTypeName

      orderings(i) = sqlTypeName match {
        case TINYINT => new ByteOrdering(
            BYTE_TYPE_INFO.createComparator(orderDirection(i)==Direction.ASCENDING, null))
        case SMALLINT => new SmallOrdering(
            SHORT_TYPE_INFO.createComparator(orderDirection(i)==Direction.ASCENDING, null))
        case INTEGER => new IntOrdering(
            INT_TYPE_INFO.createComparator(orderDirection(i)==Direction.ASCENDING, null))
        case BIGINT => new LongOrdering(
            LONG_TYPE_INFO.createComparator(orderDirection(i)==Direction.ASCENDING, null))
        case FLOAT => new FloatOrdering(
            FLOAT_TYPE_INFO.createComparator(orderDirection(i)==Direction.ASCENDING, null))
        case DOUBLE => new DoubleOrdering(
            DOUBLE_TYPE_INFO.createComparator(orderDirection(i)==Direction.ASCENDING, null))
        case DECIMAL => new DecimalOrdering(
            BIG_DEC_TYPE_INFO.createComparator(orderDirection(i)==Direction.ASCENDING, null))
        case VARCHAR | CHAR => new StringOrdering(
            STRING_TYPE_INFO.createComparator(orderDirection(i)==Direction.ASCENDING, null))
        //should be updated when times are merged in master branch based on their types
        case TIMESTAMP => new TimestampOrdering(
            LONG_TYPE_INFO.createComparator(orderDirection(i)==Direction.ASCENDING, null))
        case sqlType: SqlTypeName =>
            throw new TableException("Sort aggregate does no support type:" + sqlType)
      }
      i += 1
    }
    
    orderings
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
   * Function returns the array of sort direction for each of the sort fields 
   * @param calcSort The LogicalSort object
   * @return [Array[Direction]]
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
   * Function returns the direction type of the time in order clause. 
   * @param calcSort The LogicalSort object
   * @return [Array[Int]]
   */
  def getTimeDirection(calcSort: LogicalSort):Direction = {
    calcSort.getCollationList.get(0).getFieldCollations.get(0).direction
  }
  
   /**
   * Function returns the time type in order clause. 
   * Expectation is that it is the primary sort field
   * @param calcSort The LogicalSort object
   * @param rowType The data type of the input
   * @return [org.apache.calcite.rel.type.RelDataType]
   */
  def getTimeType(calcSort: LogicalSort, rowType: RelDataType): RelDataType = {

    //need to identify time between others order fields
    //
    val ind = calcSort.getCollationList.get(0).getFieldCollations.get(0).getFieldIndex
    rowType.getFieldList.get(ind).getValue
  }

   /**
   * Function returns the direction type of a field in order clause. 
   * @param calcSort The LogicalSort object
   * @return [org.apache.calcite.rel.RelFieldCollation.Direction]
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

class LongOrdering(private val ord: TypeComparator[JLong]) extends UntypedOrdering {

  override def compare(x: Any, y: Any): Int = {
    val xL = x.asInstanceOf[JLong]
    val yL = y.asInstanceOf[JLong]
    ord.compare(xL, yL)
  }
}

class IntOrdering(private val ord: TypeComparator[JInt]) extends UntypedOrdering {

  override def compare(x: Any, y: Any): Int = {
    val xI = x.asInstanceOf[JInt]
    val yI = y.asInstanceOf[JInt]
    ord.compare(xI, yI)
  }
}

class FloatOrdering(private val ord: TypeComparator[JFloat]) extends UntypedOrdering {

  override def compare(x: Any, y: Any): Int = {
    val xF = x.asInstanceOf[JFloat]
    val yF = y.asInstanceOf[JFloat]
    ord.compare(xF, yF)
  }
}

class DoubleOrdering(private val ord: TypeComparator[JDouble]) extends UntypedOrdering {

  override def compare(x: Any, y: Any): Int = {
    val xD = x.asInstanceOf[JDouble]
    val yD = y.asInstanceOf[JDouble]
    ord.compare(xD, yD)
  }
}

class DecimalOrdering(private val ord: TypeComparator[JBigDecimal]) extends UntypedOrdering {

  override def compare(x: Any, y: Any): Int = {
    val xBD = x.asInstanceOf[JBigDecimal]
    val yBD = y.asInstanceOf[JBigDecimal]
    ord.compare(xBD, yBD)
  }
}

class ByteOrdering(private val ord: TypeComparator[JByte]) extends UntypedOrdering {

  override def compare(x: Any, y: Any): Int = {
    val xB = x.asInstanceOf[JByte]
    val yB = y.asInstanceOf[JByte]
    ord.compare(xB, yB)
  }
}

class SmallOrdering(private val ord: TypeComparator[JShort]) extends UntypedOrdering {

  override def compare(x: Any, y: Any): Int = {
    val xS = x.asInstanceOf[JShort]
    val yS = y.asInstanceOf[JShort]
    ord.compare(xS, yS)
  }
}

class StringOrdering(private val ord: TypeComparator[JString]) extends UntypedOrdering {

  override def compare(x: Any, y: Any): Int = {
    val xS = x.asInstanceOf[JString]
    val yS = y.asInstanceOf[JString]
    ord.compare(xS, yS)
  }
}


/**
 * Ordering object for timestamps. As there is no implicit Ordering for [java.sql.Timestamp]
 * we need to compare based on the Long value of the timestamp
 */
class TimestampOrdering(private val ord: TypeComparator[JLong]) extends UntypedOrdering {

  override def compare(x: Any, y: Any): Int = {
    val xTs = x.asInstanceOf[Timestamp]
    val yTs = y.asInstanceOf[Timestamp]
    ord.compare(xTs.getTime, yTs.getTime)
  }
}


/**
 * Called every time when a sort operation is done. It applies the Comparator objects in cascade
 *
 * @param orderedComparators the sort Comparator objects with type casting
 * @param keyIndexes the sort index fields on which to apply the comparison on the inputs 
 */
class SortRowComparator(
    private val orderedComparators:Array[UntypedOrdering],
    private val keyIndexes:Array[Int]) extends Comparator[Row] with Serializable {
  override def compare(arg0:Row, arg1:Row):Int = {
   
     var i = 0
     var result:Int = 0 
     while (i<keyIndexes.size) {
          
          val compareResult = orderedComparators(i).compare(
              arg0.getField(keyIndexes(i)), arg1.getField(keyIndexes(i)))
          
          compareResult match {
            case 0 => i += 1 //same key and need to sort on consequent keys 
            case g => { result = g  //in case the case does not return the result          
              i = keyIndexes.size // exit and return the result
            }
          }
        }  
     result //all sort fields were equal, hence elements are equal
  }
  
}


/**
 * Identity map for forwarding the fields based on their arriving times
 */
private[flink] class IdentityRowMap extends MapFunction[Row,Row] {
   override def map(value:Row):Row ={
     value
   }
 }
