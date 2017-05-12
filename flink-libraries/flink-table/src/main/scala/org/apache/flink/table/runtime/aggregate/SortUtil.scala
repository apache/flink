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
import org.apache.flink.types.Row
import org.apache.calcite.rel.`type`._
import org.apache.calcite.rel.RelCollation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.functions.AggregateFunction
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.flink.table.api.TableException
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.`type`.SqlTypeName._
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
import org.apache.flink.api.common.operators.Order
import org.apache.calcite.rex.{RexLiteral, RexNode}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.AtomicType
import org.apache.flink.api.java.typeutils.runtime.RowComparator
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}

import scala.collection.JavaConverters._

/**
 * Class represents a collection of helper methods to build the sort logic.
 * It encapsulates as well the implementation for ordering and generic interfaces
 */

object SortUtil {

  
  /**
   * Function creates [org.apache.flink.streaming.api.functions.ProcessFunction] for sorting 
   * elements based on rowtime and potentially other fields
   * @param collationSort The Sort collation list
   * @param inputType input row type
   * @param execCfg table environment execution configuration
   * @return org.apache.flink.streaming.api.functions.ProcessFunction
   */
  private[flink] def createRowTimeSortFunction(
    collationSort: RelCollation,
    inputType: RelDataType,
    inputTypeInfo: TypeInformation[Row],
    execCfg: ExecutionConfig): ProcessFunction[CRow, CRow] = {

    val keySortFields = getSortFieldIndexList(collationSort)
    val keySortDirections = getSortFieldDirectionList(collationSort)

       //drop time from comparison as we sort on time in the states and result emission
    val keyIndexesNoTime = keySortFields.slice(1, keySortFields.size)
    val keyDirectionsNoTime = keySortDirections.slice(1, keySortDirections.size)
    val booleanOrderings = getSortFieldDirectionBooleanList(collationSort)
    val booleanDirectionsNoTime = booleanOrderings.slice(1, booleanOrderings.size)
    
    val fieldComps = createFieldComparators(inputType, 
        keyIndexesNoTime, keyDirectionsNoTime, execCfg)
    val fieldCompsRefs = fieldComps.asInstanceOf[Array[TypeComparator[AnyRef]]]
    
    val rowComp = createRowComparator(inputType,
        keyIndexesNoTime, fieldCompsRefs, booleanDirectionsNoTime)
    val collectionRowComparator = new CollectionRowComparator(rowComp)
    
    val inputCRowType = CRowTypeInfo(inputTypeInfo)
 
    new RowTimeSortProcessFunction(
      inputType.getFieldCount,
      inputCRowType,
      collectionRowComparator)

  }
  
  
  /**
   * Function creates [org.apache.flink.streaming.api.functions.ProcessFunction] for sorting 
   * elements based on proctime and potentially other fields
   * @param collationSort The Sort collation list
   * @param inputType input row type
   * @param execCfg table environment execution configuration
   * @return org.apache.flink.streaming.api.functions.ProcessFunction
   */
  private[flink] def createProcTimeSortFunction(
    collationSort: RelCollation,
    inputType: RelDataType,
    inputTypeInfo: TypeInformation[Row],
    execCfg: ExecutionConfig): ProcessFunction[CRow, CRow] = {

    val keySortFields = getSortFieldIndexList(collationSort)
    val keySortDirections = getSortFieldDirectionList(collationSort)

    
       //drop time from comparison
    val keyIndexesNoTime = keySortFields.slice(1, keySortFields.size)
    val keyDirectionsNoTime = keySortDirections.slice(1, keySortDirections.size)
    val booleanOrderings = getSortFieldDirectionBooleanList(collationSort)
    val booleanDirectionsNoTime = booleanOrderings.slice(1, booleanOrderings.size)
    
    val fieldComps = createFieldComparators(inputType, 
        keyIndexesNoTime, keyDirectionsNoTime, execCfg)
    val fieldCompsRefs = fieldComps.asInstanceOf[Array[TypeComparator[AnyRef]]]
    
    val rowComp = createRowComparator(inputType, 
        keyIndexesNoTime, fieldCompsRefs, booleanDirectionsNoTime)
    val collectionRowComparator = new CollectionRowComparator(rowComp)
    
    val inputCRowType = CRowTypeInfo(inputTypeInfo)
    
    new ProcTimeSortProcessFunction(
      inputType.getFieldCount,
      inputCRowType,
      collectionRowComparator)

  }

  
   /**
   * Function creates comparison objects based on the field types
   * @param inputType input row type
   * @param keyIndex the indexes of the fields on which the sorting is done. 
   * @param orderDirection the directions of each sort field. 
   * @param execConfig the configuration environment
   * @return Array of TypeComparator objects
   */
  def createFieldComparators(
      inputType: RelDataType,
      keyIndex: Array[Int],
      orderDirection: Array[Direction],
      execConfig: ExecutionConfig): Array[TypeComparator[_]] = {
    
    var iOrder = 0
    for (i <- keyIndex) yield {

      val order = if (orderDirection(iOrder) == Direction.ASCENDING) true else false
      iOrder += 1
      val fieldTypeInfo = FlinkTypeFactory.toTypeInfo(inputType.getFieldList.get(i).getType)
      fieldTypeInfo match {
        case a: AtomicType[_] => a.createComparator(order, execConfig)
        case _ => throw new TableException(s"Unsupported field type $fieldTypeInfo to sort on.")
      }
    }
  }
  
   /**
   * Function creates a RowComparator based on the typed comparators
   * @param inputRowType input row type
   * @param fieldIdxs the indexes of the fields on which the sorting is done. 
   * @param fieldComps the array of typed comparators
   * @param fieldOrders the directions of each sort field (true = ASC). 
   * @return A Row TypeComparator object 
   */
  def createRowComparator(
    inputRowType: RelDataType,
    fieldIdxs: Array[Int],
    fieldComps: Array[TypeComparator[AnyRef]],
    fieldOrders: Array[Boolean]): TypeComparator[Row] = {

  val rowComp = new RowComparator(
    inputRowType.getFieldCount,
    fieldIdxs,
    fieldComps,
    new Array[TypeSerializer[AnyRef]](0), //used only for serialized comparisons
    fieldOrders)

  rowComp
}
  
 
  /**
   * Function returns the array of indexes for the fields on which the sort is done
   * @param collationSort The Sort collation list
   * @return [Array[Int]]
   */
  def getSortFieldIndexList(collationSort: RelCollation): Array[Int] = {
    val keyFields = collationSort.getFieldCollations.toArray()
    for (f <- keyFields) yield f.asInstanceOf[RelFieldCollation].getFieldIndex
  }
  
   /**
   * Function returns the array of sort direction for each of the sort fields 
   * @param collationSort The Sort collation list
   * @return [Array[Direction]]
   */
  def getSortFieldDirectionList(collationSort: RelCollation): Array[Direction] = {
    val keyFields = collationSort.getFieldCollations.toArray()
    for(f <- keyFields) yield f.asInstanceOf[RelFieldCollation].getDirection
  }

   /**
   * Function returns the array of sort direction for each of the sort fields 
   * @param collationSort The Sort collation list
   * @return [Array[Direction]]
   */
  def getSortFieldDirectionBooleanList(collationSort: RelCollation): Array[Boolean] = {
    val keyFields = collationSort.getFieldCollations
    var i = 0
    val keySortDirection = new Array[Boolean](keyFields.size())
    while (i < keyFields.size()) {
      keySortDirection(i) = 
        if (getDirection(collationSort,i) == Direction.ASCENDING) true else false 
      i += 1
    }
    keySortDirection
  }
  
  
   /**
   * Function returns the direction type of the time in order clause. 
   * @param collationSort The Sort collation list of objects
   * @return [org.apache.calcite.rel.RelFieldCollation.Direction]
   */
  def getTimeDirection(collationSort: RelCollation): Direction = {
    collationSort.getFieldCollations.get(0).direction
  }
  
   /**
   * Function returns the time type in order clause. 
   * Expectation is that it is the primary sort field
   * @param collationSort The Sort collation list
   * @param rowType The data type of the input
   * @return [org.apache.calcite.rel.type.RelDataType]
   */
  def getTimeType(collationSort: RelCollation, rowType: RelDataType): RelDataType = {

    //need to identify time between others ordering fields
    val ind = collationSort.getFieldCollations.get(0).getFieldIndex
    rowType.getFieldList.get(ind).getValue
  }

   /**
   * Function returns the direction type of a field in order clause. 
   * @param collationSort The Sort collation list
   * @return [org.apache.calcite.rel.RelFieldCollation.Direction]
   */
  def getDirection(collationSort: RelCollation, sortField:Int): Direction = {
    
    collationSort.getFieldCollations.get(sortField).direction match {
      case Direction.ASCENDING => Direction.ASCENDING
      case Direction.DESCENDING => Direction.DESCENDING
      case _ =>  throw new TableException("SQL/Table does not support such sorting")
    }
    
  }
  
  def directionToOrder(direction: Direction) = {
    direction match {
      case Direction.ASCENDING | Direction.STRICTLY_ASCENDING => Order.ASCENDING
      case Direction.DESCENDING | Direction.STRICTLY_DESCENDING => Order.DESCENDING
      case _ => throw new IllegalArgumentException("Unsupported direction.")
    }
  }
  
  def getSortFieldToString(collationSort: RelCollation, rowRelDataType: RelDataType): String = {
    val fieldCollations = collationSort.getFieldCollations.asScala  
    .map(c => (c.getFieldIndex, directionToOrder(c.getDirection)))

    val sortFieldsToString = fieldCollations
      .map(col => s"${
        rowRelDataType.getFieldNames.get(col._1)} ${col._2.getShortName}" ).mkString(", ")
    
    sortFieldsToString
  }
  
  def getOffsetToString(offset: RexNode): String = {
    val offsetToString = s"$offset"
    offsetToString
  }
  
  def getFetchToString(fetch: RexNode, offset: RexNode): String = {
    val limitEnd = getFetchLimitEnd(fetch, offset)
    val fetchToString = if (limitEnd == Long.MaxValue) {
      "unlimited"
    } else {
      s"$limitEnd"
    }
    fetchToString
  }
  
  def getFetchLimitEnd (fetch: RexNode, offset: RexNode): Long = {
    val limitEnd: Long = if (fetch != null) {
      RexLiteral.intValue(fetch) + getFetchLimitStart(fetch, offset)
    } else {
      Long.MaxValue
    }
    limitEnd
  }
  
  def getFetchLimitStart (fetch: RexNode, offset: RexNode): Long = {
     val limitStart: Long = if (offset != null) {
      RexLiteral.intValue(offset)
     } else {
       0L
     }
     limitStart
  }
  
}

/**
 * Wrapper for Row TypeComparator to a Java Comparator object
 */
class CollectionRowComparator(
    private val rowComp: TypeComparator[Row]) extends Comparator[Row] with Serializable {
  
  override def compare(arg0:Row, arg1:Row):Int = {
    rowComp.compare(arg0, arg1)
  }
}



/**
 * Identity map for forwarding the fields based on their arriving times
 */
private[flink] class IdentityRowMap extends MapFunction[CRow,CRow] {
   override def map(value:CRow):CRow ={
     value
   }
 }
