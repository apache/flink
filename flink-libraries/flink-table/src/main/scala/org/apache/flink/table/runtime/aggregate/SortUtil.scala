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
import org.apache.calcite.sql.`type`.SqlTypeName._
import java.util.{ List => JList, ArrayList }
import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
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
import org.apache.calcite.rex.RexLiteral
import java.math.{BigDecimal=>JBigDecimal}

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
    //drop time from comparison as we sort on time in the states and result emission
    val keyIndexesNoTime = keySortFields.slice(1, keySortFields.size)
    val booleanOrderings = getSortFieldDirectionBooleanList(collationSort)
    val booleanDirectionsNoTime = booleanOrderings.slice(1, booleanOrderings.size)
    
    val fieldComps = createFieldComparators(
      inputType, 
      keyIndexesNoTime, 
      booleanDirectionsNoTime,
      execCfg)
    val fieldCompsRefs = fieldComps.asInstanceOf[Array[TypeComparator[AnyRef]]]
    
    val rowComp = new RowComparator(
       inputType.getFieldCount,
       keyIndexesNoTime,
       fieldCompsRefs,
       new Array[TypeSerializer[AnyRef]](0), //used only for object comparisons
       booleanDirectionsNoTime)
      
    val collectionRowComparator = new CollectionRowComparator(rowComp)
    
    val inputCRowType = CRowTypeInfo(inputTypeInfo)
 
    new RowTimeSortProcessFunction(
      inputCRowType,
      collectionRowComparator)

  }
  
  /**
   * Function creates [org.apache.flink.streaming.api.functions.ProcessFunction] for sorting   
   * with offset elements based on rowtime and potentially other fields with
   * @param collationSort The Sort collation list
   * @param sortOffset The offset indicator
   * @param inputType input row type
   * @param inputTypeInfo input type information
   * @param execCfg table environment execution configuration
   * @return org.apache.flink.streaming.api.functions.ProcessFunction
   */
  private[flink] def createRowTimeSortFunctionRetractionOffset(
    collationSort: RelCollation,
    sortOffset: RexNode,
    inputType: RelDataType,
    inputTypeInfo: TypeInformation[Row],
    execCfg: ExecutionConfig): ProcessFunction[CRow, CRow] = {

    val keySortFields = getSortFieldIndexList(collationSort)
    //drop time from comparison
    val keyIndexesNoTime = keySortFields.slice(1, keySortFields.size)
    val booleanOrderings = getSortFieldDirectionBooleanList(collationSort)
    val booleanDirectionsNoTime = booleanOrderings.slice(1, booleanOrderings.size)
    
    val fieldComps = createFieldComparators(
      inputType, 
      keyIndexesNoTime, 
      booleanDirectionsNoTime,
      execCfg)
    val fieldCompsRefs = fieldComps.asInstanceOf[Array[TypeComparator[AnyRef]]]
    
    val rowComp = new RowComparator(
       inputType.getFieldCount,
       keyIndexesNoTime,
       fieldCompsRefs,
       new Array[TypeSerializer[AnyRef]](0), //used only for object comparisons
       booleanDirectionsNoTime)
      
    val collectionRowComparator = new CollectionRowComparator(rowComp)
    
    val inputCRowType = CRowTypeInfo(inputTypeInfo)
    
    val offsetInt = sortOffset.asInstanceOf[RexLiteral].getValue.asInstanceOf[JBigDecimal].intValue
    
    new RowTimeSortProcessFunctionOffset(
      offsetInt,
      inputCRowType,
      collectionRowComparator)

  }
  
  
  /**
   * Function creates [org.apache.flink.streaming.api.functions.ProcessFunction] for sorting   
   * with (offset and) fetch elements based on rowtime and potentially other fields with
   * @param collationSort The Sort collation list
   * @param sortOffset The offset indicator
   * @param sortFetch The fetch indicator
   * @param inputType input row type
   * @param inputTypeInfo input type information
   * @param execCfg table environment execution configuration
   * @return org.apache.flink.streaming.api.functions.ProcessFunction
   */
  private[flink] def createRowTimeSortFunctionRetractionOffsetFetch(
    collationSort: RelCollation,
    sortOffset: RexNode,
    sortFetch: RexNode,
    inputType: RelDataType,
    inputTypeInfo: TypeInformation[Row],
    execCfg: ExecutionConfig): ProcessFunction[CRow, CRow] = {

    val keySortFields = getSortFieldIndexList(collationSort)
    //drop time from comparison
    val keyIndexesNoTime = keySortFields.slice(1, keySortFields.size)
    val booleanOrderings = getSortFieldDirectionBooleanList(collationSort)
    val booleanDirectionsNoTime = booleanOrderings.slice(1, booleanOrderings.size)
    
    val fieldComps = createFieldComparators(
      inputType, 
      keyIndexesNoTime, 
      booleanDirectionsNoTime,
      execCfg)
    val fieldCompsRefs = fieldComps.asInstanceOf[Array[TypeComparator[AnyRef]]]
    
    val rowComp = new RowComparator(
       inputType.getFieldCount,
       keyIndexesNoTime,
       fieldCompsRefs,
       new Array[TypeSerializer[AnyRef]](0), //used only for object comparisons
       booleanDirectionsNoTime)
      
    val collectionRowComparator = new CollectionRowComparator(rowComp)
    
    val inputCRowType = CRowTypeInfo(inputTypeInfo)
    
    val offsetInt = if(sortOffset != null) {
      sortOffset.asInstanceOf[RexLiteral].getValue.asInstanceOf[JBigDecimal].intValue
    } else {
      0
    }
    val fetchInt = sortFetch.asInstanceOf[RexLiteral].getValue.asInstanceOf[JBigDecimal].intValue
        
    
    new RowTimeSortProcessFunctionOffsetFetch(
      offsetInt,
      fetchInt,
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
    //drop time from comparison
    val keyIndexesNoTime = keySortFields.slice(1, keySortFields.size)
    val booleanOrderings = getSortFieldDirectionBooleanList(collationSort)
    val booleanDirectionsNoTime = booleanOrderings.slice(1, booleanOrderings.size)
    
    val fieldComps = createFieldComparators(
      inputType, 
      keyIndexesNoTime, 
      booleanDirectionsNoTime,
      execCfg)
    val fieldCompsRefs = fieldComps.asInstanceOf[Array[TypeComparator[AnyRef]]]
    
    val rowComp = new RowComparator(
       inputType.getFieldCount,
       keyIndexesNoTime,
       fieldCompsRefs,
       new Array[TypeSerializer[AnyRef]](0), //used only for object comparisons
       booleanDirectionsNoTime)
      
    val collectionRowComparator = new CollectionRowComparator(rowComp)
    
    val inputCRowType = CRowTypeInfo(inputTypeInfo)
    
    new ProcTimeSortProcessFunction(
      inputCRowType,
      collectionRowComparator)

  }
  
  
  /**
   * Function creates [org.apache.flink.streaming.api.functions.ProcessFunction] for sorting 
   * elements based on proctime and potentially other fields
   * @param collationSort The Sort collation list
   * @param sortOffset The offset indicator
   * @param inputType input row type
   * @param inputTypeInfo input type information
   * @param execCfg table environment execution configuration
   * @return org.apache.flink.streaming.api.functions.ProcessFunction
   */
  private[flink] def createProcTimeSortFunctionRetractionOffset(
    collationSort: RelCollation,
    sortOffset: RexNode,
    inputType: RelDataType,
    inputTypeInfo: TypeInformation[Row],
    execCfg: ExecutionConfig): ProcessFunction[CRow, CRow] = {

    val keySortFields = getSortFieldIndexList(collationSort)
    //drop time from comparison
    val keyIndexesNoTime = keySortFields.slice(1, keySortFields.size)
    val booleanOrderings = getSortFieldDirectionBooleanList(collationSort)
    val booleanDirectionsNoTime = booleanOrderings.slice(1, booleanOrderings.size)
    
    val fieldComps = createFieldComparators(
      inputType, 
      keyIndexesNoTime, 
      booleanDirectionsNoTime,
      execCfg)
    val fieldCompsRefs = fieldComps.asInstanceOf[Array[TypeComparator[AnyRef]]]
    
    val rowComp = new RowComparator(
       inputType.getFieldCount,
       keyIndexesNoTime,
       fieldCompsRefs,
       new Array[TypeSerializer[AnyRef]](0), //used only for object comparisons
       booleanDirectionsNoTime)
      
    val collectionRowComparator = new CollectionRowComparator(rowComp)
    
    val inputCRowType = CRowTypeInfo(inputTypeInfo)
    
    val offsetInt = sortOffset.asInstanceOf[RexLiteral].getValue.asInstanceOf[JBigDecimal].intValue
    
    new ProcTimeSortProcessFunctionOffset(
      offsetInt,
      inputCRowType,
      collectionRowComparator)

  }

  
  /**
   * Function creates [org.apache.flink.streaming.api.functions.ProcessFunction] for sorting 
   * elements based on proctime and potentially other fields
   * @param collationSort The Sort collation list
   * @param sortOffset The offset indicator. null value indicates only fetch
   * @param inputType input row type
   * @param inputTypeInfo input type information
   * @param execCfg table environment execution configuration
   * @return org.apache.flink.streaming.api.functions.ProcessFunction
   */
  private[flink] def createProcTimeSortFunctionRetractionOffsetFetch(
    collationSort: RelCollation,
    sortOffset: RexNode,
    sortFetch: RexNode,
    inputType: RelDataType,
    inputTypeInfo: TypeInformation[Row],
    execCfg: ExecutionConfig): ProcessFunction[CRow, CRow] = {

    val keySortFields = getSortFieldIndexList(collationSort)
    //drop time from comparison
    val keyIndexesNoTime = keySortFields.slice(1, keySortFields.size)
    val booleanOrderings = getSortFieldDirectionBooleanList(collationSort)
    val booleanDirectionsNoTime = booleanOrderings.slice(1, booleanOrderings.size)
    
    val fieldComps = createFieldComparators(
      inputType, 
      keyIndexesNoTime, 
      booleanDirectionsNoTime,
      execCfg)
    val fieldCompsRefs = fieldComps.asInstanceOf[Array[TypeComparator[AnyRef]]]
    
    val rowComp = new RowComparator(
       inputType.getFieldCount,
       keyIndexesNoTime,
       fieldCompsRefs,
       new Array[TypeSerializer[AnyRef]](0), //used only for object comparisons
       booleanDirectionsNoTime)
      
    val collectionRowComparator = new CollectionRowComparator(rowComp)
    
    val inputCRowType = CRowTypeInfo(inputTypeInfo)
    
    val offsetInt = if(sortOffset != null) {
      sortOffset.asInstanceOf[RexLiteral].getValue.asInstanceOf[JBigDecimal].intValue
    } else {
      0
    }
    val fetchInt = sortFetch.asInstanceOf[RexLiteral].getValue.asInstanceOf[JBigDecimal].intValue
    
    new ProcTimeSortProcessFunctionOffsetFetch(
      offsetInt,
      fetchInt,
      inputCRowType,
      collectionRowComparator)

  }
  
  
  /**
   * Function creates [org.apache.flink.streaming.api.functions.ProcessFunction] for sorting 
   * elements based on proctime alone and selecting the offset
   * @param sortOffset The offset indicator
   * @param inputTypeInfo input type information
   * @return org.apache.flink.streaming.api.functions.ProcessFunction
   */
  private[flink] def createIdentifyProcTimeSortFunctionRetractionOffset(
    sortOffset: RexNode,
    inputTypeInfo: TypeInformation[Row]): ProcessFunction[CRow, CRow] = {

    val inputCRowType = CRowTypeInfo(inputTypeInfo)
    
    val offsetInt = sortOffset.asInstanceOf[RexLiteral].getValue.asInstanceOf[JBigDecimal].intValue
    
    new ProcTimeIdentitySortProcessFunctionOffset(
      offsetInt,
      inputCRowType)

  }
  
  
  /**
   * Function creates [org.apache.flink.streaming.api.functions.ProcessFunction] for sorting 
   * elements based on proctime alone and selecting the offset
   * @param sortOffset The offset indicator. null value indicates only fetch
   * @param sortFetch The offset indicator
   * @param inputTypeInfo input type information
   * @return org.apache.flink.streaming.api.functions.ProcessFunction
   */
  private[flink] def createIdentifyProcTimeSortFunctionRetractionOffsetFetch(
    sortOffset: RexNode,
    sortFetch: RexNode,
    inputTypeInfo: TypeInformation[Row]): ProcessFunction[CRow, CRow] = {

    val inputCRowType = CRowTypeInfo(inputTypeInfo)
    
    val offsetInt = if(sortOffset != null) {
      sortOffset.asInstanceOf[RexLiteral].getValue.asInstanceOf[JBigDecimal].intValue
    } else {
      0
    }
    val fetchInt = sortFetch.asInstanceOf[RexLiteral].getValue.asInstanceOf[JBigDecimal].intValue
    
    new ProcTimeIdentitySortProcessFunctionOffsetFetch(
      offsetInt,
      fetchInt,
      inputCRowType)

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
      booleanOrdering: Array[Boolean],
      execConfig: ExecutionConfig): Array[TypeComparator[_]] = {
    
    for ((k, o) <- keyIndex.zip(booleanOrdering)) yield {
      FlinkTypeFactory.toTypeInfo(inputType.getFieldList.get(k).getType) match {
        case a: AtomicType[_] => a.createComparator(o, execConfig)
        case x: TypeInformation[_] =>  
          throw new TableException(s"Unsupported field type $x to sort on.")
      }
    }
    
  }
  
 
  /**
   * Returns the array of indexes for the fields on which the sort is done
   * @param collationSort The Sort collation list
   * @return [[Array[Int]]] The array containing the indexes of the comparison fields
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
  def getSortFieldDirectionBooleanList(collationSort: RelCollation): Array[Boolean] = {
    var i = 0
    val keyFields = collationSort.getFieldCollations
    val keySortDirection = new Array[Boolean](keyFields.size())
    while (i < keyFields.size()) {
      keySortDirection(i) = collationSort.getFieldCollations.get(i).direction match {
        case Direction.ASCENDING => true
        case Direction.DESCENDING => false
        case _ =>  throw new TableException("SQL/Table does not support such sorting")
      }  
      i += 1
    }
    keySortDirection
  }
  
  
   /**
   * Function returns the direction type of the time in order clause. 
   * @param collationSort The Sort collation list of objects
   * @return [org.apache.calcite.rel.RelFieldCollation.Direction] The time reference
   */
  def getTimeDirection(collationSort: RelCollation): Direction = {
    collationSort.getFieldCollations.get(0).direction
  }
  
  
   /**
   * Function returns the time type in order clause. 
   * Expectation is that it is the primary sort field
   * @param collationSort The Sort collation list
   * @param rowType The data type of the input
   * @return [org.apache.calcite.rel.type.RelDataType] The time type
   */
  def getTimeType(collationSort: RelCollation, rowType: RelDataType): RelDataType = {

    //need to identify time between others ordering fields
    val ind = collationSort.getFieldCollations.get(0).getFieldIndex
    rowType.getFieldList.get(ind).getType
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
private[flink] class IdentityCRowMap extends MapFunction[CRow,CRow] {
   override def map(value:CRow):CRow ={
     value
   }
 }
