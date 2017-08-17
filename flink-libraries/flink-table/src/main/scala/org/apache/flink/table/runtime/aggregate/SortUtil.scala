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

import org.apache.calcite.rel.`type`._
import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rel.RelFieldCollation
import org.apache.calcite.rel.RelFieldCollation.Direction

import org.apache.flink.types.Row
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.api.common.typeutils.TypeComparator
import org.apache.flink.api.java.typeutils.runtime.RowComparator
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.typeinfo.AtomicType
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.calcite.rex.{RexLiteral, RexNode}
import java.math.{BigDecimal=>JBigDecimal}

import java.util.Comparator

import scala.collection.JavaConverters._

/**
 * Class represents a collection of helper methods to build the sort logic.
 * It encapsulates as well the implementation for ordering and generic interfaces
 */
object SortUtil {

  
   /**
   * Creates a row comparator if defined 
   *
   * @param collationSort The list of sort collations.
   * @param inputType The row type of the input.
   * @param execCfg Execution configuration to configure comparators.
   * @return An object that contains the comparator if defined
   */
  private[flink] def createWrappedRowComparator(
       collationSort: RelCollation,
       inputType: RelDataType,
       execCfg: ExecutionConfig): Option[CollectionRowComparator] = {
    
    val collectionRowComparator = if (collationSort.getFieldCollations.size() > 1) {

      val rowComp = createRowComparator(
        inputType,
        collationSort.getFieldCollations.asScala.tail, // strip off time collation
        execCfg)

      Some(new CollectionRowComparator(rowComp))
    } else {
      None
    }
     
    collectionRowComparator
  }
  
   /**
   * Creates a collection row comparator 
   *
   * @param collationSort The list of sort collations.
   * @param inputType The row type of the input.
   * @param execCfg Execution configuration to configure comparators.
   * @return An object that contains the comparator
   */
  private[flink] def createCollectionRowComparator(
       collationSort: RelCollation,
       inputType: RelDataType,
       execCfg: ExecutionConfig): CollectionRowComparator = {
    
     val rowComp = createRowComparator(
      inputType,
      collationSort.getFieldCollations.asScala.tail, // strip off time collation
      execCfg)

     new CollectionRowComparator(rowComp)
  }
  
  /**
   * Function creates a normalized value for offset with value 0 if offset is not defined
   * @param sortOffset The offset indicator
   * @return offset
   */
  private[flink] def getNormalizedOffset(sortOffset: RexNode): Int = {
    if(sortOffset != null) {
      sortOffset.asInstanceOf[RexLiteral].getValue.asInstanceOf[JBigDecimal].intValue
    } else {
      0
    }
  }
  
  
  /**
   * Function creates a normalized value for fetch with value -1 if fetch is not defined
   * @param sortOffset The fetch indicator
   * @return fetch
   */
  private[flink] def getNormalizedFetch(sortFetch: RexNode): Int = {
    if(sortFetch != null) {
      sortFetch.asInstanceOf[RexLiteral].getValue.asInstanceOf[JBigDecimal].intValue
    } else {
      -1
    }
  }
  
  
  /**
   * Creates a RowComparator for the provided field collations and input type.
   *
   * @param inputType the row type of the input.
   * @param fieldCollations the field collations
   * @param execConfig the execution configuration.
    *
   * @return A RowComparator for the provided sort collations and input type.
   */
  private def createRowComparator(
      inputType: RelDataType,
      fieldCollations: Seq[RelFieldCollation],
      execConfig: ExecutionConfig): RowComparator = {

    val sortFields = fieldCollations.map(_.getFieldIndex)
    val sortDirections = fieldCollations.map(_.direction).map {
      case Direction.ASCENDING => true
      case Direction.DESCENDING => false
      case _ =>  throw new TableException("SQL/Table does not support such sorting")
    }

    val fieldComps = for ((k, o) <- sortFields.zip(sortDirections)) yield {
      FlinkTypeFactory.toTypeInfo(inputType.getFieldList.get(k).getType) match {
        case a: AtomicType[AnyRef] => a.createComparator(o, execConfig)
        case x: TypeInformation[_] =>  
          throw new TableException(s"Unsupported field type $x to sort on.")
      }
    }

    new RowComparator(
      new RowSchema(inputType).physicalArity,
      sortFields.toArray,
      fieldComps.toArray,
      new Array[TypeSerializer[AnyRef]](0), // not required because we only compare objects.
      sortDirections.toArray)
    
  }
 
  /**
   * Returns the direction of the first sort field.
   *
   * @param collationSort The list of sort collations.
   * @return The direction of the first sort field.
   */
  def getFirstSortDirection(collationSort: RelCollation): Direction = {
    collationSort.getFieldCollations.get(0).direction
  }
  
  /**
   * Returns the first sort field.
   *
   * @param collationSort The list of sort collations.
   * @param rowType The row type of the input.
   * @return The first sort field.
   */
  def getFirstSortField(collationSort: RelCollation, rowType: RelDataType): RelDataTypeField = {
    val idx = collationSort.getFieldCollations.get(0).getFieldIndex
    rowType.getFieldList.get(idx)
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
