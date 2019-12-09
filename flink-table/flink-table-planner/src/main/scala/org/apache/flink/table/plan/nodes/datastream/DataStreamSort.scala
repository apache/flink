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

package org.apache.flink.table.plan.nodes.datastream

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rel.{RelCollation, RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.java.functions.NullByteKeySelector
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamQueryConfig, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.CommonSort
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.planner.StreamPlanner
import org.apache.flink.table.runtime.aggregate._
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}

/**
 * Flink RelNode which matches along with Sort Rule.
 *
 */
class DataStreamSort(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    inputSchema: RowSchema,
    schema: RowSchema,
    sortCollation: RelCollation,
    sortOffset: RexNode,
    sortFetch: RexNode,
    description: String)
  extends Sort(cluster, traitSet, inputNode, sortCollation, sortOffset, sortFetch)
  with CommonSort
  with DataStreamRel {

  override def deriveRowType(): RelDataType = schema.relDataType

  override def copy(
    traitSet: RelTraitSet,
    input: RelNode,
    newCollation: RelCollation,
    offset: RexNode,
    fetch: RexNode): Sort = {
    
    new DataStreamSort(
      cluster,
      traitSet,
      input,
      inputSchema,
      schema,
      newCollation,
      offset,
      fetch,
      description)
  }

  override def toString: String = {
    sortToString(schema.relDataType, sortCollation, sortOffset, sortFetch)
  }
  
  override def explainTerms(pw: RelWriter) : RelWriter = {
    sortExplainTerms(
      pw.input("input", getInput()),
      schema.relDataType,
      sortCollation, 
      sortOffset, 
      sortFetch)
  }

  override def translateToPlan(
      planner: StreamPlanner,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {
    
    val inputDS = input.asInstanceOf[DataStreamRel].translateToPlan(planner, queryConfig)
    
    // need to identify time between others order fields. Time needs to be first sort element
    val timeType = SortUtil.getFirstSortField(sortCollation, schema.relDataType).getType
    
    // time ordering needs to be ascending
    if (SortUtil.getFirstSortDirection(sortCollation) != Direction.ASCENDING) {
      throw new TableException("Primary sort order of a streaming table must be ascending on time.")
    }
    
    val execCfg = planner.getExecutionEnvironment.getConfig
      
    // enable to extend for other types of aggregates that will not be implemented in a window
    timeType match {
        case _ if FlinkTypeFactory.isProctimeIndicatorType(timeType)  =>
            (sortOffset, sortFetch) match {
              case (_: RexNode, _: RexNode)  => // offset and fetch needs retraction
                throw new TableException(
                  "Streaming tables do not support sort with offset and fetch.")
              case (_, _: RexNode) => // offset needs retraction
                throw new TableException("Streaming tables do not support sort with fetch.")
              case (_: RexNode, _) =>  // fetch needs retraction
                throw new TableException("Streaming tables do not support sort with offset.")
              case _ => createSortProcTime(inputDS, execCfg)
            }
        case _ if FlinkTypeFactory.isRowtimeIndicatorType(timeType) =>
            (sortOffset, sortFetch) match {
              case (_: RexNode, _: RexNode)  => // offset and fetch needs retraction
                throw new TableException(
                  "Streaming tables do not support sort with offset and fetch")
              case (_, _: RexNode) => // offset needs retraction
                throw new TableException("Streaming tables do not support sort with fetch")
              case (_: RexNode, _) =>  // fetch needs retraction
                throw new TableException("Streaming tables do not support sort with offset")
              case _ => createSortRowTime(inputDS, execCfg)
            }
        case _ =>
          throw new TableException(
            "Primary sort order of a streaming table must be ascending on time.")
    }
    
  }

  /**
   * Create Sort logic based on processing time
   */
  def createSortProcTime(
    inputDS: DataStream[CRow],
    execCfg: ExecutionConfig): DataStream[CRow] = {

   val returnTypeInfo = CRowTypeInfo(schema.typeInfo)
    
    // if the order has secondary sorting fields in addition to the proctime
    if (sortCollation.getFieldCollations.size() > 1) {
    
      val KeyedProcessFunction = SortUtil.createProcTimeSortFunction(
        sortCollation,
        inputSchema.relDataType,
        inputSchema.typeInfo,
        execCfg)
      
      inputDS.keyBy(new NullByteKeySelector[CRow])
        .process(KeyedProcessFunction).setParallelism(1).setMaxParallelism(1)
        .returns(returnTypeInfo)
        .asInstanceOf[DataStream[CRow]]
    } else {
      // if the order is done only on proctime we only need to forward the elements
      inputDS
        .map(new IdentityCRowMap())
        .setParallelism(1).setMaxParallelism(1)
        .returns(returnTypeInfo)
        .asInstanceOf[DataStream[CRow]]
    }   
  }
  
  /**
   * Create Sort logic based on row time
   */
  def createSortRowTime(
    inputDS: DataStream[CRow],
    execCfg: ExecutionConfig): DataStream[CRow] = {

    val returnTypeInfo = CRowTypeInfo(schema.typeInfo)
       
    val keyedProcessFunction = SortUtil.createRowTimeSortFunction(
      sortCollation,
      inputSchema.relDataType,
      inputSchema.typeInfo,
      execCfg)

    inputDS.keyBy(new NullByteKeySelector[CRow])
      .process(keyedProcessFunction).setParallelism(1).setMaxParallelism(1)
      .returns(returnTypeInfo)
      .asInstanceOf[DataStream[CRow]]
       
  }

}
