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

import org.apache.calcite.plan.{ RelOptCluster, RelTraitSet }
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{ RelNode, RelWriter, SingleRel }
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.datastream.{ AllWindowedStream, DataStream, KeyedStream, WindowedStream }
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{ Window => DataStreamWindow }
import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.expressions._
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.plan.nodes.CommonAggregate
import org.apache.flink.table.plan.nodes.datastream.DataStreamAggregate._
import org.apache.flink.table.runtime.aggregate.AggregateUtil._
import org.apache.flink.table.runtime.aggregate._
import org.apache.flink.table.typeutils.TypeCheckUtils.isTimeInterval
import org.apache.flink.table.typeutils.{ RowIntervalTypeInfo, TimeIntervalTypeInfo }
import org.apache.flink.types.Row
import org.apache.calcite.rel.logical.LogicalSort
import org.apache.calcite.sql.SqlAggFunction
import org.apache.flink.table.plan.nodes.datastream.DataStreamRel
import org.apache.flink.table.api.TableException
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.functions.ProcTimeType
import org.apache.flink.table.functions.RowTimeType
import org.apache.calcite.rel.core.Sort
import org.apache.flink.api.java.functions.NullByteKeySelector
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.flink.table.runtime.aggregate.SortUtil._

/**
  * Flink RelNode which matches along with Sort Rule.
  *
  */
class DataStreamSort(
  sort: LogicalSort,
  cluster: RelOptCluster,
  traitSet: RelTraitSet,
  inputNode: RelNode,
  rowRelDataType: RelDataType,
  inputType: RelDataType,
  description: String)
    extends SingleRel(cluster, traitSet, inputNode) with DataStreamRel {

  override def deriveRowType(): RelDataType = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamSort(
      sort,
      cluster,
      traitSet,
      inputs.get(0),
      rowRelDataType,
      inputType,
      description + sort.getId())
  }

  override def toString: String = {
    s"Sort($sort)" +
      " on fields: (${sort.collation.getFieldCollations})"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("aggregate", sort)
      .item("sort fields",sort.collation.getFieldCollations)
      .itemIf("offset", sort.offset, sort.offset!=null)
      .itemIf("fetch", sort.fetch, sort.fetch!=null)
      .item("input", inputNode)
  }

  override def translateToPlan(tableEnv: StreamTableEnvironment): DataStream[Row] = {

    val inputDS = getInput.asInstanceOf[DataStreamRel].translateToPlan(tableEnv)
    
    //need to identify time between others order fields. Time needs to be first sort element
    val timeType = SortUtil.getTimeType(sort,inputType)
    
    //time ordering needs to be ascending
    if (SortUtil.getTimeDirection(sort) != Direction.ASCENDING) {
      throw new TableException("SQL/Table supports only ascending time ordering")
    }
      
     
    val (offset,fetch) = (sort.offset,sort.fetch)
    
    //enable to extend for other types of aggregates that will not be implemented in a window
    timeType match {
        case _: ProcTimeType =>
            (offset,fetch) match {
              case (o:Any,f:Any)  => null             // offset and fetch needs retraction
              case (_,f:Any) => null                  // offset needs retraction
              case (o:Any,_) => null                  // fetch needs retraction
              case _ => createSortProcTime(inputDS)   //sort can be done with/without retraction
            }
        case _: RowTimeType =>
          throw new TableException("SQL/Table does not support sort on row time")
        case _ =>
          throw new TableException("SQL/Table needs to have sort on time as first sort element")
    }
    
  }

  /**
   * Create Sort logic based on processing time
   */
  def createSortProcTime(
    inputDS: DataStream[Row]): DataStream[Row] = {

          
    // get the output types
    //Sort does not do project.= Hence it will output also the ordering proctime field
    //[TODO]Do we need to drop some of the ordering fields? (implement a projection logic?
    val rowTypeInfo = FlinkTypeFactory.toInternalRowTypeInfo(getRowType).asInstanceOf[RowTypeInfo]
    
    //if the order has secondary sorting fields in addition to the proctime
    if( SortUtil.getSortFieldIndexList(sort).size > 1) {
    
      val processFunction = SortUtil.createProcTimeSortFunction(sort,inputType)
      
      inputDS
            .keyBy(new NullByteKeySelector[Row])
            .process(processFunction).setParallelism(1).setMaxParallelism(1)
            .returns(rowTypeInfo)
            .asInstanceOf[DataStream[Row]]
    } else {
      //if the order is done only on proctime we only need to forward the elements
        inputDS.keyBy(new NullByteKeySelector[Row])
          .map(new IdentityRowMap())
          .setParallelism(1).setMaxParallelism(1)
          .returns(rowTypeInfo)
          .asInstanceOf[DataStream[Row]]
    }   
  }
  

}


