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
import org.apache.calcite.rel.logical.LogicalAggregate
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

class DataStreamNonWindowAggregate(
  calc: LogicalAggregate,
  cluster: RelOptCluster,
  traitSet: RelTraitSet,
  inputNode: RelNode,
  rowType: RelDataType,
  description: String)
    extends SingleRel(cluster, traitSet, inputNode) with DataStreamRel {

  override def deriveRowType(): RelDataType = rowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamNonWindowAggregate(
      calc,
      cluster,
      traitSet,
      inputs.get(0),
      rowType,
      description + calc.getId())
  }

  override def toString: String = {
    s"Aggregate($calc)"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("aggregate", calc)
      .item("input", inputNode)
  }

  override def translateToPlan(tableEnv: StreamTableEnvironment): DataStream[Row] = {

    val inputDS = getInput.asInstanceOf[DataStreamRel].translateToPlan(tableEnv)

    //enable to extend for other types of aggregates that will not be implemented in a window
    calc.getAggCallList.get(0).getAggregation match {
      case _: SqlSingleValueAggFunction =>
        createLastSingleValueAggregate(inputDS)
      case _ =>
        throw new TableException("SQL/Table does not support such aggregates type")
    }
  }

  /**
   * Create Aggregator for keeping the last value that was seen
   */
  def createLastSingleValueAggregate(
    inputDS: DataStream[Row]): DataStream[Row] = {

    
     // get the output types
    val rowTypeInfo = FlinkTypeFactory.toInternalRowTypeInfo(getRowType).asInstanceOf[RowTypeInfo]
    
    inputDS.flatMap(new RichFlatMapFunction[Row, Row] {

      @transient var state: ValueState[Row] = _

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        state = getRuntimeContext.getState(
          new ValueStateDescriptor[Row]("state-last-element", rowTypeInfo))

      }

      override def flatMap(value: Row, out: Collector[Row]): Unit = {
        state.update(value)
        out.collect(value)
      }
    })
  }

}


