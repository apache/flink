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
import org.apache.calcite.rel.{ RelNode, RelWriter, BiRel }
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
import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.calcite.rel.core.JoinRelType
import org.apache.flink.table.api.TableException
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult
import org.apache.flink.streaming.api.datastream.CoGroupedStreams.TaggedUnion
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.evictors.Evictor.EvictorContext
import java.lang.Iterable
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue
import org.apache.flink.api.common.functions.RichFlatJoinFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.util.Collector

class DataStreamJoin(
  calc: LogicalJoin,
  cluster: RelOptCluster,
  traitSet: RelTraitSet,
  inputLeft: RelNode,
  inputRight: RelNode,
  rowType: RelDataType,
  description: String)
    extends BiRel(cluster, traitSet, inputLeft, inputRight) with DataStreamRel {

  override def deriveRowType(): RelDataType = rowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamJoin(
      calc,
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      rowType,
      description + calc.getId())
  }

  override def toString: String = {
    s"Join(${
      if (!calc.getCondition.isAlwaysTrue()) {
        s"condition: (${calc.getCondition}), "
      } else {
        ""
      }
    }left: ($inputLeft), right($inputRight))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("condition", calc.getCondition, !calc.getCondition.isAlwaysTrue())
      .item("join", calc)
      .item("left", inputLeft)
      .item("right", inputRight)
  }

  override def translateToPlan(tableEnv: StreamTableEnvironment): DataStream[Row] = {

    val inputDSLeft = inputLeft.asInstanceOf[DataStreamRel].translateToPlan(tableEnv)
    val inputDSRight = inputRight.asInstanceOf[DataStreamRel].translateToPlan(tableEnv)

    //define the setup for various types of joins to be supported
    (calc.getCondition.isAlwaysTrue(), calc.getJoinType) match {
      case (true, JoinRelType.LEFT) =>
        createInnerQueryJoin(inputDSLeft, inputDSRight)
      case (_, _) =>
        throw new TableException("Table does not support this type of JOIN.")
    }

    null
  }

  def createInnerQueryJoin(
    inputDSLeft: DataStream[Row], inputDSRight: DataStream[Row]): DataStream[Row] = {

    // get the output types
    val rowTypeInfo = FlinkTypeFactory.toInternalRowTypeInfo(getRowType).asInstanceOf[RowTypeInfo]

    val result = inputDSLeft.join(inputDSRight)
      .where(new EmptyKeySelector()).equalTo(new EmptyKeySelector())
      .window(GlobalWindows.create())
      .trigger(new ProcTimeLeftJoinTrigger())
      .evictor(new FullEvictor())
      .apply(new JoinProcTimeForInnerQuerry(rowTypeInfo))

    null
  }

}

class EmptyKeySelector extends KeySelector[Row, Integer] {
  override def getKey(value: Row): Integer = {
    0
  }
}

class ProcTimeLeftJoinTrigger extends Trigger[Object, GlobalWindow] with Serializable {

  /*
   * Check if element comes from the left stream case in which we should fire
   */
  override def onElement(element: Object,
    timestamp: Long,
    window: GlobalWindow, ctx: TriggerContext): TriggerResult = {
    element match {
      case elementPair: TaggedUnion[Row, Row] => {
        if (elementPair.isOne()) {
          TriggerResult.FIRE_AND_PURGE
        } else {
          TriggerResult.CONTINUE
        }
      }
      case _ => TriggerResult.CONTINUE
    }
  }

  /*
   * We operate on processing time so we move on each element
   */
  override def onProcessingTime(timestamp: Long, window: 
      GlobalWindow, ctx: TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  /*
   * We operate on processing time so we move on each element
   */
  override def onEventTime(timestamp: Long,
    window: GlobalWindow,
    ctx: TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: GlobalWindow, ctx: TriggerContext): Unit = {

  }
}

class FullEvictor extends Evictor[Object, GlobalWindow] {
  override def evictBefore(x1: Iterable[TimestampedValue[Object]],
    size: Int, window: GlobalWindow, ctx: EvictorContext): Unit = {
  }

  override def evictAfter(x1: Iterable[TimestampedValue[Object]],
    size: Int, window: GlobalWindow, ctx: EvictorContext): Unit = {
    val iter = x1.iterator()
    while (iter.hasNext()) {
      iter.remove()
    }
  }
}

class JoinProcTimeForInnerQuerry(
    private val rowTypeInfo: RowTypeInfo) extends RichFlatJoinFunction[Row, Row, Row] {

  private var lastValueRight: ValueState[Row] = _

  override def open(configuration: Configuration): Unit = {
    val stateDescriptor: ValueStateDescriptor[Row] =
      new ValueStateDescriptor[Row]("overState", rowTypeInfo)
    lastValueRight = getRuntimeContext.getState(stateDescriptor)
  }
  override def join(first: Row, second: Row, out: Collector[Row]): Unit = {

    var secondarity = 0
    var secondR = second
    if (second != null) {
      lastValueRight.update(second)
      secondarity = second.getArity
    } else {
      secondR = lastValueRight.value()
      if (secondR != null) {
        secondarity = secondR.getArity
      }
    }

    if (first != null) {
      val outrez = new Row(first.getArity + secondarity)
      var i = 0
      while (i < first.getArity) {
        outrez.setField(i, first.getField(i))
        i += 1
      }
      i = 0
      while (i < secondarity) {
        outrez.setField(i, secondR.getField(i))
        i += 1
      }
      out.collect(outrez)
    }
  }
}


