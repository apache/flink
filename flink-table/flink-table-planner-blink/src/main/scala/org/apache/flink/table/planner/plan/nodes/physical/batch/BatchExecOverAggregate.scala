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

package org.apache.flink.table.planner.plan.nodes.physical.batch

import org.apache.flink.api.dag.Transformation
import org.apache.flink.configuration.MemorySize
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.binary.BinaryRowData
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.CodeGeneratorContext
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator
import org.apache.flink.table.planner.codegen.over.{MultiFieldRangeBoundComparatorCodeGenerator, RangeBoundComparatorCodeGenerator}
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil
import org.apache.flink.table.planner.plan.utils.AggregateUtil.transformToBatchAggregateInfoList
import org.apache.flink.table.planner.plan.utils.OverAggregateUtil
import org.apache.flink.table.planner.plan.utils.OverAggregateUtil.getLongBoundary
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator
import org.apache.flink.table.runtime.operators.over.frame.OffsetOverFrame.CalcOffsetFunc
import org.apache.flink.table.runtime.operators.over.frame._
import org.apache.flink.table.runtime.operators.over.{BufferDataOverWindowOperator, NonBufferOverWindowOperator}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.LogicalTypeRoot.{BIGINT, INTEGER, SMALLINT}

import org.apache.calcite.plan._
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{AggregateCall, Window}
import org.apache.calcite.rex.RexWindowBound
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.tools.RelBuilder

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode for sort-based over [[Window]] aggregate.
  */
class BatchExecOverAggregate(
    cluster: RelOptCluster,
    relBuilder: RelBuilder,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    inputRowType: RelDataType,
    grouping: Array[Int],
    orderKeyIndices: Array[Int],
    orders: Array[Boolean],
    nullIsLasts: Array[Boolean],
    windowGroupToAggCallToAggFunction: Seq[
      (Window.Group, Seq[(AggregateCall, UserDefinedFunction)])],
    logicWindow: Window)
  extends BatchExecOverAggregateBase(
    cluster,
    relBuilder,
    traitSet,
    inputRel,
    outputRowType,
    inputRowType,
    grouping,
    orderKeyIndices,
    orders,
    nullIsLasts,
    windowGroupToAggCallToAggFunction,
    logicWindow) {

  lazy val needBufferData: Boolean = {
    modeToGroupToAggCallToAggFunction.exists {
      case (mode, windowGroup, _) =>
        mode match {
          case OverWindowMode.Insensitive => false
          case OverWindowMode.Row
            if (windowGroup.lowerBound.isCurrentRow && windowGroup.upperBound.isCurrentRow) ||
                (windowGroup.lowerBound.isUnbounded && windowGroup.upperBound.isCurrentRow) =>
            false
          case _ => true
        }
    }
  }

  override def deriveRowType: RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new BatchExecOverAggregate(
      cluster,
      relBuilder,
      traitSet,
      inputs.get(0),
      outputRowType,
      inputRowType,
      grouping,
      orderKeyIndices,
      orders,
      nullIsLasts,
      windowGroupToAggCallToAggFunction,
      logicWindow)
  }

  override protected def translateToPlanInternal(
      planner: BatchPlanner): Transformation[RowData] = {
    val config = planner.getTableConfig
    val input = getInputNodes.get(0).translateToPlan(planner)
        .asInstanceOf[Transformation[RowData]]
    val outputType = FlinkTypeFactory.toLogicalRowType(getRowType)

    //The generated sort is used for generating the comparator among partitions.
    //So here not care the ASC or DESC for the grouping fields.
    //TODO just replace comparator to equaliser
    val collation = grouping.map(_ => (true, false))
    val genComparator =  ComparatorCodeGenerator.gen(
      config,
      "SortComparator",
      grouping,
      grouping.map(inputType.getTypeAt),
      collation.map(_._1),
      collation.map(_._2))

    var managedMemory: Long = 0L
    val operator = if (!needBufferData) {
      //operator needn't cache data
      val aggHandlers = modeToGroupToAggCallToAggFunction.map { case (_, _, aggCallToAggFunction) =>
        val aggInfoList = transformToBatchAggregateInfoList(
          aggCallToAggFunction.map(_._1),
          // use aggInputType which considers constants as input instead of inputType
          inputTypeWithConstants,
          orderKeyIndices)
        val codeGenCtx = CodeGeneratorContext(config)
        val generator = new AggsHandlerCodeGenerator(
          codeGenCtx,
          relBuilder,
          inputType.getChildren,
          copyInputField = false)
        // over agg code gen must pass the constants
        generator
          .needAccumulate()
          .withConstants(constants)
          .generateAggsHandler("BoundedOverAggregateHelper", aggInfoList)
      }.toArray

      val resetAccumulators =
        modeToGroupToAggCallToAggFunction.map { case (mode, windowGroup, _) =>
          mode == OverWindowMode.Row &&
              windowGroup.lowerBound.isCurrentRow &&
              windowGroup.upperBound.isCurrentRow
        }.toArray
      new NonBufferOverWindowOperator(aggHandlers, genComparator, resetAccumulators)
    } else {
      val windowFrames = createOverWindowFrames(config)
      managedMemory = MemorySize.parse(config.getConfiguration.getString(
        ExecutionConfigOptions.TABLE_EXEC_RESOURCE_EXTERNAL_BUFFER_MEMORY)).getBytes
      new BufferDataOverWindowOperator(
        windowFrames,
        genComparator,
        inputType.getChildren.forall(t => BinaryRowData.isInFixedLengthPart(t)))
    }
    ExecNodeUtil.createOneInputTransformation(
      input,
      getRelDetailedDescription,
      SimpleOperatorFactory.of(operator),
      InternalTypeInfo.of(outputType),
      input.getParallelism,
      managedMemory)
  }

  def createOverWindowFrames(config: TableConfig): Array[OverWindowFrame] = {

    modeToGroupToAggCallToAggFunction.flatMap { case (mode, windowGroup, aggCallToAggFunction) =>
      mode match {
        case OverWindowMode.Offset =>
          //Split the aggCalls to different over window frame because the length of window frame
          //lies on the offset of the window frame.
          aggCallToAggFunction.map { case (aggCall, _) =>
            val aggInfoList = transformToBatchAggregateInfoList(
              Seq(aggCall),
              inputTypeWithConstants,
              orderKeyIndices,
              Array[Boolean](true) /* needRetraction = true, See LeadLagAggFunction */)

            val generator = new AggsHandlerCodeGenerator(
              CodeGeneratorContext(config),
              relBuilder,
              inputType.getChildren,
              copyInputField = false)

            // over agg code gen must pass the constants
            val genAggsHandler = generator
              .needAccumulate()
              .needRetract()
              .withConstants(constants)
              .generateAggsHandler("BoundedOverAggregateHelper", aggInfoList)

            // LEAD is behind the currentRow, so we need plus offset.
            // LAG is in front of the currentRow, so we need minus offset.
            val flag = if (aggCall.getAggregation.kind == SqlKind.LEAD) 1 else -1

            // Either long or function.
            val (offset, calcOffsetFunc) = {
              // LEAD ( expression [, offset [, default] ] )
              // LAG ( expression [, offset [, default] ] )
              // The second arg mean the offset arg index for leag/lag function, default is 1.
              if (aggCall.getArgList.length >= 2) {
                val constantIndex =
                  aggCall.getArgList.get(1) - OverAggregateUtil.calcOriginInputRows(logicWindow)
                if (constantIndex < 0) {
                  val rowIndex = aggCall.getArgList.get(1)
                  val func = inputType.getTypeAt(rowIndex).getTypeRoot match {
                    case BIGINT =>
                      new CalcOffsetFunc {
                        override def calc(value: RowData): Long = {
                          value.getLong(rowIndex) * flag
                        }
                      }
                    case INTEGER =>
                      new CalcOffsetFunc {
                        override def calc(value: RowData): Long = {
                          value.getInt(rowIndex).toLong * flag
                        }
                      }
                    case SMALLINT =>
                      new CalcOffsetFunc {
                        override def calc(value: RowData): Long = {
                          value.getShort(rowIndex).toLong * flag
                        }
                      }
                    case _ => throw new RuntimeException(
                      "The column type must be in long/int/short.")
                  }
                  (null, func)
                } else {
                  val constantOffset = logicWindow.constants.get(
                    constantIndex).getValueAs(classOf[java.lang.Long])
                  (constantOffset * flag, null)
                }
              } else {
                (1L * flag, null)
              }
            }
            new OffsetOverFrame(
              genAggsHandler,
              offset.asInstanceOf[java.lang.Long],
              calcOffsetFunc.asInstanceOf[CalcOffsetFunc])
          }

        case _ =>
          val aggInfoList = transformToBatchAggregateInfoList(
            aggCallToAggFunction.map(_._1),
            //use aggInputType which considers constants as input instead of inputSchema.relDataType
            inputTypeWithConstants,
            orderKeyIndices)
          val codeGenCtx = CodeGeneratorContext(config)
          val generator = new AggsHandlerCodeGenerator(
            codeGenCtx,
            relBuilder,
            inputType.getChildren,
            copyInputField = false)

          // over agg code gen must pass the constants
          val genAggsHandler = generator
              .needAccumulate()
              .withConstants(constants)
              .generateAggsHandler("BoundedOverAggregateHelper", aggInfoList)

          val logicalInputType = inputType
          val logicalValueType = generator.valueType
          mode match {
            case OverWindowMode.Range if isUnboundedWindow(windowGroup) =>
              Array(new UnboundedOverWindowFrame(genAggsHandler, logicalValueType))

            case OverWindowMode.Range if isUnboundedPrecedingWindow(windowGroup) =>
              val genBoundComparator = createBoundComparator(
                config, windowGroup, windowGroup.upperBound, isLowerBound = false)
              Array(new RangeUnboundedPrecedingOverFrame(genAggsHandler, genBoundComparator))

            case OverWindowMode.Range if isUnboundedFollowingWindow(windowGroup) =>
              val genBoundComparator = createBoundComparator(
                config, windowGroup, windowGroup.lowerBound, isLowerBound = true)
              Array(new RangeUnboundedFollowingOverFrame(
                logicalValueType, genAggsHandler, genBoundComparator))

            case OverWindowMode.Range if isSlidingWindow(windowGroup) =>
              val genlBoundComparator = createBoundComparator(
                config, windowGroup, windowGroup.lowerBound, isLowerBound = true)
              val genrBoundComparator = createBoundComparator(
                config, windowGroup, windowGroup.upperBound, isLowerBound = false)
              Array(new RangeSlidingOverFrame(
                logicalInputType, logicalValueType,
                genAggsHandler, genlBoundComparator, genrBoundComparator))

            case OverWindowMode.Row if isUnboundedWindow(windowGroup) =>
              Array(new UnboundedOverWindowFrame(genAggsHandler, logicalValueType))

            case OverWindowMode.Row if isUnboundedPrecedingWindow(windowGroup) =>

              Array(new RowUnboundedPrecedingOverFrame(genAggsHandler,
                getLongBoundary(logicWindow, windowGroup.upperBound)))

            case OverWindowMode.Row if isUnboundedFollowingWindow(windowGroup) =>
              Array(new RowUnboundedFollowingOverFrame(
                logicalValueType,
                genAggsHandler,
                getLongBoundary(logicWindow, windowGroup.lowerBound)))

            case OverWindowMode.Row if isSlidingWindow(windowGroup) =>
              Array(new RowSlidingOverFrame(
                logicalInputType,
                logicalValueType,
                genAggsHandler,
                getLongBoundary(logicWindow, windowGroup.lowerBound),
                getLongBoundary(logicWindow, windowGroup.upperBound)))

            case OverWindowMode.Insensitive => Array(new InsensitiveOverFrame(genAggsHandler))
          }
      }
    }.toArray
  }

  private def createBoundComparator(
      config: TableConfig,
      windowGroup: Window.Group,
      windowBound: RexWindowBound,
      isLowerBound: Boolean): GeneratedRecordComparator = {
    val bound = OverAggregateUtil.getBoundary(logicWindow, windowBound)
    if (!windowBound.isCurrentRow) {
      //Range Window only support comparing based on a field.
      val sortKey = orderKeyIndices(0)
      new RangeBoundComparatorCodeGenerator(
        relBuilder,
        config,
        inputType,
        bound,
        sortKey,
        inputType.getTypeAt(sortKey),
        orders(0),
        isLowerBound).generateBoundComparator("RangeBoundComparator")
    } else {
      //if the bound is current row, then window support comparing based on multi fields.
      new MultiFieldRangeBoundComparatorCodeGenerator(
        config,
        inputType,
        orderKeyIndices,
        orderKeyIndices.map(inputType.getTypeAt),
        orders,
        nullIsLasts,
        isLowerBound).generateBoundComparator("MultiFieldRangeBoundComparator")
    }
  }
}
