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
import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow}
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.planner.CalcitePair
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.CodeGeneratorContext
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator
import org.apache.flink.table.planner.codegen.over.{MultiFieldRangeBoundComparatorCodeGenerator, RangeBoundComparatorCodeGenerator}
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.planner.plan.cost.{FlinkCost, FlinkCostFactory}
import org.apache.flink.table.planner.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.planner.plan.nodes.physical.batch.OverWindowMode.OverWindowMode
import org.apache.flink.table.planner.plan.nodes.resource.NodeResourceUtil
import org.apache.flink.table.planner.plan.rules.physical.batch.BatchExecJoinRuleBase
import org.apache.flink.table.planner.plan.utils.AggregateUtil.transformToBatchAggregateInfoList
import org.apache.flink.table.planner.plan.utils.OverAggregateUtil.getLongBoundary
import org.apache.flink.table.planner.plan.utils.{FlinkRelOptUtil, OverAggregateUtil, RelExplainUtil}
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator
import org.apache.flink.table.runtime.operators.over.frame.OffsetOverFrame.CalcOffsetFunc
import org.apache.flink.table.runtime.operators.over.frame.{InsensitiveOverFrame, OffsetOverFrame, OverWindowFrame, RangeSlidingOverFrame, RangeUnboundedFollowingOverFrame, RangeUnboundedPrecedingOverFrame, RowSlidingOverFrame, RowUnboundedFollowingOverFrame, RowUnboundedPrecedingOverFrame, UnboundedOverWindowFrame}
import org.apache.flink.table.runtime.operators.over.{BufferDataOverWindowOperator, NonBufferOverWindowOperator}
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo
import org.apache.flink.table.types.logical.LogicalTypeRoot.{BIGINT, INTEGER, SMALLINT}
import org.apache.calcite.plan._
import org.apache.calcite.rel.RelDistribution.Type._
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Window.Group
import org.apache.calcite.rel.core.{AggregateCall, Window}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexWindowBound
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.fun.SqlLeadLagAggFunction
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.ImmutableIntList
import org.apache.flink.configuration.MemorySize

import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

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
  extends SingleRel(cluster, traitSet, inputRel)
  with BatchPhysicalRel
  with BatchExecNode[BaseRow] {

  private lazy val modeToGroupToAggCallToAggFunction:
    Seq[(OverWindowMode, Window.Group, Seq[(AggregateCall, UserDefinedFunction)])] =
    splitOutOffsetOrInsensitiveGroup()

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

  private val constants = logicWindow.constants
  private val inputTypeWithConstants = {
    val constantTypes = constants.map(c => FlinkTypeFactory.toLogicalType(c.getType))
    val inputTypeNamesWithConstants =
      inputType.getFieldNames ++ constants.indices.map(i => "TMP" + i)
    val inputTypesWithConstants = inputType.getChildren ++ constantTypes
    cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
        .buildRelNodeRowType(inputTypeNamesWithConstants, inputTypesWithConstants)
  }

  lazy val aggregateCalls: Seq[AggregateCall] =
    windowGroupToAggCallToAggFunction.flatMap(_._2).map(_._1)

  private lazy val inputType = FlinkTypeFactory.toLogicalRowType(inputRowType)

  def getGrouping: Array[Int] = grouping

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

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    // sort is done in the last sort operator.
    val inputRows = mq.getRowCount(getInput())
    if (inputRows == null) {
      return null
    }
    val cpu = FlinkCost.FUNC_CPU_COST * inputRows *
      modeToGroupToAggCallToAggFunction.flatMap(_._3).size
    val averageRowSize: Double = mq.getAverageRowSize(this)
    val memCost = averageRowSize
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(mq.getRowCount(this), cpu, 0, 0, memCost)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val partitionKeys: Array[Int] = grouping
    val groups = modeToGroupToAggCallToAggFunction.map(_._2)
    val writer = super.explainTerms(pw)
      .itemIf("partitionBy", RelExplainUtil.fieldToString(partitionKeys, inputRowType),
        partitionKeys.nonEmpty)
      .itemIf("orderBy",
        RelExplainUtil.collationToString(groups.head.orderKeys, inputRowType),
        orderKeyIndices.nonEmpty)

    var offset = inputRowType.getFieldCount
    groups.zipWithIndex.foreach { case (group, index) =>
      val namedAggregates = generateNamedAggregates(group)
      val select = RelExplainUtil.overAggregationToString(
        inputRowType,
        outputRowType,
        constants,
        namedAggregates,
        outputInputName = false,
        rowTypeOffset = offset)
      offset += namedAggregates.size
      val windowRange = RelExplainUtil.windowRangeToString(logicWindow, group)
      writer.item("window#" + index, select + windowRange)
    }
    writer.item("select", getRowType.getFieldNames.mkString(", "))
  }

  private def generateNamedAggregates(
      groupWindow: Group): Seq[CalcitePair[AggregateCall, String]] = {
    val aggregateCalls = groupWindow.getAggregateCalls(logicWindow)
    for (i <- 0 until aggregateCalls.size())
      yield new CalcitePair[AggregateCall, String](aggregateCalls.get(i), "windowAgg$" + i)
  }

  private def splitOutOffsetOrInsensitiveGroup()
  : Seq[(OverWindowMode, Window.Group, Seq[(AggregateCall, UserDefinedFunction)])] = {

    def compareTo(o1: Window.RexWinAggCall, o2: Window.RexWinAggCall): Boolean = {
      val allowsFraming1 = o1.getOperator.allowsFraming
      val allowsFraming2 = o2.getOperator.allowsFraming
      if (!allowsFraming1 && !allowsFraming2) {
        o1.getOperator.getClass == o2.getOperator.getClass
      } else {
        allowsFraming1 == allowsFraming2
      }
    }

    def inferGroupMode(group: Window.Group): OverWindowMode = {
      val aggCall = group.aggCalls(0)
      if (aggCall.getOperator.allowsFraming()) {
        if (group.isRows) OverWindowMode.Row else OverWindowMode.Range
      } else {
        if (aggCall.getOperator.isInstanceOf[SqlLeadLagAggFunction]) {
          OverWindowMode.Offset
        } else {
          OverWindowMode.Insensitive
        }
      }
    }

    def createNewGroup(
        group: Window.Group,
        aggCallsBuffer: Seq[(Window.RexWinAggCall, (AggregateCall, UserDefinedFunction))])
    : (OverWindowMode, Window.Group, Seq[(AggregateCall, UserDefinedFunction)]) = {
      val newGroup = new Window.Group(
        group.keys,
        group.isRows,
        group.lowerBound,
        group.upperBound,
        group.orderKeys,
        aggCallsBuffer.map(_._1))
      val mode = inferGroupMode(newGroup)
      (mode, group, aggCallsBuffer.map(_._2))
    }

    val windowGroupInfo =
      ArrayBuffer[(OverWindowMode, Window.Group, Seq[(AggregateCall, UserDefinedFunction)])]()
    windowGroupToAggCallToAggFunction.foreach { case (group, aggCallToAggFunction) =>
      var lastAggCall: Window.RexWinAggCall = null
      val aggCallsBuffer =
        ArrayBuffer[(Window.RexWinAggCall, (AggregateCall, UserDefinedFunction))]()
      group.aggCalls.zip(aggCallToAggFunction).foreach { case (aggCall, aggFunction) =>
        if (lastAggCall != null && !compareTo(lastAggCall, aggCall)) {
          windowGroupInfo.add(createNewGroup(group, aggCallsBuffer))
          aggCallsBuffer.clear()
        }
        aggCallsBuffer.add((aggCall, aggFunction))
        lastAggCall = aggCall
      }
      if (aggCallsBuffer.nonEmpty) {
        windowGroupInfo.add(createNewGroup(group, aggCallsBuffer))
        aggCallsBuffer.clear()
      }
    }
    windowGroupInfo
  }

  override def satisfyTraits(requiredTraitSet: RelTraitSet): Option[RelNode] = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    val requiredCollation = requiredTraitSet.getTrait(RelCollationTraitDef.INSTANCE)
    if (requiredDistribution.getType == ANY && requiredCollation.getFieldCollations.isEmpty) {
      return None
    }

    val selfProvidedTraitSet = inferProvidedTraitSet()
    if (selfProvidedTraitSet.satisfies(requiredTraitSet)) {
      // Current node can satisfy the requiredTraitSet,return the current node with ProvidedTraitSet
      return Some(copy(selfProvidedTraitSet, Seq(getInput)))
    }

    val inputFieldCnt = getInput.getRowType.getFieldCount
    val canSatisfy = if (requiredDistribution.getType == ANY) {
      true
    } else {
      if (!grouping.isEmpty) {
        if (requiredDistribution.requireStrict) {
          requiredDistribution.getKeys == ImmutableIntList.of(grouping: _*)
        } else {
          val isAllFieldsFromInput = requiredDistribution.getKeys.forall(_ < inputFieldCnt)
          if (isAllFieldsFromInput) {
            val tableConfig = FlinkRelOptUtil.getTableConfigFromContext(this)
            if (tableConfig.getConfiguration.getBoolean(
              BatchExecJoinRuleBase.TABLE_OPTIMIZER_SHUFFLE_BY_PARTIAL_KEY_ENABLED)) {
              ImmutableIntList.of(grouping: _*).containsAll(requiredDistribution.getKeys)
            } else {
              requiredDistribution.getKeys == ImmutableIntList.of(grouping: _*)
            }
          } else {
            // If requirement distribution keys are not all comes from input directly,
            // cannot satisfy requirement distribution and collations.
            false
          }
        }
      } else {
        requiredDistribution.getType == SINGLETON
      }
    }
    // If OverAggregate can provide distribution, but it's traits cannot satisfy required
    // distribution, cannot push down distribution and collation requirement (because later
    // shuffle will destroy previous collation.
    if (!canSatisfy) {
      return None
    }

    var inputRequiredTraits = getInput.getTraitSet
    var providedTraits = selfProvidedTraitSet
    val providedCollation = selfProvidedTraitSet.getTrait(RelCollationTraitDef.INSTANCE)
    if (!requiredDistribution.isTop) {
      inputRequiredTraits = inputRequiredTraits.replace(requiredDistribution)
      providedTraits = providedTraits.replace(requiredDistribution)
    }

    if (providedCollation.satisfies(requiredCollation)) {
      // the providedCollation can satisfy the requirement,
      // so don't push down the sort into it's input.
    } else if (providedCollation.getFieldCollations.isEmpty &&
      requiredCollation.getFieldCollations.nonEmpty) {
      // If OverAgg cannot provide collation itself, try to push down collation requirements into
      // it's input if collation fields all come from input node.
      val canPushDownCollation = requiredCollation.getFieldCollations
          .forall(_.getFieldIndex < inputFieldCnt)
      if (canPushDownCollation) {
        inputRequiredTraits = inputRequiredTraits.replace(requiredCollation)
        providedTraits = providedTraits.replace(requiredCollation)
      }
    } else {
      // Don't push down the sort into it's input,
      // due to the provided collation will destroy the input's provided collation.
    }
    val newInput = RelOptRule.convert(getInput, inputRequiredTraits)
    Some(copy(providedTraits, Seq(newInput)))
  }

  private def inferProvidedTraitSet(): RelTraitSet = {
    var selfProvidedTraitSet = getTraitSet
    // provided distribution
    val providedDistribution = if (grouping.nonEmpty) {
      FlinkRelDistribution.hash(grouping.map(Integer.valueOf).toList, requireStrict = false)
    } else {
      FlinkRelDistribution.SINGLETON
    }
    selfProvidedTraitSet = selfProvidedTraitSet.replace(providedDistribution)
    // provided collation
    val firstGroup = windowGroupToAggCallToAggFunction.head._1
    if (OverAggregateUtil.needCollationTrait(logicWindow, firstGroup)) {
      val collation = OverAggregateUtil.createCollation(firstGroup)
      if (!collation.equals(RelCollations.EMPTY)) {
        selfProvidedTraitSet = selfProvidedTraitSet.replace(collation)
      }
    }
    selfProvidedTraitSet
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior = DamBehavior.PIPELINED

  override def getInputNodes: util.List[ExecNode[BatchPlanner, _]] =
    List(getInput.asInstanceOf[ExecNode[BatchPlanner, _]])

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      planner: BatchPlanner): Transformation[BaseRow] = {
    val config = planner.getTableConfig
    val input = getInputNodes.get(0).translateToPlan(planner)
        .asInstanceOf[Transformation[BaseRow]]
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

    var managedMemoryInMB: Int = 0
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
      val memText = config.getConfiguration.getString(
        ExecutionConfigOptions.TABLE_EXEC_RESOURCE_EXTERNAL_BUFFER_MEMORY)
      managedMemoryInMB = MemorySize.parse(memText).getMebiBytes
      new BufferDataOverWindowOperator(
        managedMemoryInMB * NodeResourceUtil.SIZE_IN_MB,
        windowFrames,
        genComparator,
        inputType.getChildren.forall(t => BinaryRow.isInFixedLengthPart(t)))
    }
    val resource = NodeResourceUtil.fromManagedMem(managedMemoryInMB)
    val ret = new OneInputTransformation(
      input,
      getRelDetailedDescription,
      operator,
      BaseRowTypeInfo.of(outputType),
      input.getParallelism)
    ret.setResources(resource, resource)
    ret
  }

  def createOverWindowFrames(config: TableConfig): Array[OverWindowFrame] = {

    def isUnboundedWindow(group: Window.Group) =
      group.lowerBound.isUnbounded && group.upperBound.isUnbounded

    def isUnboundedPrecedingWindow(group: Window.Group) =
      group.lowerBound.isUnbounded && !group.upperBound.isUnbounded

    def isUnboundedFollowingWindow(group: Window.Group) =
      !group.lowerBound.isUnbounded && group.upperBound.isUnbounded

    def isSlidingWindow(group: Window.Group) =
      !group.lowerBound.isUnbounded && !group.upperBound.isUnbounded

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
                        override def calc(value: BaseRow): Long = {
                          value.getLong(rowIndex) * flag
                        }
                      }
                    case INTEGER =>
                      new CalcOffsetFunc {
                        override def calc(value: BaseRow): Long = {
                          value.getInt(rowIndex).toLong * flag
                        }
                      }
                    case SMALLINT =>
                      new CalcOffsetFunc {
                        override def calc(value: BaseRow): Long = {
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

object OverWindowMode extends Enumeration {
  type OverWindowMode = Value
  val Row: OverWindowMode = Value
  val Range: OverWindowMode = Value
  //Then it is a special kind of Window when the agg is LEAD&LAG.
  val Offset: OverWindowMode = Value
  val Insensitive: OverWindowMode = Value
}
