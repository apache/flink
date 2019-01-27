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

package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.functions.UserDefinedFunction
import org.apache.flink.table.api.types._
import org.apache.flink.table.api.{BatchTableEnvironment, TableConfig, TableConfigOptions}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGenUtils.newName
import org.apache.flink.table.codegen._
import org.apache.flink.table.codegen.agg.{AggsHandlerCodeGenerator, BatchExecAggregateCodeGen}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.plan.cost.FlinkBatchCost._
import org.apache.flink.table.plan.cost.FlinkCostFactory
import org.apache.flink.table.plan.nodes.exec.RowBatchExecNode
import org.apache.flink.table.plan.nodes.exec.batch.BatchExecNodeVisitor
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.nodes.physical.batch.OverWindowMode.OverWindowMode
import org.apache.flink.table.plan.util.AggregateUtil.{CalcitePair, transformToBatchAggregateInfoList}
import org.apache.flink.table.plan.util.{AggregateUtil, FlinkRelOptUtil, OverAggregateUtil}
import org.apache.flink.table.runtime.overagg._
import org.apache.flink.table.typeutils.TypeUtils
import org.apache.flink.table.util.NodeResourceUtil

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelDistribution.Type._
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Window.Group
import org.apache.calcite.rel.core.{AggregateCall, Window}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.{RexLiteral, RexWindowBound}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.fun.SqlLeadLagAggFunction
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.ImmutableIntList

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * sort agg operator of over window
 */
class BatchExecOverAggregate(
    cluster: RelOptCluster,
    relBuilder: RelBuilder,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    windowGroupToAggCallToAggFunction: Seq[(Window.Group,
        Seq[(AggregateCall, UserDefinedFunction)])],
    rowRelDataType: RelDataType,
    inputRelDataType: RelDataType,
    grouping: Array[Int],
    orderKeyIdxs: Array[Int],
    orders: Array[Boolean],
    nullIsLasts: Array[Boolean],
    logicWindow: Window)
  extends SingleRel(cluster, traitSet, inputNode)
  with BatchExecAggregateCodeGen
  with BatchPhysicalRel
  with RowBatchExecNode {

  private lazy val modeToGroupToAggCallToAggFunction: Seq[(OverWindowMode, Window.Group, Seq[
      (AggregateCall, UserDefinedFunction)])] = splitOutOffsetOrInsensitiveGroup

  lazy val aggregateCalls: Seq[AggregateCall] = windowGroupToAggCallToAggFunction.flatMap(_._2)
      .map(_._1)

  override def deriveRowType: RelDataType = rowRelDataType

  def getGrouping: Array[Int] = grouping

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    // sort is done in the last sort operator.
    val inputRows = mq.getRowCount(getInput())
    if (inputRows == null) {
      return null
    }
    val cpu = FUNC_CPU_COST * inputRows * modeToGroupToAggCallToAggFunction.flatMap(_._3).size
    val averageRowSize: Double = mq.getAverageRowSize(this)
    val memCost = averageRowSize
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(mq.getRowCount(this), cpu, 0, 0, memCost)
  }

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new BatchExecOverAggregate(
      cluster,
      relBuilder,
      traitSet,
      inputs.get(0),
      windowGroupToAggCallToAggFunction,
      getRowType,
      inputRelDataType,
      grouping,
      orderKeyIdxs,
      orders,
      nullIsLasts,
      logicWindow)
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
    if (OverAggregateUtil.needCollationTrait(input, logicWindow, firstGroup)) {
      val collation = OverAggregateUtil.createFlinkRelCollation(firstGroup)
      if (!collation.equals(RelCollations.EMPTY)) {
        selfProvidedTraitSet = selfProvidedTraitSet.replace(collation)
      }
    }
    selfProvidedTraitSet
  }

  override def satisfyTraitsByInput(requiredTraitSet: RelTraitSet): RelNode = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    val requiredCollation = requiredTraitSet.getTrait(RelCollationTraitDef.INSTANCE)
    if (requiredDistribution.getType == ANY && requiredCollation.getFieldCollations.isEmpty) {
      return null
    }
    val selfProvidedTraitSet = inferProvidedTraitSet()

    if (selfProvidedTraitSet.satisfies(requiredTraitSet)) {
      // Current node can satisfy the requiredTraitSet,return the current node with ProvidedTraitSet
      return copy(selfProvidedTraitSet, Seq(getInput))
    }

    val providedCollation = selfProvidedTraitSet.getTrait(RelCollationTraitDef.INSTANCE)

    val inputFieldCnt = getInput.getRowType.getFieldCount
    val canPushDownDistribution = if (requiredDistribution.getType == ANY) {
      true
    } else {
      if (!grouping.isEmpty) {
        if (requiredDistribution.requireStrict) {
          requiredDistribution.getKeys == ImmutableIntList.of(grouping: _*)
        } else {
          val isAllFieldsFromInput = requiredDistribution.getKeys.forall(_ < inputFieldCnt)
          if (isAllFieldsFromInput) {
            val tableConfig = FlinkRelOptUtil.getTableConfig(this)
            if (tableConfig.getConf.getBoolean(
              TableConfigOptions.SQL_OPTIMIZER_SHUFFLE_PARTIAL_KEY_ENABLED)) {
              ImmutableIntList.of(grouping: _*).containsAll(requiredDistribution.getKeys)
            } else {
              requiredDistribution.getKeys == ImmutableIntList.of(grouping: _*)
            }
          } else {
            // If requirement distribution keys are not all comes from input directly, cannot push
            // down requirement distribution and collations.
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
    if (!canPushDownDistribution) {
      return null
    }

    var pushDownTraits = getInput.getTraitSet
    var providedTraits = selfProvidedTraitSet
    if (!requiredDistribution.isTop) {
      pushDownTraits = pushDownTraits.replace(requiredDistribution)
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
        pushDownTraits = pushDownTraits.replace(requiredCollation)
        providedTraits = providedTraits.replace(requiredCollation)
      }
    } else {
      // Don't push down the sort into it's input,
      // due to the provided collation will destroy the input's provided collation.
    }
    val newInput = RelOptRule.convert(getInput, pushDownTraits)
    copy(providedTraits, Seq(newInput))
  }

  private def generateNamedAggregates(
      groupWindow: Group): Seq[CalcitePair[AggregateCall, String]] = {
    val aggregateCalls = groupWindow.getAggregateCalls(logicWindow)
    for (i <- 0 until aggregateCalls.size())
      yield new CalcitePair[AggregateCall, String](aggregateCalls.get(i), "windowAgg$" + i)
  }

  //print node plan
  override def explainTerms(pw: RelWriter): RelWriter = {
    val partitionKeys: Array[Int] = grouping
    val groups = modeToGroupToAggCallToAggFunction.map(_._2)
    val constants: Seq[RexLiteral] = logicWindow.constants

    val writer = super.explainTerms(pw)
      .itemIf("partitionBy", OverAggregateUtil.partitionToString(rowRelDataType, partitionKeys),
        partitionKeys.nonEmpty)
      .itemIf("orderBy", OverAggregateUtil.orderingToString(
        rowRelDataType, groups.head.orderKeys.getFieldCollations),
        orderKeyIdxs.nonEmpty)

    var offset = inputRelDataType.getFieldCount
    groups.zipWithIndex.foreach { case (group, index) =>
      val namedAggregates = generateNamedAggregates(group)
      val select = OverAggregateUtil.aggregationToString(
        inputRelDataType,
        constants,
        rowRelDataType,
        namedAggregates,
        outputInputName = false,
        rowTypeOffset = offset)
      offset += namedAggregates.size
      val windowRange = OverAggregateUtil.windowRangeToString(logicWindow, group)
      writer.item("window#" + index, select + windowRange)
    }
    writer.item("select", getRowType.getFieldNames.mkString(", "))
  }

  override def isDeterministic: Boolean = AggregateUtil.isDeterministic(aggregateCalls)

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior: DamBehavior = DamBehavior.PIPELINED

  override def accept(visitor: BatchExecNodeVisitor): Unit = visitor.visit(this)

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  /**
    * Internal method, translates the [[org.apache.flink.table.plan.nodes.exec.BatchExecNode]]
    * into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    */
  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {
    val input = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]
    val outputType = FlinkTypeFactory.toInternalBaseRowTypeInfo(getRowType)

    //The generated sort is used for generating the comparator among partitions.
    //So here not care the ASC or DESC for the grouping fields.
    val collation = grouping.map(_ => (true, false))
    val inputRowType = FlinkTypeFactory.toInternalRowType(inputRelDataType)
    val (comparators, serializers) = TypeUtils.flattenComparatorAndSerializer(
      inputRowType.getArity, grouping, collation.map(_._1),
      inputRowType.getFieldTypes.map(_.toInternalType))
    val sortCodeGen = new SortCodeGenerator(
      grouping, grouping.map(inputRowType.getInternalTypeAt).map(_.toInternalType), comparators,
      collation.map(_._1), collation.map(_._2))
    val generatorSort = GeneratedSorter(
      null,
      sortCodeGen.generateRecordComparator("SortComparator"),
      serializers,
      comparators)

    val (needBuffer, needResets) = needBufferDataToNeedResetAcc
    if (!needBuffer.contains(true)) {
      //operator needn't cache data
      val constants = logicWindow.constants
      val constantTypes = constants.map(c => FlinkTypeFactory.toTypeInfo(c.getType))
      val inputTypeNamesWithConstants =
        inputRowType.getFieldNames ++ constants.indices.map(i => "TMP" + i)
      val inputTypesWithConstants =
        inputRowType.getFieldTypes.map(
          TypeConverters.createExternalTypeInfoFromDataType) ++ constantTypes
      val inputTypeWithConstants = logicWindow.getCluster.getTypeFactory
          .asInstanceOf[FlinkTypeFactory]
          .buildLogicalRowType(inputTypeNamesWithConstants, inputTypesWithConstants)

      val aggHandlers = modeToGroupToAggCallToAggFunction.map { case (_, _, aggCallToAggFunction) =>
        val aggInfoList = transformToBatchAggregateInfoList(
          aggCallToAggFunction.map(_._1),
          // use aggInputType which considers constants as input instead of inputSchema.relDataType
          inputTypeWithConstants,
          orderKeyIdxs)
        val codeGenCtx = CodeGeneratorContext(tableEnv.getConfig, supportReference = true)
        val generator = new AggsHandlerCodeGenerator(
          codeGenCtx,
          relBuilder,
          inputRelDataType.getFieldList.map(f => FlinkTypeFactory.toInternalType(f.getType)),
          needRetract = false,
          needMerge = false,
          tableEnv.getConfig.getNullCheck,
          copyInputField = false)
        // over agg code gen must pass the constants
        generator.withConstants(constants).generateAggsHandler(
          "BoundedOverAggregateHelper", aggInfoList)
      }.toArray
      val operator = new OverWindowOperator(aggHandlers, needResets, generatorSort)
      val transformation = new OneInputTransformation(input, "OverAggregate", operator,
        outputType, getResource.getParallelism)
      tableEnv.getRUKeeper.addTransformation(this, transformation)
      transformation.setResources(getResource.getReservedResourceSpec,
        getResource.getPreferResourceSpec)
      transformation
    } else {
      val windowFrames = createOverWindowFrames(tableEnv, inputRowType)
      val operator = new BufferDataOverWindowOperator(
        (getResource.getReservedManagedMem * NodeResourceUtil.SIZE_IN_MB).toInt,
        windowFrames,
        generatorSort)
      val transformation = new OneInputTransformation(input, "OverAggregate", operator,
        outputType, getResource.getParallelism)
      tableEnv.getRUKeeper.addTransformation(this, transformation)
      transformation.setResources(getResource.getReservedResourceSpec,
        getResource.getPreferResourceSpec)
      transformation
    }
  }

  def createOverWindowFrames(tableEnv: BatchTableEnvironment, inType: RowType)
    : Array[OverWindowFrame] = {
    val config = tableEnv.getConfig
    val constants = logicWindow.constants
    val constantTypes = constants.map(c => FlinkTypeFactory.toTypeInfo(c.getType))
    val inputTypeNamesWithConstants = inType.getFieldNames ++ constants.indices.map(i => "TMP" + i)
    val inputTypesWithConstants = inType.getFieldTypes.map(
      TypeConverters.createExternalTypeInfoFromDataType) ++ constantTypes
    val inputTypeWithConstants = logicWindow.getCluster.getTypeFactory
        .asInstanceOf[FlinkTypeFactory]
        .buildLogicalRowType(inputTypeNamesWithConstants, inputTypesWithConstants)
    modeToGroupToAggCallToAggFunction.flatMap { case (mode, windowGroup, aggCallToAggFunction) =>

      mode match {
        case OverWindowMode.Offset =>
          //Split the aggCalls to different over window frame because the length of window frame
          //lies on the offset of the window frame.
          val needRetraction = true
          aggCallToAggFunction.map { case (aggCall, _) =>
            val aggInfoList = transformToBatchAggregateInfoList(
              Seq(aggCall),
              inputTypeWithConstants,
              orderKeyIdxs,
              Array(needRetraction))
            val codeGenCtx = CodeGeneratorContext(config, supportReference = true)
            val generator = new AggsHandlerCodeGenerator(
              codeGenCtx,
              relBuilder,
              inputRelDataType.getFieldList.map(f => FlinkTypeFactory.toInternalType(f.getType)),
              needRetract = true,
              needMerge = false,
              config.getNullCheck,
              copyInputField = false)
            // over agg code gen must pass the constants
            val genAggsHandler = generator.withConstants(constants)
                .generateAggsHandler("BoundedOverAggregateHelper", aggInfoList)
            // The second arg mean the offset for leag/lag function, and the default value is 1L.
            val flag = if (aggCall.getAggregation.kind == SqlKind.LEAD) 1 else -1
            val (offset, calcOffsetFunc) = if (aggCall.getArgList.length >= 2) {
              val constantIndex =
                aggCall.getArgList.get(1) - OverAggregateUtil.calcOriginInputRows(logicWindow)
              if (constantIndex < 0) {
                val rowIndex = aggCall.getArgList.get(1)
                val func = inType.getInternalTypeAt(rowIndex) match {
                  case _: LongType => (value: BaseRow) => value.getLong(rowIndex) * flag
                  case _: IntType => (value: BaseRow) => value.getInt(rowIndex).toLong * flag
                  case _: ShortType => (value: BaseRow) => value.getShort(rowIndex).toLong * flag
                  case _ => throw new RuntimeException("The column type must be in long/int/short.")
                }
                (rowIndex * 1L, func)
              } else {
                val constantOffset = logicWindow.constants.get(
                  constantIndex).getValueAs(classOf[java.lang.Long]) * flag
                (constantOffset, null)
              }
            } else {
              (1L * flag, null)
            }
           new OffsetOverWindowFrame(genAggsHandler, offset, calcOffsetFunc)
          }

        case _ =>
          val aggInfoList = transformToBatchAggregateInfoList(
            aggCallToAggFunction.map(_._1),
            //use aggInputType which considers constants as input instead of inputSchema.relDataType
            inputTypeWithConstants,
            orderKeyIdxs)
          val codeGenCtx = CodeGeneratorContext(config, supportReference = true)
          val generator = new AggsHandlerCodeGenerator(
            codeGenCtx,
            relBuilder,
            inputRelDataType.getFieldList.map(f => FlinkTypeFactory.toInternalType(f.getType)),
            needRetract = false,
            needMerge = false,
            config.getNullCheck,
            copyInputField = false)

          // over agg code gen must pass the constants
          val genAggsHandler = generator.withConstants(constants)
              .generateAggsHandler("BoundedOverAggregateHelper", aggInfoList)

          mode match {
            case OverWindowMode.Range if isUnboundedWindow(windowGroup) =>
              Array(new UnboundedOverWindowFrame(genAggsHandler, generator.valueType))

            case OverWindowMode.Range if isUnboundedPrecedingWindow(windowGroup) =>
              val genBoundComparator = createBoundOrdering(isRow = false, config, inType,
                windowGroup,
                windowGroup.upperBound, isLowerBound = false)
              Array(new UnboundedPrecedingOverWindowFrame(genAggsHandler, genBoundComparator))

            case OverWindowMode.Range if isUnboundedFollowingWindow(windowGroup) =>
              val genBoundComparator = createBoundOrdering(isRow = false, config, inType,
                windowGroup,
                windowGroup.lowerBound, isLowerBound = true)
              Array(new UnboundedFollowingOverWindowFrame(
                genAggsHandler, genBoundComparator, generator.valueType))

            case OverWindowMode.Range if isSlidingWindow(windowGroup) =>
              val genlBoundComparator = createBoundOrdering(isRow = false, config, inType,
                windowGroup,
                windowGroup.lowerBound, isLowerBound = true)
              val genrBoundComparator = createBoundOrdering(
                isRow = false, config, inType,
                windowGroup,
                windowGroup.upperBound, isLowerBound = false)
              Array(new SlidingOverWindowFrame(
                inType, generator.valueType,
                genAggsHandler, genlBoundComparator, genrBoundComparator))

            case OverWindowMode.Row if isUnboundedWindow(windowGroup) =>
              Array(new UnboundedOverWindowFrame(genAggsHandler, generator.valueType))

            case OverWindowMode.Row if isUnboundedPrecedingWindow(windowGroup) =>
              val genBoundComparator = createBoundOrdering(
                isRow = true, config, inType,
                windowGroup,
                windowGroup.upperBound, isLowerBound = false)
              Array(new UnboundedPrecedingOverWindowFrame(genAggsHandler, genBoundComparator))

            case OverWindowMode.Row if isUnboundedFollowingWindow(windowGroup) =>
              val genBoundComparator = createBoundOrdering(
                isRow = true, config, inType,
                windowGroup,
                windowGroup.lowerBound, isLowerBound = true)
              Array(new UnboundedFollowingOverWindowFrame(
                genAggsHandler, genBoundComparator, generator.valueType))

            case OverWindowMode.Row if isSlidingWindow(windowGroup) =>
              val genlBoundComparator = createBoundOrdering(
                isRow = true, config, inType,
                windowGroup,
                windowGroup.lowerBound, isLowerBound = true)
              val genrBoundComparator = createBoundOrdering(
                isRow = true, config, inType,
                windowGroup,
                windowGroup.upperBound, isLowerBound = false)
              Array(new SlidingOverWindowFrame(
                inType, generator.valueType,
                genAggsHandler, genlBoundComparator, genrBoundComparator))

            case OverWindowMode.Insensitive => Array(new InsensitiveWindowFrame(genAggsHandler))
          }
      }
    }.toArray
  }

  private[flink] def createBoundOrdering(isRow: Boolean,
      config: TableConfig, inType: RowType, windowGroup: Window.Group,
      windowBound: RexWindowBound, isLowerBound: Boolean): GeneratedBoundComparator = {
    val bound = OverAggregateUtil.getBoundary(logicWindow, windowBound)
    if (isRow) {
      new RowBoundComparatorCodeGenerator(
        config, bound.asInstanceOf[Long]).generateBoundComparator(newName("RowBoundComparator"))
    } else if (!windowBound.isCurrentRow) {
      //Range Window only support comparing based on a field.
      val sortKey = orderKeyIdxs(0)
      new RangeBoundComparatorCodeGenerator(
        relBuilder, config, inType, bound, sortKey,
        inType.getInternalTypeAt(sortKey).toInternalType,
        orders(0), isLowerBound).generateBoundComparator(newName("RangeBoundComparator"))
    } else {
      //if the bound is current row, then window support comparing based on multi fields.
      new MultiFieldRangeBoundComparatorCodeGenerator(
        inType, orderKeyIdxs,
        orderKeyIdxs.map(inType.getInternalTypeAt).map(_.toInternalType),
        orders, nullIsLasts, isLowerBound).generateBoundComparator(
        newName("MultiFieldRangeBoundComparator"))
    }
  }

  private[flink] def splitOutOffsetOrInsensitiveGroup
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
      : (OverWindowMode, Window.Group, Seq[(AggregateCall,
        UserDefinedFunction)]) = {
      val newGroup = new Window.Group(group.keys, group.isRows, group.lowerBound, group
          .upperBound, group.orderKeys, aggCallsBuffer.map(_._1))
      val mode = inferGroupMode(newGroup)
      (mode, group, aggCallsBuffer.map(_._2))
    }

    val windowGroupInfo = ArrayBuffer[(OverWindowMode, Window.Group, Seq[(AggregateCall,
        UserDefinedFunction)])]()
    windowGroupToAggCallToAggFunction.foreach { case (group, aggCallToAggFunction) =>
      var lastAggCall: Window.RexWinAggCall = null
      val aggCallsBuffer = ArrayBuffer[(Window.RexWinAggCall, (AggregateCall,
          UserDefinedFunction))]()
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

  private[flink] def needBufferDataToNeedResetAcc: (Array[Boolean], Array[Boolean]) = {
    val ret = modeToGroupToAggCallToAggFunction.map { case (mode, windowGroup, _) =>
      mode match {
        case OverWindowMode.Insensitive => (false, false)

        case OverWindowMode.Row if windowGroup.lowerBound.isCurrentRow &&
            windowGroup.upperBound.isCurrentRow => (false, true)

        case OverWindowMode.Row if windowGroup.lowerBound.isUnbounded &&
            windowGroup.upperBound.isCurrentRow => (false, false)

        case _ => (true, false)
      }
    }.toArray
    (ret.map(_._1), ret.map(_._2))
  }

  private[flink] def isUnboundedWindow(windowGroup: Window.Group): Boolean =
    windowGroup.lowerBound.isUnbounded && windowGroup.upperBound.isUnbounded

  private[flink] def isUnboundedPrecedingWindow(windowGroup: Window.Group): Boolean =
    windowGroup.lowerBound.isUnbounded && !windowGroup.upperBound.isUnbounded

  private[flink] def isUnboundedFollowingWindow(windowGroup: Window.Group): Boolean =
    !windowGroup.lowerBound.isUnbounded && windowGroup.upperBound.isUnbounded

  private[flink] def isSlidingWindow(windowGroup: Window.Group): Boolean =
    !windowGroup.lowerBound.isUnbounded && !windowGroup.upperBound.isUnbounded
}

object OverWindowMode extends Enumeration {
  type OverWindowMode = Value
  val Row: OverWindowMode = Value
  val Range: OverWindowMode = Value
  //Then it is a special kind of Window when the agg is LEAD&LAG.
  val Offset: OverWindowMode = Value
  val Insensitive: OverWindowMode = Value
}
