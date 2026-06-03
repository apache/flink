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
package org.apache.flink.table.planner.plan.optimize.program

import org.apache.flink.legacy.table.sinks.{AppendStreamTableSink, RetractStreamTableSink, StreamTableSink, UpsertStreamTableSink}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.planner.calcite.{FlinkTypeFactory, RexTableArgCall}
import org.apache.flink.table.planner.plan.`trait`._
import org.apache.flink.table.planner.plan.nodes.physical.stream._
import org.apache.flink.table.planner.plan.utils._
import org.apache.flink.table.planner.sinks.DataStreamTableSink
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig
import org.apache.flink.table.runtime.operators.join.FlinkJoinType
import org.apache.flink.table.types.inference.{StaticArgument, StaticArgumentTrait}

import org.apache.calcite.linq4j.Ord
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.JoinRelType

import scala.collection.JavaConversions._

/** An optimize program to infer ChangelogMode for every physical node. */
class FlinkChangelogModeInferenceProgram extends FlinkOptimizeProgram[StreamOptimizeContext] {

  override def optimize(root: RelNode, context: StreamOptimizeContext): RelNode = {
    // step1: satisfy ModifyKindSet trait
    val physicalRoot = root.asInstanceOf[StreamPhysicalRel]
    val rootWithModifyKindSet = new SatisfyModifyKindSetTraitVisitor().visit(
      physicalRoot,
      // we do not propagate the ModifyKindSet requirement and requester among blocks
      // set default ModifyKindSet requirement and requester for root
      ModifyKindSetTrait.ALL_CHANGES,
      "ROOT"
    )

    // step2: satisfy UpdateKind trait
    val rootModifyKindSet = getModifyKindSet(rootWithModifyKindSet)
    // use the required UpdateKindTrait from parent blocks
    val requiredUpdateKindTraits = if (rootModifyKindSet.contains(ModifyKind.UPDATE)) {
      if (context.isUpdateBeforeRequired) {
        Seq(UpdateKindTrait.BEFORE_AND_AFTER)
      } else {
        // update_before is not required, and input contains updates
        // try ONLY_UPDATE_AFTER first, and then BEFORE_AND_AFTER
        Seq(UpdateKindTrait.ONLY_UPDATE_AFTER, UpdateKindTrait.BEFORE_AND_AFTER)
      }
    } else {
      // there is no updates
      Seq(UpdateKindTrait.NONE)
    }

    val updateKindTraitVisitor = new SatisfyUpdateKindTraitVisitor(context)
    val updateRoot = requiredUpdateKindTraits.flatMap {
      requiredUpdateKindTrait =>
        val updated = updateKindTraitVisitor.visit(rootWithModifyKindSet, requiredUpdateKindTrait)
        if (updated.isPresent) Some(updated.get) else None
    }

    // step3: satisfy DeleteKind trait
    val requiredDeleteKindTraits = if (rootModifyKindSet.contains(ModifyKind.DELETE)) {
      root match {
        case _: StreamPhysicalSink =>
          // try DELETE_BY_KEY first, and then FULL_DELETE
          Seq(DeleteKindTrait.DELETE_BY_KEY, DeleteKindTrait.FULL_DELETE)
        case _ =>
          // for non-sink nodes prefer full deletes
          Seq(DeleteKindTrait.FULL_DELETE)
      }
    } else {
      // there is no deletes
      Seq(DeleteKindTrait.NONE)
    }

    val deleteKindTraitVisitor = new SatisfyDeleteKindTraitVisitor(context)
    val finalRoot = if (updateRoot.isEmpty) {
      updateRoot
    } else {
      requiredDeleteKindTraits.flatMap {
        requiredDeleteKindTrait =>
          val deleteRoot = deleteKindTraitVisitor.visit(updateRoot.head, requiredDeleteKindTrait)
          if (deleteRoot.isPresent) Some(deleteRoot.get) else None
      }
    }

    // step4: sanity check and return non-empty root
    if (finalRoot.isEmpty) {
      val plan = FlinkRelOptUtil.toString(root, withChangelogTraits = true)
      throw new TableException(
        "Can't generate a valid execution plan for the given query:\n" + plan)
    } else {
      finalRoot.head
    }
  }

  /**
   * A visitor which will try to satisfy the required [[ModifyKindSetTrait]] from root.
   *
   * <p>After traversed by this visitor, every node should have a correct [[ModifyKindSetTrait]] or
   * an exception should be thrown if the planner doesn't support to satisfy the required
   * [[ModifyKindSetTrait]].
   */
  private class SatisfyModifyKindSetTraitVisitor {

    /**
     * Try to satisfy the required [[ModifyKindSetTrait]] from root.
     *
     * <p>Each node should first require a [[ModifyKindSetTrait]] to its children. If the trait
     * provided by children does not satisfy the required one, it should throw an exception and
     * prompt the user that plan is not supported. The required [[ModifyKindSetTrait]] may come from
     * the node's parent, or come from the node itself, depending on whether the node will destroy
     * the trait provided by children or pass the trait from children.
     *
     * <p>Each node should provide [[ModifyKindSetTrait]] according to current node's behavior and
     * the ModifyKindSetTrait provided by children.
     *
     * @param rel
     *   the node who should satisfy the requiredTrait
     * @param requiredTrait
     *   the required ModifyKindSetTrait
     * @param requester
     *   the requester who starts the requirement, used for better exception message
     * @return
     *   A converted node which satisfy required traits by inputs node of current node. Or throws
     *   exception if required trait can’t be satisfied.
     */
    def visit(
        rel: StreamPhysicalRel,
        requiredTrait: ModifyKindSetTrait,
        requester: String): StreamPhysicalRel = rel match {
      case sink: StreamPhysicalSink =>
        val name = s"Table sink '${sink.contextResolvedTable.getIdentifier.asSummaryString()}'"
        val queryModifyKindSet = deriveQueryDefaultChangelogMode(sink.getInput, name)
        val sinkRequiredTrait =
          ModifyKindSetTrait.fromChangelogMode(sink.tableSink.getChangelogMode(queryModifyKindSet))
        val children = visitChildren(sink, sinkRequiredTrait, name)
        val sinkTrait = sink.getTraitSet.plus(ModifyKindSetTrait.EMPTY)
        // ignore required trait from context, because sink is the true root
        sink.copy(sinkTrait, children).asInstanceOf[StreamPhysicalRel]

      case sink: StreamPhysicalLegacySink[_] =>
        val (sinkRequiredTrait, name) = sink.sink match {
          case _: UpsertStreamTableSink[_] =>
            (ModifyKindSetTrait.ALL_CHANGES, "UpsertStreamTableSink")
          case _: RetractStreamTableSink[_] =>
            (ModifyKindSetTrait.ALL_CHANGES, "RetractStreamTableSink")
          case _: AppendStreamTableSink[_] =>
            (ModifyKindSetTrait.INSERT_ONLY, "AppendStreamTableSink")
          case _: StreamTableSink[_] =>
            (ModifyKindSetTrait.INSERT_ONLY, "StreamTableSink")
          case ds: DataStreamTableSink[_] =>
            if (ds.withChangeFlag) {
              (ModifyKindSetTrait.ALL_CHANGES, "toRetractStream")
            } else {
              (ModifyKindSetTrait.INSERT_ONLY, "toAppendStream")
            }
          case _ =>
            throw new UnsupportedOperationException(
              s"Unsupported sink '${sink.sink.getClass.getSimpleName}'")
        }
        val children = visitChildren(sink, sinkRequiredTrait, name)
        val sinkTrait = sink.getTraitSet.plus(ModifyKindSetTrait.EMPTY)
        // ignore required trait from context, because sink is the true root
        sink.copy(sinkTrait, children).asInstanceOf[StreamPhysicalRel]

      case agg: StreamPhysicalGroupAggregate =>
        // agg support all changes in input
        val children = visitChildren(agg, ModifyKindSetTrait.ALL_CHANGES)
        val inputModifyKindSet = getModifyKindSet(children.head)
        val builder = ModifyKindSet
          .newBuilder()
          .addContainedKind(ModifyKind.INSERT)
          .addContainedKind(ModifyKind.UPDATE)
        if (
          inputModifyKindSet.contains(ModifyKind.UPDATE) ||
          inputModifyKindSet.contains(ModifyKind.DELETE)
        ) {
          builder.addContainedKind(ModifyKind.DELETE)
        }
        val providedTrait = new ModifyKindSetTrait(builder.build())
        createNewNode(agg, children, providedTrait, requiredTrait, requester)

      case tagg: StreamPhysicalGroupTableAggregateBase =>
        // table agg support all changes in input
        val children = visitChildren(tagg, ModifyKindSetTrait.ALL_CHANGES)
        // table aggregate will produce all changes, including deletions
        createNewNode(tagg, children, ModifyKindSetTrait.ALL_CHANGES, requiredTrait, requester)

      case agg: StreamPhysicalPythonGroupAggregate =>
        // agg support all changes in input
        val children = visitChildren(agg, ModifyKindSetTrait.ALL_CHANGES)
        val inputModifyKindSet = getModifyKindSet(children.head)
        val builder = ModifyKindSet
          .newBuilder()
          .addContainedKind(ModifyKind.INSERT)
          .addContainedKind(ModifyKind.UPDATE)
        if (
          inputModifyKindSet.contains(ModifyKind.UPDATE) ||
          inputModifyKindSet.contains(ModifyKind.DELETE)
        ) {
          builder.addContainedKind(ModifyKind.DELETE)
        }
        val providedTrait = new ModifyKindSetTrait(builder.build())
        createNewNode(agg, children, providedTrait, requiredTrait, requester)

      case window: StreamPhysicalGroupWindowAggregateBase =>
        // WindowAggregate and WindowTableAggregate support all changes in input
        val children = visitChildren(window, ModifyKindSetTrait.ALL_CHANGES)
        val builder = ModifyKindSet
          .newBuilder()
          .addContainedKind(ModifyKind.INSERT)
        if (window.emitStrategy.produceUpdates) {
          builder.addContainedKind(ModifyKind.UPDATE)
        }
        val providedTrait = new ModifyKindSetTrait(builder.build())
        createNewNode(window, children, providedTrait, requiredTrait, requester)

      case window: StreamPhysicalWindowAggregate =>
        // WindowAggregate and WindowTableAggregate support all changes in input
        val children = visitChildren(window, ModifyKindSetTrait.ALL_CHANGES)
        // TODO support early / late fire and then this node may produce update records
        val providedTrait = new ModifyKindSetTrait(ModifyKindSet.INSERT_ONLY)
        createNewNode(window, children, providedTrait, requiredTrait, requester)

      case _: StreamPhysicalWindowRank | _: StreamPhysicalWindowDeduplicate =>
        // WindowAggregate, WindowRank, WindowDeduplicate support insert-only in input
        val children = visitChildren(rel, ModifyKindSetTrait.INSERT_ONLY)
        val providedTrait = ModifyKindSetTrait.INSERT_ONLY
        createNewNode(rel, children, providedTrait, requiredTrait, requester)

      case rank: StreamPhysicalRank if RankUtil.isDeduplication(rank) =>
        val children = visitChildren(rel, ModifyKindSetTrait.ALL_CHANGES)
        val tableConfig = unwrapTableConfig(rank)

        // if the rank is deduplication and can be executed as insert-only, forward that information
        val insertOnly = children.forall(ChangelogPlanUtils.isInsertOnly)

        val providedTrait = {
          if (
            insertOnly && RankUtil.outputInsertOnlyInDeduplicate(
              tableConfig,
              RankUtil.keepLastDeduplicateRow(rank.orderKey))
          ) {
            // Deduplicate outputs append only if first row is kept and mini batching is disabled
            ModifyKindSetTrait.INSERT_ONLY
          } else {
            ModifyKindSetTrait.ALL_CHANGES
          }
        }

        createNewNode(rel, children, providedTrait, requiredTrait, requester)

      case rank: StreamPhysicalRank if !RankUtil.isDeduplication(rank) =>
        // Rank supports consuming all changes
        val children = visitChildren(rel, ModifyKindSetTrait.ALL_CHANGES)
        createNewNode(rel, children, ModifyKindSetTrait.ALL_CHANGES, requiredTrait, requester)

      case limit: StreamPhysicalLimit =>
        // limit support all changes in input
        val children = visitChildren(limit, ModifyKindSetTrait.ALL_CHANGES)
        val providedTrait = if (getModifyKindSet(children.head).isInsertOnly) {
          ModifyKindSetTrait.INSERT_ONLY
        } else {
          ModifyKindSetTrait.ALL_CHANGES
        }
        createNewNode(limit, children, providedTrait, requiredTrait, requester)

      case _: StreamPhysicalSortLimit =>
        // SortLimit supports consuming all changes
        val children = visitChildren(rel, ModifyKindSetTrait.ALL_CHANGES)
        createNewNode(rel, children, ModifyKindSetTrait.ALL_CHANGES, requiredTrait, requester)

      case sort: StreamPhysicalSort =>
        // Sort supports consuming all changes
        val children = visitChildren(rel, ModifyKindSetTrait.ALL_CHANGES)
        // Sort will buffer all inputs, and produce insert-only messages when input is finished
        createNewNode(sort, children, ModifyKindSetTrait.INSERT_ONLY, requiredTrait, requester)

      case cep: StreamPhysicalMatch =>
        // CEP only supports consuming insert-only and producing insert-only changes
        // give a better requester name for exception message
        val children = visitChildren(cep, ModifyKindSetTrait.INSERT_ONLY, "Match Recognize")
        createNewNode(cep, children, ModifyKindSetTrait.INSERT_ONLY, requiredTrait, requester)

      case over: StreamPhysicalOverAggregate =>
        // OverAggregate can only support insert for row-time/proc-time sort keys
        var overRequiredTrait = ModifyKindSetTrait.INSERT_ONLY
        val builder = ModifyKindSet
          .newBuilder()
          .addContainedKind(ModifyKind.INSERT)
        val groups = over.logicWindow.groups

        if (!groups.isEmpty && !groups.get(0).orderKeys.getFieldCollations.isEmpty) {
          // All aggregates are computed over the same window and order by is supported for only 1 field
          val orderKeyIndex = groups.get(0).orderKeys.getFieldCollations.get(0).getFieldIndex
          val orderKeyType = over.logicWindow.getRowType.getFieldList.get(orderKeyIndex).getType
          if (
            !FlinkTypeFactory.isRowtimeIndicatorType(orderKeyType)
            && !FlinkTypeFactory.isProctimeIndicatorType(orderKeyType)
          ) {
            // Only non row-time/proc-time sort can support UPDATES
            builder.addContainedKind(ModifyKind.UPDATE)
            builder.addContainedKind(ModifyKind.DELETE)
            overRequiredTrait = ModifyKindSetTrait.ALL_CHANGES
          }
        }
        val children = visitChildren(over, overRequiredTrait)
        val providedTrait = new ModifyKindSetTrait(builder.build())
        createNewNode(over, children, providedTrait, requiredTrait, requester)

      case _: StreamPhysicalTemporalSort | _: StreamPhysicalIntervalJoin |
          _: StreamPhysicalPythonOverAggregate =>
        // TemporalSort, IntervalJoin only support consuming insert-only
        // and producing insert-only changes
        val children = visitChildren(rel, ModifyKindSetTrait.INSERT_ONLY)
        createNewNode(rel, children, ModifyKindSetTrait.INSERT_ONLY, requiredTrait, requester)

      case _: StreamPhysicalMLPredictTableFunction | _: StreamPhysicalVectorSearchTableFunction =>
        // MLPredict, VectorSearch supports only support consuming insert-only
        val children = visitChildren(rel, ModifyKindSetTrait.INSERT_ONLY)
        createNewNode(rel, children, ModifyKindSetTrait.INSERT_ONLY, requiredTrait, requester)

      case join: StreamPhysicalJoin =>
        // join support all changes in input
        val children = visitChildren(rel, ModifyKindSetTrait.ALL_CHANGES)
        val leftKindSet = getModifyKindSet(children.head)
        val rightKindSet = getModifyKindSet(children.last)
        val innerOrSemi = join.joinSpec.getJoinType == FlinkJoinType.INNER ||
          join.joinSpec.getJoinType == FlinkJoinType.SEMI
        val providedTrait = if (innerOrSemi) {
          // forward left and right modify operations
          new ModifyKindSetTrait(leftKindSet.union(rightKindSet))
        } else {
          // otherwise, it may produce any kinds of changes
          ModifyKindSetTrait.ALL_CHANGES
        }
        createNewNode(join, children, providedTrait, requiredTrait, requester)

      case windowJoin: StreamPhysicalWindowJoin =>
        // Currently, window join only supports INSERT_ONLY in input
        val children = visitChildren(rel, ModifyKindSetTrait.INSERT_ONLY)
        createNewNode(
          windowJoin,
          children,
          ModifyKindSetTrait.INSERT_ONLY,
          requiredTrait,
          requester)

      case temporalJoin: StreamPhysicalTemporalJoin =>
        // currently, temporal join supports all kings of changes, including right side
        val children = visitChildren(temporalJoin, ModifyKindSetTrait.ALL_CHANGES)
        // forward left input changes
        val leftTrait = children.head.getTraitSet.getTrait(ModifyKindSetTraitDef.INSTANCE)
        createNewNode(temporalJoin, children, leftTrait, requiredTrait, requester)

      case multiJoin: StreamPhysicalMultiJoin =>
        // multi-join supports all changes in input
        val children = visitChildren(multiJoin, ModifyKindSetTrait.ALL_CHANGES)
        val allInnerJoins = multiJoin.getJoinTypes.forall(_ == JoinRelType.INNER)
        val providedTrait = if (allInnerJoins) {
          // if all are inner joins, forward all modify operations from children
          val kindSets = children.map(getModifyKindSet)
          new ModifyKindSetTrait(ModifyKindSet.union(kindSets: _*))
        } else {
          // if there is any outer join, it may produce any kinds of changes
          ModifyKindSetTrait.ALL_CHANGES
        }
        createNewNode(multiJoin, children, providedTrait, requiredTrait, requester)

      case _: StreamPhysicalCalcBase | _: StreamPhysicalCorrelateBase |
          _: StreamPhysicalLookupJoin | _: StreamPhysicalExchange | _: StreamPhysicalExpand |
          _: StreamPhysicalMiniBatchAssigner | _: StreamPhysicalWatermarkAssigner |
          _: StreamPhysicalWindowTableFunction =>
        // transparent forward requiredTrait to children
        val children = visitChildren(rel, requiredTrait, requester)
        val childrenTrait = children.head.getTraitSet.getTrait(ModifyKindSetTraitDef.INSTANCE)
        // forward children mode
        createNewNode(rel, children, childrenTrait, requiredTrait, requester)

      case union: StreamPhysicalUnion =>
        // transparent forward requiredTrait to children
        val children = visitChildren(rel, requiredTrait, requester)
        // union provides all possible kinds of children have
        val providedKindSet = ModifyKindSet.union(children.map(getModifyKindSet): _*)
        createNewNode(
          union,
          children,
          new ModifyKindSetTrait(providedKindSet),
          requiredTrait,
          requester)

      case normalize: StreamPhysicalChangelogNormalize =>
        // changelog normalize support update&delete input
        val children = visitChildren(normalize, ModifyKindSetTrait.ALL_CHANGES)
        // changelog normalize will output all changes
        val providedTrait = ModifyKindSetTrait.ALL_CHANGES
        createNewNode(normalize, children, providedTrait, requiredTrait, requester)

      case ts: StreamPhysicalTableSourceScan =>
        // ScanTableSource supports produces updates and deletions
        val providedTrait = ModifyKindSetTrait.fromChangelogMode(ts.tableSource.getChangelogMode)
        createNewNode(ts, List(), providedTrait, requiredTrait, requester)

      case _: StreamPhysicalDataStreamScan | _: StreamPhysicalLegacyTableSourceScan |
          _: StreamPhysicalValues =>
        // DataStream, TableSource and Values only support producing insert-only messages
        createNewNode(rel, List(), ModifyKindSetTrait.INSERT_ONLY, requiredTrait, requester)

      case scan: StreamPhysicalIntermediateTableScan =>
        val providedTrait = new ModifyKindSetTrait(scan.intermediateTable.modifyKindSet)
        createNewNode(scan, List(), providedTrait, requiredTrait, requester)

      case process: StreamPhysicalProcessTableFunction =>
        // Accepted changes depend on table argument declaration
        val requiredChildrenTraits = StreamPhysicalProcessTableFunction
          .getProvidedInputArgs(process.getCall)
          .map(arg => arg.e)
          .map(
            arg =>
              if (arg.is(StaticArgumentTrait.SUPPORT_UPDATES)) {
                ModifyKindSetTrait.ALL_CHANGES
              } else {
                ModifyKindSetTrait.INSERT_ONLY
              })
          .toList
        val children = if (requiredChildrenTraits.isEmpty) {
          // Constant function has a single StreamPhysicalValues input
          visitChildren(process, ModifyKindSetTrait.INSERT_ONLY)
        } else {
          visitChildren(process, requiredChildrenTraits)
        }
        // Query PTF for updating vs. non-updating
        val providedModifyTrait = queryPtfChangelogMode(
          process,
          children,
          requiredTrait.modifyKindSet.toChangelogModeBuilder.build(),
          ModifyKindSetTrait.fromChangelogMode,
          ModifyKindSetTrait.INSERT_ONLY)
        createNewNode(process, children, providedModifyTrait, requiredTrait, requester)

      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported visit for ${rel.getClass.getSimpleName}")
    }

    private def visitChildren(
        parent: StreamPhysicalRel,
        requiredChildrenTrait: ModifyKindSetTrait): List[StreamPhysicalRel] = {
      visitChildren(parent, requiredChildrenTrait, getNodeName(parent))
    }

    private def visitChildren(
        parent: StreamPhysicalRel,
        requiredChildrenTrait: ModifyKindSetTrait,
        requester: String): List[StreamPhysicalRel] = {
      val newChildren = for (i <- 0 until parent.getInputs.size()) yield {
        visitChild(parent, i, requiredChildrenTrait, requester)
      }
      newChildren.toList
    }

    private def visitChildren(
        parent: StreamPhysicalRel,
        requiredChildrenTraits: List[ModifyKindSetTrait]): List[StreamPhysicalRel] = {
      val requester = getNodeName(parent)
      val newChildren = for (i <- 0 until parent.getInputs.size()) yield {
        visitChild(parent, i, requiredChildrenTraits(i), requester)
      }
      newChildren.toList
    }

    private def visitChild(
        parent: StreamPhysicalRel,
        childOrdinal: Int,
        requiredChildTrait: ModifyKindSetTrait,
        requester: String): StreamPhysicalRel = {
      val child = parent.getInput(childOrdinal).asInstanceOf[StreamPhysicalRel]
      this.visit(child, requiredChildTrait, requester)
    }

    private def getNodeName(rel: StreamPhysicalRel): String = {
      val prefix = "StreamExec"
      val typeName = rel.getRelTypeName
      if (typeName.startsWith(prefix)) {
        typeName.substring(prefix.length)
      } else {
        typeName
      }
    }

    /**
     * Derives the [[ModifyKindSetTrait]] of query plan without required ModifyKindSet validation.
     */
    private def deriveQueryDefaultChangelogMode(queryNode: RelNode, name: String): ChangelogMode = {
      val newNode =
        visit(queryNode.asInstanceOf[StreamPhysicalRel], ModifyKindSetTrait.ALL_CHANGES, name)
      getModifyKindSet(newNode).toDefaultChangelogMode
    }

    private def createNewNode(
        node: StreamPhysicalRel,
        children: List[StreamPhysicalRel],
        providedTrait: ModifyKindSetTrait,
        requiredTrait: ModifyKindSetTrait,
        requestedOwner: String): StreamPhysicalRel = {
      if (!providedTrait.satisfies(requiredTrait)) {
        val diff = providedTrait.modifyKindSet.minus(requiredTrait.modifyKindSet)
        val diffString = diff.getContainedKinds.toList.sorted // for deterministic error message
          .map(_.toString.toLowerCase)
          .mkString(" and ")
        // creates a new node based on the new children, to have a more correct node description
        // e.g. description of GroupAggregate is based on the ModifyKindSetTrait of children
        val tempNode = node.copy(node.getTraitSet, children).asInstanceOf[StreamPhysicalRel]
        val nodeString = tempNode.getRelDetailedDescription
        throw new TableException(
          s"$requestedOwner doesn't support consuming $diffString changes " +
            s"which is produced by node $nodeString")
      }
      val newTraitSet = node.getTraitSet.plus(providedTrait)
      node.copy(newTraitSet, children).asInstanceOf[StreamPhysicalRel]
    }
  }

  private def isNonUpsertKeyCondition(calc: StreamPhysicalCalcBase): Boolean =
    ChangelogModeInferenceUtils.isNonUpsertKeyCondition(calc)

  private def getModifyKindSet(node: RelNode): ModifyKindSet =
    ChangelogModeInferenceUtils.getModifyKindSet(node)

  // ----------------------------------------------------------------------------------------------
  // PTF helper methods
  // ----------------------------------------------------------------------------------------------

  private def toChangelogMode(
      node: StreamPhysicalRel,
      updateKindTrait: Option[UpdateKindTrait],
      deleteKindTrait: Option[DeleteKindTrait]): ChangelogMode =
    ChangelogModeInferenceUtils.toChangelogMode(
      node,
      updateKindTrait.orNull,
      deleteKindTrait.orNull)

  private def ptfRequiresUpdateBefore(
      tableArg: StaticArgument,
      tableArgCall: RexTableArgCall,
      input: StreamPhysicalRel): Boolean =
    ChangelogModeInferenceUtils.ptfRequiresUpdateBefore(tableArg, tableArgCall, input)

  private def extractPtfTableArgComponents(
      process: StreamPhysicalProcessTableFunction,
      child: StreamPhysicalRel,
      inputArg: Ord[StaticArgument]): (StaticArgument, RexTableArgCall, ModifyKindSet) = {
    val components =
      ChangelogModeInferenceUtils.extractPtfTableArgComponents(process, child, inputArg)
    (components.tableArg, components.tableArgCall, components.modifyKindSet)
  }

  private def queryPtfChangelogMode[T](
      process: StreamPhysicalProcessTableFunction,
      children: List[StreamPhysicalRel],
      requiredChangelogMode: ChangelogMode,
      toTraitSet: ChangelogMode => T,
      defaultTraitSet: T): T =
    ChangelogModeInferenceUtils.queryPtfChangelogMode[T](
      process,
      children,
      requiredChangelogMode,
      (mode: ChangelogMode) => toTraitSet(mode),
      defaultTraitSet)
}
