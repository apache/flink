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
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.config.ExecutionConfigOptions.UpsertMaterialize
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.functions.ChangelogFunction
import org.apache.flink.table.functions.ChangelogFunction.ChangelogContext
import org.apache.flink.table.planner.calcite.{FlinkTypeFactory, RexTableArgCall}
import org.apache.flink.table.planner.plan.`trait`._
import org.apache.flink.table.planner.plan.`trait`.DeleteKindTrait.{deleteOnKeyOrNone, fullDeleteOrNone, DELETE_BY_KEY}
import org.apache.flink.table.planner.plan.`trait`.UpdateKindTrait.{beforeAfterOrNone, onlyAfterOrNone, BEFORE_AND_AFTER, ONLY_UPDATE_AFTER}
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.physical.stream._
import org.apache.flink.table.planner.plan.optimize.ChangelogNormalizeRequirementResolver
import org.apache.flink.table.planner.plan.utils._
import org.apache.flink.table.planner.plan.utils.RankProcessStrategy.{AppendFastStrategy, RetractStrategy, UpdateFastStrategy}
import org.apache.flink.table.planner.sinks.DataStreamTableSink
import org.apache.flink.table.planner.utils.{JavaScalaConversionUtil, ShortcutUtils}
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig
import org.apache.flink.table.runtime.operators.join.FlinkJoinType
import org.apache.flink.table.types.inference.{StaticArgument, StaticArgumentTrait}
import org.apache.flink.types.RowKind

import org.apache.calcite.linq4j.Ord
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex.RexCall
import org.apache.calcite.util.ImmutableBitSet

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
        updateKindTraitVisitor.visit(rootWithModifyKindSet, requiredUpdateKindTrait)
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
          deleteKindTraitVisitor.visit(updateRoot.head, requiredDeleteKindTrait)
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

      case ml_predict: StreamPhysicalMLPredictTableFunction =>
        // MLPredict supports only support consuming insert-only
        val children = visitChildren(ml_predict, ModifyKindSetTrait.INSERT_ONLY)
        createNewNode(
          ml_predict,
          children,
          ModifyKindSetTrait.INSERT_ONLY,
          requiredTrait,
          requester)

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

  /**
   * A visitor which will try to satisfy the required [[UpdateKindTrait]] from root.
   *
   * <p>After traversed by this visitor, every node should have a correct [[UpdateKindTrait]] or
   * returns None if the planner doesn't support to satisfy the required [[UpdateKindTrait]].
   */
  private class SatisfyUpdateKindTraitVisitor(private val context: StreamOptimizeContext) {

    /**
     * Try to satisfy the required [[UpdateKindTrait]] from root.
     *
     * <p>Each node will first require a UpdateKindTrait to its children. The required
     * UpdateKindTrait may come from the node's parent, or come from the node itself, depending on
     * whether the node will destroy the trait provided by children or pass the trait from children.
     *
     * <p>If the node will pass the children's UpdateKindTrait without destroying it, then return a
     * new node with new inputs and forwarded UpdateKindTrait.
     *
     * <p>If the node will destroy the children's UpdateKindTrait, then the node itself needs to be
     * converted, or a new node should be generated to satisfy the required trait, such as marking
     * itself not to generate UPDATE_BEFORE, or generating a new node to filter UPDATE_BEFORE.
     *
     * @param rel
     *   the node who should satisfy the requiredTrait
     * @param requiredUpdateTrait
     *   the required UpdateKindTrait
     * @return
     *   A converted node which satisfies required traits by input nodes of current node. Or None if
     *   required traits cannot be satisfied.
     */
    def visit(
        rel: StreamPhysicalRel,
        requiredUpdateTrait: UpdateKindTrait): Option[StreamPhysicalRel] =
      rel match {
        case sink: StreamPhysicalSink =>
          val sinkRequiredTraits = inferSinkRequiredTraits(sink)
          val upsertMaterialize = analyzeUpsertMaterializeStrategy(sink)
          visitSink(sink.copy(upsertMaterialize), sinkRequiredTraits)

        case sink: StreamPhysicalLegacySink[_] =>
          val childModifyKindSet = getModifyKindSet(sink.getInput)
          val onlyAfter = onlyAfterOrNone(childModifyKindSet)
          val beforeAndAfter = beforeAfterOrNone(childModifyKindSet)
          val sinkRequiredTraits = sink.sink match {
            case _: UpsertStreamTableSink[_] =>
              // support both ONLY_AFTER and BEFORE_AFTER, but prefer ONLY_AFTER
              Seq(onlyAfter, beforeAndAfter)
            case _: RetractStreamTableSink[_] =>
              Seq(beforeAndAfter)
            case _: AppendStreamTableSink[_] | _: StreamTableSink[_] =>
              Seq(UpdateKindTrait.NONE)
            case ds: DataStreamTableSink[_] =>
              if (ds.withChangeFlag) {
                if (ds.needUpdateBefore) {
                  Seq(beforeAndAfter)
                } else {
                  // support both ONLY_AFTER and BEFORE_AFTER, but prefer ONLY_AFTER
                  Seq(onlyAfter, beforeAndAfter)
                }
              } else {
                Seq(UpdateKindTrait.NONE)
              }
          }
          visitSink(sink, sinkRequiredTraits)

        case _: StreamPhysicalGroupAggregate | _: StreamPhysicalGroupTableAggregate |
            _: StreamPhysicalLimit | _: StreamPhysicalPythonGroupAggregate |
            _: StreamPhysicalPythonGroupTableAggregate | _: StreamPhysicalGroupWindowAggregateBase |
            _: StreamPhysicalWindowAggregate | _: StreamPhysicalOverAggregate =>
          // Aggregate, TableAggregate, OverAggregate, Limit, GroupWindowAggregate, WindowAggregate,
          // and WindowTableAggregate requires update_before if there are updates
          val requiredChildUpdateTrait = beforeAfterOrNone(getModifyKindSet(rel.getInput(0)))
          val children = visitChildren(rel, requiredChildUpdateTrait)
          // use requiredTrait as providedTrait, because they should support all kinds of UpdateKind
          createNewNode(rel, children, requiredUpdateTrait)

        case _: StreamPhysicalWindowRank | _: StreamPhysicalWindowDeduplicate |
            _: StreamPhysicalTemporalSort | _: StreamPhysicalMatch | _: StreamPhysicalIntervalJoin |
            _: StreamPhysicalPythonOverAggregate | _: StreamPhysicalWindowJoin =>
          // WindowRank, WindowDeduplicate, Deduplicate, TemporalSort, CEP,
          // and IntervalJoin, WindowJoin require nothing about UpdateKind.
          val children = visitChildren(rel, UpdateKindTrait.NONE)
          createNewNode(rel, children, requiredUpdateTrait)

        case rank: StreamPhysicalRank =>
          val rankStrategies =
            RankProcessStrategy.analyzeRankProcessStrategies(rank, rank.partitionKey, rank.orderKey)
          visitRankStrategies(
            rankStrategies,
            requiredUpdateTrait,
            rankStrategy => rank.copy(rankStrategy))

        case sortLimit: StreamPhysicalSortLimit =>
          val rankStrategies = RankProcessStrategy.analyzeRankProcessStrategies(
            sortLimit,
            ImmutableBitSet.of(),
            sortLimit.getCollation)
          visitRankStrategies(
            rankStrategies,
            requiredUpdateTrait,
            rankStrategy => sortLimit.copy(rankStrategy))

        case sort: StreamPhysicalSort =>
          val requiredChildTrait = beforeAfterOrNone(getModifyKindSet(sort.getInput))
          val children = visitChildren(sort, requiredChildTrait)
          createNewNode(sort, children, requiredUpdateTrait)

        case join: StreamPhysicalJoin =>
          val onlyAfterByParent = requiredUpdateTrait.updateKind == UpdateKind.ONLY_UPDATE_AFTER
          val children = join.getInputs.zipWithIndex.map {
            case (child, childOrdinal) =>
              val physicalChild = child.asInstanceOf[StreamPhysicalRel]
              val supportOnlyAfter = join.inputUniqueKeyContainsJoinKey(childOrdinal)
              val inputModifyKindSet = getModifyKindSet(physicalChild)
              if (onlyAfterByParent) {
                if (inputModifyKindSet.contains(ModifyKind.UPDATE) && !supportOnlyAfter) {
                  // the parent requires only-after, however, the join doesn't support this
                  None
                } else {
                  this.visit(physicalChild, onlyAfterOrNone(inputModifyKindSet))
                }
              } else {
                this.visit(physicalChild, beforeAfterOrNone(inputModifyKindSet))
              }
          }
          if (children.exists(_.isEmpty)) {
            None
          } else {
            createNewNode(join, Some(children.flatten.toList), requiredUpdateTrait)
          }

        case temporalJoin: StreamPhysicalTemporalJoin =>
          val left = temporalJoin.getLeft.asInstanceOf[StreamPhysicalRel]
          val right = temporalJoin.getRight.asInstanceOf[StreamPhysicalRel]

          // the left input required trait depends on it's parent in temporal join
          // the left input will send message to parent
          val requiredUpdateBeforeByParent =
            requiredUpdateTrait.updateKind == UpdateKind.BEFORE_AND_AFTER
          val leftInputModifyKindSet = getModifyKindSet(left)
          val leftRequiredTrait = if (requiredUpdateBeforeByParent) {
            beforeAfterOrNone(leftInputModifyKindSet)
          } else {
            onlyAfterOrNone(leftInputModifyKindSet)
          }
          val newLeftOption = this.visit(left, leftRequiredTrait)

          val rightInputModifyKindSet = getModifyKindSet(right)
          // currently temporal join support changelog stream as the right side
          // so it supports both ONLY_AFTER and BEFORE_AFTER, but prefer ONLY_AFTER
          val newRightOption = this.visit(right, onlyAfterOrNone(rightInputModifyKindSet)) match {
            case Some(newRight) => Some(newRight)
            case None =>
              val beforeAfter = beforeAfterOrNone(rightInputModifyKindSet)
              this.visit(right, beforeAfter)
          }

          (newLeftOption, newRightOption) match {
            case (Some(newLeft), Some(newRight)) =>
              val leftTrait = newLeft.getTraitSet.getTrait(UpdateKindTraitDef.INSTANCE)
              createNewNode(temporalJoin, Some(List(newLeft, newRight)), leftTrait)
            case _ =>
              None
          }

        // if the condition is applied on the upsert key, we can emit whatever the requiredTrait
        // is, because we will filter all records based on the condition that applies to that key
        case calc: StreamPhysicalCalcBase =>
          if (
            requiredUpdateTrait == UpdateKindTrait.ONLY_UPDATE_AFTER &&
            isNonUpsertKeyCondition(calc)
          ) {
            // we don't expect filter to satisfy ONLY_UPDATE_AFTER update kind,
            // to solve the bad case like a single 'cnt < 10' condition after aggregation.
            // See FLINK-9528.
            None
          } else {
            // otherwise, forward UpdateKind requirement
            visitChildren(rel, requiredUpdateTrait) match {
              case None => None
              case Some(children) =>
                val childTrait = children.head.getTraitSet.getTrait(UpdateKindTraitDef.INSTANCE)
                createNewNode(rel, Some(children), childTrait)
            }
          }

        case _: StreamPhysicalCorrelateBase | _: StreamPhysicalLookupJoin |
            _: StreamPhysicalExchange | _: StreamPhysicalExpand |
            _: StreamPhysicalMiniBatchAssigner | _: StreamPhysicalWatermarkAssigner |
            _: StreamPhysicalWindowTableFunction | _: StreamPhysicalMLPredictTableFunction =>
          // transparent forward requiredTrait to children
          visitChildren(rel, requiredUpdateTrait) match {
            case None => None
            case Some(children) =>
              val childTrait = children.head.getTraitSet.getTrait(UpdateKindTraitDef.INSTANCE)
              createNewNode(rel, Some(children), childTrait)
          }

        case union: StreamPhysicalUnion =>
          val children = union.getInputs.map {
            case child: StreamPhysicalRel =>
              val childModifyKindSet = getModifyKindSet(child)
              val requiredChildTrait = if (childModifyKindSet.isInsertOnly) {
                UpdateKindTrait.NONE
              } else {
                requiredUpdateTrait
              }
              this.visit(child, requiredChildTrait)
          }.toList

          if (children.exists(_.isEmpty)) {
            None
          } else {
            val updateKinds = children.flatten
              .map(_.getTraitSet.getTrait(UpdateKindTraitDef.INSTANCE))
            // union can just forward changes, can't actively satisfy to another changelog mode
            val providedTrait = if (updateKinds.forall(k => UpdateKindTrait.NONE == k)) {
              // if all the children is NO_UPDATE, union is NO_UPDATE
              UpdateKindTrait.NONE
            } else {
              // otherwise, merge update kinds.
              val merged = updateKinds
                .map(_.updateKind)
                .reduce {
                  (l, r) =>
                    (l, r) match {
                      case (UpdateKind.NONE, r: UpdateKind) => r
                      case (l: UpdateKind, UpdateKind.NONE) => l
                      case (l: UpdateKind, r: UpdateKind) if l == r => l
                      // UNION doesn't support to union ONLY_UPDATE_AFTER and BEFORE_AND_AFTER inputs
                      case (_, _) => return None
                    }
                }
              new UpdateKindTrait(merged)
            }
            createNewNode(union, Some(children.flatten), providedTrait)
          }

        case normalize: StreamPhysicalChangelogNormalize =>
          // changelog normalize currently only supports input only sending UPDATE_AFTER
          val children = visitChildren(normalize, UpdateKindTrait.ONLY_UPDATE_AFTER)
          // use requiredTrait as providedTrait,
          // because changelog normalize supports all kinds of UpdateKind
          createNewNode(rel, children, requiredUpdateTrait)

        case ts: StreamPhysicalTableSourceScan =>
          // currently only support BEFORE_AND_AFTER if source produces updates
          val providedTrait = UpdateKindTrait.fromChangelogMode(ts.tableSource.getChangelogMode)
          val newSource = createNewNode(rel, Some(List()), providedTrait)
          if (
            providedTrait.equals(UpdateKindTrait.BEFORE_AND_AFTER) &&
            requiredUpdateTrait.equals(UpdateKindTrait.ONLY_UPDATE_AFTER)
          ) {
            // requiring only-after, but the source is CDC source, then drop update_before manually
            val dropUB = new StreamPhysicalDropUpdateBefore(rel.getCluster, rel.getTraitSet, rel)
            createNewNode(dropUB, newSource.map(s => List(s)), requiredUpdateTrait)
          } else {
            newSource
          }

        case _: StreamPhysicalDataStreamScan | _: StreamPhysicalLegacyTableSourceScan |
            _: StreamPhysicalValues =>
          createNewNode(rel, Some(List()), UpdateKindTrait.NONE)

        case scan: StreamPhysicalIntermediateTableScan =>
          val providedTrait = if (scan.intermediateTable.isUpdateBeforeRequired) {
            // we can't drop UPDATE_BEFORE if it is required by other parent blocks
            UpdateKindTrait.BEFORE_AND_AFTER
          } else {
            requiredUpdateTrait
          }
          if (!providedTrait.satisfies(requiredUpdateTrait)) {
            // require ONLY_AFTER but can only provide BEFORE_AND_AFTER
            None
          } else {
            createNewNode(rel, Some(List()), providedTrait)
          }

        case process: StreamPhysicalProcessTableFunction =>
          // Required update traits depend on the table argument declaration,
          // input traits, partition keys, and upsert keys
          val inputArgs = StreamPhysicalProcessTableFunction
            .getProvidedInputArgs(process.getCall)
          val children = process.getInputs
            .map(_.asInstanceOf[StreamPhysicalRel])
            .zipWithIndex
            .map {
              case (child, inputIndex) =>
                // For PTF without table arguments (i.e. values child)
                if (inputArgs.isEmpty) {
                  this.visit(child, UpdateKindTrait.NONE)
                }
                // Derive the required update trait for table arguments
                else {
                  val inputArg = inputArgs.get(inputIndex)
                  val (tableArg, tableArgCall, modifyKindSet) =
                    extractPtfTableArgComponents(process, child, inputArg)
                  val requiredUpdateTrait =
                    if (
                      !modifyKindSet.isInsertOnly && tableArg.is(
                        StaticArgumentTrait.SUPPORT_UPDATES)
                    ) {
                      if (isPtfUpsert(tableArg, tableArgCall, child)) {
                        UpdateKindTrait.ONLY_UPDATE_AFTER
                      } else {
                        UpdateKindTrait.BEFORE_AND_AFTER
                      }
                    } else {
                      UpdateKindTrait.NONE
                    }
                  this.visit(child, requiredUpdateTrait)
                }
            }
            .toList
            .flatten
          // Query PTF for upsert vs. retract
          val providedUpdateTrait = queryPtfChangelogMode(
            process,
            children,
            toChangelogMode(process, Some(requiredUpdateTrait), None),
            UpdateKindTrait.fromChangelogMode,
            UpdateKindTrait.NONE)
          createNewNode(rel, Some(children), providedUpdateTrait)

        case multiJoin: StreamPhysicalMultiJoin =>
          val onlyAfterByParent = requiredUpdateTrait.updateKind == UpdateKind.ONLY_UPDATE_AFTER
          val children = multiJoin.getInputs.zipWithIndex.map {
            case (child, childOrdinal) =>
              val physicalChild = child.asInstanceOf[StreamPhysicalRel]
              val supportOnlyAfter = multiJoin.inputUniqueKeyContainsCommonJoinKey(childOrdinal)
              val inputModifyKindSet = getModifyKindSet(physicalChild)
              if (onlyAfterByParent) {
                if (inputModifyKindSet.contains(ModifyKind.UPDATE) && !supportOnlyAfter) {
                  // the parent requires only-after, however, the multi-join doesn't support this for this input
                  None
                } else {
                  this.visit(physicalChild, onlyAfterOrNone(inputModifyKindSet))
                }
              } else {
                this.visit(physicalChild, beforeAfterOrNone(inputModifyKindSet))
              }
          }

          if (children.exists(_.isEmpty)) {
            None
          } else {
            createNewNode(multiJoin, Some(children.flatten.toList), requiredUpdateTrait)
          }

        case _ =>
          throw new UnsupportedOperationException(
            s"Unsupported visit for ${rel.getClass.getSimpleName}")

      }

    private def visitChildren(
        parent: StreamPhysicalRel,
        requiredChildrenUpdateTrait: UpdateKindTrait): Option[List[StreamPhysicalRel]] = {
      val newChildren = for (child <- parent.getInputs) yield {
        this.visit(child.asInstanceOf[StreamPhysicalRel], requiredChildrenUpdateTrait) match {
          case None =>
            // return None if one of the children can't satisfy
            return None
          case Some(newChild) =>
            val providedUpdateTrait = newChild.getTraitSet.getTrait(UpdateKindTraitDef.INSTANCE)
            if (!providedUpdateTrait.satisfies(requiredChildrenUpdateTrait)) {
              // the provided trait can't satisfy required trait, thus we should return None.
              return None
            }
            newChild
        }
      }
      Some(newChildren.toList)
    }

    private def createNewNode(
        node: StreamPhysicalRel,
        childrenOption: Option[List[StreamPhysicalRel]],
        providedUpdateTrait: UpdateKindTrait): Option[StreamPhysicalRel] = childrenOption match {
      case None =>
        None
      case Some(children) =>
        val modifyKindSetTrait = node.getTraitSet.getTrait(ModifyKindSetTraitDef.INSTANCE)
        val nodeDescription = node.getRelDetailedDescription
        val isUpdateKindValid = providedUpdateTrait.updateKind match {
          case UpdateKind.NONE =>
            !modifyKindSetTrait.modifyKindSet.contains(ModifyKind.UPDATE)
          case UpdateKind.BEFORE_AND_AFTER | UpdateKind.ONLY_UPDATE_AFTER =>
            modifyKindSetTrait.modifyKindSet.contains(ModifyKind.UPDATE)
        }
        if (!isUpdateKindValid) {
          throw new TableException(
            s"UpdateKindTrait $providedUpdateTrait conflicts with " +
              s"ModifyKindSetTrait $modifyKindSetTrait. " +
              s"This is a bug in planner, please file an issue. \n" +
              s"Current node is $nodeDescription.")
        }

        val newTraitSet = node.getTraitSet.plus(providedUpdateTrait)
        Some(node.copy(newTraitSet, children).asInstanceOf[StreamPhysicalRel])
    }

    /**
     * Try all possible rank strategies and return the first viable new node.
     *
     * @param rankStrategies
     *   all possible supported rank strategy by current node
     * @param requiredUpdateKindTrait
     *   the required UpdateKindTrait by parent of rank node
     * @param applyRankStrategy
     *   a function to apply rank strategy to get a new copied rank node
     */
    private def visitRankStrategies(
        rankStrategies: Seq[RankProcessStrategy],
        requiredUpdateKindTrait: UpdateKindTrait,
        applyRankStrategy: RankProcessStrategy => StreamPhysicalRel): Option[StreamPhysicalRel] = {
      // go pass every RankProcessStrategy, apply the rank strategy to get a new copied rank node,
      // return the first satisfied converted node
      for (strategy <- rankStrategies) {
        val requiredChildrenTrait = strategy match {
          case _: UpdateFastStrategy => UpdateKindTrait.ONLY_UPDATE_AFTER
          case _: RetractStrategy => UpdateKindTrait.BEFORE_AND_AFTER
          case _: AppendFastStrategy => UpdateKindTrait.NONE
        }
        val node = applyRankStrategy(strategy)
        val children = visitChildren(node, requiredChildrenTrait)
        val newNode = createNewNode(node, children, requiredUpdateKindTrait)
        if (newNode.isDefined) {
          return newNode
        }
      }
      None
    }

    private def visitSink(
        sink: StreamPhysicalRel,
        sinkRequiredTraits: Seq[UpdateKindTrait]): Option[StreamPhysicalRel] = {
      val children = sinkRequiredTraits.flatMap(t => visitChildren(sink, t))
      if (children.isEmpty) {
        None
      } else {
        val sinkTrait = sink.getTraitSet.plus(UpdateKindTrait.NONE)
        Some(sink.copy(sinkTrait, children.head).asInstanceOf[StreamPhysicalRel])
      }
    }

    /**
     * Infer sink required traits by the sink node and its input. Sink required traits is based on
     * the sink node's changelog mode, the only exception is when sink's pk(s) not exactly the same
     * as the changeLogUpsertKeys and sink' changelog mode is ONLY_UPDATE_AFTER.
     */
    private def inferSinkRequiredTraits(sink: StreamPhysicalSink): Seq[UpdateKindTrait] = {
      val childModifyKindSet = getModifyKindSet(sink.getInput)
      val onlyAfter = onlyAfterOrNone(childModifyKindSet)
      val beforeAndAfter = beforeAfterOrNone(childModifyKindSet)
      val sinkTrait = UpdateKindTrait.fromChangelogMode(
        sink.tableSink.getChangelogMode(childModifyKindSet.toDefaultChangelogMode))

      val sinkRequiredTraits = if (sinkTrait.equals(ONLY_UPDATE_AFTER)) {
        // if sink's pk(s) are not exactly match input changeLogUpsertKeys then it will fallback
        // to beforeAndAfter mode for the correctness
        var requireBeforeAndAfter: Boolean = false
        val sinkDefinedPks = sink.contextResolvedTable.getResolvedSchema.getPrimaryKeyIndexes

        if (sinkDefinedPks.nonEmpty) {
          val sinkPks = ImmutableBitSet.of(sinkDefinedPks: _*)
          val fmq = FlinkRelMetadataQuery.reuseOrCreate(sink.getCluster.getMetadataQuery)
          val changeLogUpsertKeys = fmq.getUpsertKeys(sink.getInput)
          // if input is UA only, primary key != upsert key (upsert key can be null) we should
          // fallback to beforeAndAfter.
          // Notice: even sink pk(s) contains input upsert key we cannot optimize to UA only,
          // this differs from batch job's unique key inference
          if (changeLogUpsertKeys == null || !changeLogUpsertKeys.exists(_.equals(sinkPks))) {
            requireBeforeAndAfter = true
          }
        }
        if (requireBeforeAndAfter) {
          Seq(beforeAndAfter)
        } else {
          Seq(onlyAfter, beforeAndAfter)
        }
      } else if (sinkTrait.equals(BEFORE_AND_AFTER)) {
        Seq(beforeAndAfter)
      } else {
        Seq(UpdateKindTrait.NONE)
      }
      sinkRequiredTraits
    }

    /**
     * Analyze whether to enable upsertMaterialize or not. In these case will return true:
     *   1. when `TABLE_EXEC_SINK_UPSERT_MATERIALIZE` set to FORCE and sink's primary key nonempty.
     *      2. when `TABLE_EXEC_SINK_UPSERT_MATERIALIZE` set to AUTO and sink's primary key doesn't
     *      contain upsertKeys of the input update stream.
     */
    private def analyzeUpsertMaterializeStrategy(sink: StreamPhysicalSink): Boolean = {
      val tableConfig = unwrapTableConfig(sink)
      val inputChangelogMode =
        ChangelogPlanUtils.getChangelogMode(sink.getInput.asInstanceOf[StreamPhysicalRel]).get
      val primaryKeys = sink.contextResolvedTable.getResolvedSchema.getPrimaryKeyIndexes
      val upsertMaterialize =
        tableConfig.get(ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE) match {
          case UpsertMaterialize.FORCE => primaryKeys.nonEmpty
          case UpsertMaterialize.NONE => false
          case UpsertMaterialize.AUTO =>
            val sinkAcceptInsertOnly = sink.tableSink
              .getChangelogMode(inputChangelogMode)
              .containsOnly(RowKind.INSERT)
            val inputInsertOnly = inputChangelogMode.containsOnly(RowKind.INSERT)

            if (!sinkAcceptInsertOnly && !inputInsertOnly && primaryKeys.nonEmpty) {
              val pks = ImmutableBitSet.of(primaryKeys: _*)
              val fmq = FlinkRelMetadataQuery.reuseOrCreate(sink.getCluster.getMetadataQuery)
              val changeLogUpsertKeys = fmq.getUpsertKeys(sink.getInput)
              // if input has update and primary key != upsert key (upsert key can be null) we should
              // enable upsertMaterialize. An optimize is: do not enable upsertMaterialize when sink
              // pk(s) contains input changeLogUpsertKeys
              if (changeLogUpsertKeys == null || !changeLogUpsertKeys.exists(pks.contains)) {
                true
              } else {
                false
              }
            } else {
              false
            }
        }
      upsertMaterialize
    }
  }

  /**
   * A visitor which will try to satisfy the required [[DeleteKindTrait]] from root.
   *
   * <p>After traversed by this visitor, every node should have a correct [[DeleteKindTrait]] or
   * returns None if the planner doesn't support to satisfy the required [[DeleteKindTrait]].
   */
  private class SatisfyDeleteKindTraitVisitor(private val context: StreamOptimizeContext) {

    /**
     * Try to satisfy the required [[DeleteKindTrait]] from root.
     *
     * <p>Each node will first require a DeleteKindTrait to its children. The required
     * DeleteKindTrait may come from the node's parent, or come from the node itself, depending on
     * whether the node will destroy the trait provided by children or pass the trait from children.
     *
     * <p>If the node will pass the children's DeleteKindTrait without destroying it, then return a
     * new node with new inputs and forwarded DeleteKindTrait.
     *
     * <p>If the node will destroy the children's UpdateKindTrait, then the node itself needs to be
     * converted, or a new node should be generated to satisfy the required trait, such as marking
     * itself not to generate UPDATE_BEFORE, or generating a new node to filter UPDATE_BEFORE.
     *
     * @param rel
     *   the node who should satisfy the requiredTrait
     * @param requiredTrait
     *   the required DeleteKindTrait
     * @return
     *   A converted node which satisfies required traits by input nodes of current node. Or None if
     *   required traits cannot be satisfied.
     */
    def visit(rel: StreamPhysicalRel, requiredTrait: DeleteKindTrait): Option[StreamPhysicalRel] =
      rel match {
        case sink: StreamPhysicalSink =>
          val sinkRequiredTraits = inferSinkRequiredTraits(sink)
          visitSink(sink, sinkRequiredTraits)

        case sink: StreamPhysicalLegacySink[_] =>
          val childModifyKindSet = getModifyKindSet(sink.getInput)
          val fullDelete = fullDeleteOrNone(childModifyKindSet)
          visitSink(sink, Seq(fullDelete))

        case _: StreamPhysicalGroupAggregate | _: StreamPhysicalGroupTableAggregate |
            _: StreamPhysicalLimit | _: StreamPhysicalPythonGroupAggregate |
            _: StreamPhysicalPythonGroupTableAggregate | _: StreamPhysicalGroupWindowAggregateBase |
            _: StreamPhysicalWindowAggregate | _: StreamPhysicalSort | _: StreamPhysicalRank |
            _: StreamPhysicalSortLimit | _: StreamPhysicalTemporalJoin |
            _: StreamPhysicalCorrelateBase | _: StreamPhysicalLookupJoin |
            _: StreamPhysicalWatermarkAssigner | _: StreamPhysicalWindowTableFunction |
            _: StreamPhysicalWindowRank | _: StreamPhysicalWindowDeduplicate |
            _: StreamPhysicalTemporalSort | _: StreamPhysicalMatch |
            _: StreamPhysicalOverAggregate | _: StreamPhysicalIntervalJoin |
            _: StreamPhysicalPythonOverAggregate | _: StreamPhysicalWindowJoin |
            _: StreamPhysicalMLPredictTableFunction =>
          // if not explicitly supported, all operators require full deletes if there are updates
          val children = rel.getInputs.map {
            case child: StreamPhysicalRel =>
              val childModifyKindSet = getModifyKindSet(child)
              this.visit(child, fullDeleteOrNone(childModifyKindSet))
          }.toList
          createNewNode(rel, Some(children.flatten), fullDeleteOrNone(getModifyKindSet(rel)))

        case process: StreamPhysicalProcessTableFunction =>
          // Required delete traits depend on the table argument declaration,
          // input traits, partition keys, and upsert keys
          val call = process.getCall
          val inputArgs = StreamPhysicalProcessTableFunction
            .getProvidedInputArgs(call)
          val children = process.getInputs
            .map(_.asInstanceOf[StreamPhysicalRel])
            .zipWithIndex
            .map {
              case (child, inputIndex) =>
                // For PTF without table arguments (i.e. values child)
                if (inputArgs.isEmpty) {
                  this.visit(child, DeleteKindTrait.NONE)
                }
                // Derive the required delete trait for table arguments
                else {
                  val inputArg = inputArgs.get(inputIndex)
                  val (tableArg, tableArgCall, modifyKindSet) =
                    extractPtfTableArgComponents(process, child, inputArg)
                  if (
                    tableArg.is(StaticArgumentTrait.SUPPORT_UPDATES)
                    && isPtfUpsert(tableArg, tableArgCall, child)
                    && !tableArg.is(StaticArgumentTrait.REQUIRE_FULL_DELETE)
                  ) {
                    this
                      .visit(child, deleteOnKeyOrNone(modifyKindSet))
                      .orElse(this.visit(child, fullDeleteOrNone(modifyKindSet)))
                  } else {
                    this.visit(child, fullDeleteOrNone(modifyKindSet))
                  }
                }
            }
            .toList
            .flatten
          val modifyTrait = getModifyKindSet(rel)
          // Query the PTF for full vs. partial deletes
          val providedDeleteTrait = queryPtfChangelogMode(
            process,
            children,
            toChangelogMode(process, None, Some(requiredTrait)),
            mode =>
              if (mode.keyOnlyDeletes()) {
                deleteOnKeyOrNone(modifyTrait)
              } else {
                fullDeleteOrNone(modifyTrait)
              },
            fullDeleteOrNone(modifyTrait)
          )
          createNewNode(process, Some(children), providedDeleteTrait)

        case join: StreamPhysicalJoin =>
          val children = join.getInputs.zipWithIndex.map {
            case (child, childOrdinal) =>
              val physicalChild = child.asInstanceOf[StreamPhysicalRel]
              val supportsDeleteByKey = join.inputUniqueKeyContainsJoinKey(childOrdinal)
              val inputModifyKindSet = getModifyKindSet(physicalChild)
              if (supportsDeleteByKey && requiredTrait == DELETE_BY_KEY) {
                this
                  .visit(physicalChild, deleteOnKeyOrNone(inputModifyKindSet))
                  .orElse(this.visit(physicalChild, fullDeleteOrNone(inputModifyKindSet)))
              } else {
                this.visit(physicalChild, fullDeleteOrNone(inputModifyKindSet))
              }
          }
          if (children.exists(_.isEmpty)) {
            None
          } else {
            val childRels = children.flatten.toList
            if (childRels.exists(r => getDeleteKind(r) == DeleteKind.DELETE_BY_KEY)) {
              createNewNode(join, Some(childRels), deleteOnKeyOrNone(getModifyKindSet(rel)))
            } else {
              createNewNode(join, Some(childRels), fullDeleteOrNone(getModifyKindSet(rel)))
            }
          }

        // if the condition is applied on the upsert key, we can emit whatever the requiredTrait
        // is, because we will filter all records based on the condition that applies to that key
        case calc: StreamPhysicalCalcBase =>
          if (
            requiredTrait == DeleteKindTrait.DELETE_BY_KEY &&
            isNonUpsertKeyCondition(calc)
          ) {
            None
          } else {
            // otherwise, forward DeleteKind requirement
            visitChildren(rel, requiredTrait) match {
              case None => None
              case Some(children) =>
                val childTrait = children.head.getTraitSet.getTrait(DeleteKindTraitDef.INSTANCE)
                createNewNode(rel, Some(children), childTrait)
            }
          }

        case _: StreamPhysicalExchange | _: StreamPhysicalExpand |
            _: StreamPhysicalMiniBatchAssigner | _: StreamPhysicalDropUpdateBefore =>
          // transparent forward requiredTrait to children
          visitChildren(rel, requiredTrait) match {
            case None => None
            case Some(children) =>
              val childTrait = children.head.getTraitSet.getTrait(DeleteKindTraitDef.INSTANCE)
              createNewNode(rel, Some(children), childTrait)
          }

        case union: StreamPhysicalUnion =>
          val children = union.getInputs.map {
            case child: StreamPhysicalRel =>
              val childModifyKindSet = getModifyKindSet(child)
              val requiredChildTrait = if (!childModifyKindSet.contains(ModifyKind.DELETE)) {
                DeleteKindTrait.NONE
              } else {
                requiredTrait
              }
              this.visit(child, requiredChildTrait)
          }.toList

          if (children.exists(_.isEmpty)) {
            None
          } else {
            val deleteKinds = children.flatten
              .map(_.getTraitSet.getTrait(DeleteKindTraitDef.INSTANCE))
            // union can just forward changes, can't actively satisfy to another changelog mode
            val providedTrait = if (deleteKinds.forall(k => DeleteKindTrait.NONE == k)) {
              // if all the children is NONE, union is NONE
              DeleteKindTrait.NONE
            } else {
              // otherwise, merge update kinds.
              val merged = deleteKinds
                .map(_.deleteKind)
                .reduce {
                  (l, r) =>
                    (l, r) match {
                      case (DeleteKind.NONE, r: DeleteKind) => r
                      case (l: DeleteKind, DeleteKind.NONE) => l
                      case (l: DeleteKind, r: DeleteKind) =>
                        if (l == r) {
                          l
                        } else {
                          // if any of the union input produces DELETE_BY_KEY, the union produces
                          // delete by key
                          DeleteKind.DELETE_BY_KEY
                        }
                    }
                }
              new DeleteKindTrait(merged)
            }
            createNewNode(union, Some(children.flatten), providedTrait)
          }

        case normalize: StreamPhysicalChangelogNormalize =>
          // if
          // 1. we don't need to produce UPDATE_BEFORE,
          // 2. children can satisfy the required delete trait,
          // 3. the normalize doesn't have filter condition which we'd lose,
          // 4. we don't use metadata columns
          // we can skip ChangelogNormalize
          if (!ChangelogNormalizeRequirementResolver.isRequired(normalize)) {
            visitChildren(normalize, requiredTrait) match {
              case Some(children) =>
                val input = children.head match {
                  case exchange: StreamPhysicalExchange =>
                    exchange.getInput
                  case _ =>
                    normalize.getInput
                }
                return Some(input.asInstanceOf[StreamPhysicalRel])
              case _ =>
            }
          }
          val childModifyKindTrait = getModifyKindSet(rel.getInput(0))

          // prefer delete by key, but accept both
          val children = visitChildren(normalize, deleteOnKeyOrNone(childModifyKindTrait))
            .orElse(visitChildren(normalize, fullDeleteOrNone(childModifyKindTrait)))

          // changelog normalize produces full deletes
          createNewNode(rel, children, fullDeleteOrNone(getModifyKindSet(rel)))

        case ts: StreamPhysicalTableSourceScan =>
          // currently only support BEFORE_AND_AFTER if source produces updates
          val providedTrait = DeleteKindTrait.fromChangelogMode(ts.tableSource.getChangelogMode)
          createNewNode(rel, Some(List()), providedTrait)

        case _: StreamPhysicalDataStreamScan | _: StreamPhysicalLegacyTableSourceScan |
            _: StreamPhysicalValues =>
          createNewNode(rel, Some(List()), DeleteKindTrait.NONE)

        case _: StreamPhysicalIntermediateTableScan =>
          createNewNode(rel, Some(List()), fullDeleteOrNone(getModifyKindSet(rel)))

        case multiJoin: StreamPhysicalMultiJoin =>
          val children = multiJoin.getInputs.zipWithIndex.map {
            case (child, childOrdinal) =>
              val physicalChild = child.asInstanceOf[StreamPhysicalRel]
              val supportsDeleteByKey = multiJoin.inputUniqueKeyContainsCommonJoinKey(childOrdinal)
              val inputModifyKindSet = getModifyKindSet(physicalChild)
              if (supportsDeleteByKey && requiredTrait == DELETE_BY_KEY) {
                this
                  .visit(physicalChild, deleteOnKeyOrNone(inputModifyKindSet))
                  .orElse(this.visit(physicalChild, fullDeleteOrNone(inputModifyKindSet)))
              } else {
                this.visit(physicalChild, fullDeleteOrNone(inputModifyKindSet))
              }
          }
          if (children.exists(_.isEmpty)) {
            None
          } else {
            val childRels = children.flatten.toList
            if (childRels.exists(r => getDeleteKind(r) == DeleteKind.DELETE_BY_KEY)) {
              createNewNode(multiJoin, Some(childRels), deleteOnKeyOrNone(getModifyKindSet(rel)))
            } else {
              createNewNode(multiJoin, Some(childRels), fullDeleteOrNone(getModifyKindSet(rel)))
            }
          }

        case _ =>
          throw new UnsupportedOperationException(
            s"Unsupported visit for ${rel.getClass.getSimpleName}")

      }

    private def visitChildren(
        parent: StreamPhysicalRel,
        requiredChildrenTrait: DeleteKindTrait): Option[List[StreamPhysicalRel]] = {
      val newChildren = for (child <- parent.getInputs) yield {
        this.visit(child.asInstanceOf[StreamPhysicalRel], requiredChildrenTrait) match {
          case None =>
            // return None if one of the children can't satisfy
            return None
          case Some(newChild) =>
            val providedTrait = newChild.getTraitSet.getTrait(DeleteKindTraitDef.INSTANCE)
            if (!providedTrait.satisfies(requiredChildrenTrait)) {
              // the provided trait can't satisfy required trait, thus we should return None.
              return None
            }
            newChild
        }
      }
      Some(newChildren.toList)
    }

    private def createNewNode(
        node: StreamPhysicalRel,
        childrenOption: Option[List[StreamPhysicalRel]],
        providedDeleteTrait: DeleteKindTrait): Option[StreamPhysicalRel] = childrenOption match {
      case None =>
        None
      case Some(children) =>
        val modifyKindSetTrait = node.getTraitSet.getTrait(ModifyKindSetTraitDef.INSTANCE)
        val nodeDescription = node.getRelDetailedDescription
        val isDeleteKindValid = providedDeleteTrait.deleteKind match {
          case DeleteKind.NONE =>
            !modifyKindSetTrait.modifyKindSet.contains(ModifyKind.DELETE)
          case DeleteKind.DELETE_BY_KEY | DeleteKind.FULL_DELETE =>
            modifyKindSetTrait.modifyKindSet.contains(ModifyKind.DELETE)
        }
        if (!isDeleteKindValid) {
          throw new TableException(
            s"DeleteKindTrait $providedDeleteTrait conflicts with " +
              s"ModifyKindSetTrait $modifyKindSetTrait. " +
              s"This is a bug in planner, please file an issue. \n" +
              s"Current node is $nodeDescription.")
        }
        val newTraitSet = node.getTraitSet.plus(providedDeleteTrait)
        Some(node.copy(newTraitSet, children).asInstanceOf[StreamPhysicalRel])
    }

    private def visitSink(
        sink: StreamPhysicalRel,
        sinkRequiredTraits: Seq[DeleteKindTrait]): Option[StreamPhysicalRel] = {
      val children = sinkRequiredTraits.flatMap(t => visitChildren(sink, t))
      if (children.isEmpty) {
        None
      } else {
        val sinkTrait = sink.getTraitSet.plus(DeleteKindTrait.NONE)
        Some(sink.copy(sinkTrait, children.head).asInstanceOf[StreamPhysicalRel])
      }
    }

    /**
     * Infer sink required traits by the sink node and its input. Sink required traits is based on
     * the sink node's changelog mode, the only exception is when sink's pk(s) not exactly the same
     * as the changeLogUpsertKeys and sink' changelog mode is DELETE_BY_KEY.
     */
    private def inferSinkRequiredTraits(sink: StreamPhysicalSink): Seq[DeleteKindTrait] = {
      val childModifyKindSet = getModifyKindSet(sink.getInput)
      val sinkChangelogMode =
        sink.tableSink.getChangelogMode(childModifyKindSet.toDefaultChangelogMode)

      val sinkDeleteTrait = DeleteKindTrait.fromChangelogMode(sinkChangelogMode)

      val fullDelete = fullDeleteOrNone(childModifyKindSet)
      if (sinkDeleteTrait.equals(DeleteKindTrait.DELETE_BY_KEY)) {
        if (areUpsertKeysDifferentFromPk(sink)) {
          Seq(fullDelete)
        } else {
          Seq(sinkDeleteTrait, fullDelete)
        }
      } else {
        Seq(fullDelete)
      }
    }

    // -------------------------------------------------------------------------------------------

    private def areUpsertKeysDifferentFromPk(sink: StreamPhysicalSink) = {
      // if sink's pk(s) are not exactly match input changeLogUpsertKeys then it will fallback
      // to beforeAndAfter mode for the correctness
      var upsertKeyDifferentFromPk: Boolean = false
      val sinkDefinedPks = sink.contextResolvedTable.getResolvedSchema.getPrimaryKeyIndexes

      if (sinkDefinedPks.nonEmpty) {
        val sinkPks = ImmutableBitSet.of(sinkDefinedPks: _*)
        val fmq = FlinkRelMetadataQuery.reuseOrCreate(sink.getCluster.getMetadataQuery)
        val changeLogUpsertKeys = fmq.getUpsertKeys(sink.getInput)
        // if input is UA only, primary key != upsert key (upsert key can be null) we should
        // fallback to beforeAndAfter.
        // Notice: even sink pk(s) contains input upsert key we cannot optimize to UA only,
        // this differs from batch job's unique key inference
        if (changeLogUpsertKeys == null || !changeLogUpsertKeys.exists(_.equals(sinkPks))) {
          upsertKeyDifferentFromPk = true
        }
      }
      upsertKeyDifferentFromPk
    }
  }

  private def isNonUpsertKeyCondition(calc: StreamPhysicalCalcBase): Boolean = {
    val program = calc.getProgram
    if (program.getCondition == null) {
      return false
    }

    val condition = program.expandLocalRef(calc.getProgram.getCondition)
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(calc.getCluster.getMetadataQuery)
    val upsertKeys = fmq.getUpsertKeys(calc.getInput())
    if (upsertKeys == null || upsertKeys.isEmpty) {
      // there are no upsert keys, so all columns are non-primary key columns
      true
    } else {
      val upsertKey = upsertKeys.head
      RexNodeExtractor
        .extractRefInputFields(JavaScalaConversionUtil.toJava(Seq(condition)))
        .exists(i => !upsertKey.get(i))
    }
  }

  private def getModifyKindSet(node: RelNode): ModifyKindSet = {
    val modifyKindSetTrait = node.getTraitSet.getTrait(ModifyKindSetTraitDef.INSTANCE)
    modifyKindSetTrait.modifyKindSet
  }

  private def getDeleteKind(node: RelNode): DeleteKind = {
    val deleteKindTrait = node.getTraitSet.getTrait(DeleteKindTraitDef.INSTANCE)
    deleteKindTrait.deleteKind
  }

  // ----------------------------------------------------------------------------------------------
  // PTF helper methods
  // ----------------------------------------------------------------------------------------------

  private def toChangelogMode(
      node: StreamPhysicalRel,
      updateKindTrait: Option[UpdateKindTrait],
      deleteKindTrait: Option[DeleteKindTrait]): ChangelogMode = {
    val modeBuilder = ChangelogMode.newBuilder()
    val givenMode = ChangelogPlanUtils
      .getChangelogMode(node)
      .getOrElse(
        throw new IllegalStateException(
          s"Unable to derive changelog mode from node $node. This is a bug."))
    givenMode.getContainedKinds.foreach(modeBuilder.addContainedKind)
    updateKindTrait match {
      case None =>
      case Some(updateKindTrait: UpdateKindTrait) =>
        if (updateKindTrait == BEFORE_AND_AFTER) {
          modeBuilder.addContainedKind(RowKind.UPDATE_BEFORE)
        }
    }
    deleteKindTrait match {
      case None =>
      case Some(deleteKindTrait: DeleteKindTrait) =>
        if (deleteKindTrait == DELETE_BY_KEY) {
          modeBuilder.keyOnlyDeletes(true)
        }
    }
    modeBuilder.build()
  }

  private def isPtfUpsert(
      tableArg: StaticArgument,
      tableArgCall: RexTableArgCall,
      input: StreamPhysicalRel): Boolean = {
    val partitionKeys = ImmutableBitSet.of(tableArgCall.getPartitionKeys: _*)
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(input.getCluster.getMetadataQuery)
    val upsertKeys = fmq.getUpsertKeys(input)
    if (
      upsertKeys == null || partitionKeys.isEmpty || !upsertKeys.contains(partitionKeys)
      || tableArg.is(StaticArgumentTrait.REQUIRE_UPDATE_BEFORE)
    ) {
      false
    } else {
      true
    }
  }

  private def extractPtfTableArgComponents(
      process: StreamPhysicalProcessTableFunction,
      child: StreamPhysicalRel,
      inputArg: Ord[StaticArgument]): (StaticArgument, RexTableArgCall, ModifyKindSet) = {
    val tableArg = inputArg.e
    val call = process.getCall
    val tableArgCall = call.operands.get(inputArg.i).asInstanceOf[RexTableArgCall]
    val modifyKindSet = getModifyKindSet(child)
    (tableArg, tableArgCall, modifyKindSet)
  }

  private def toPtfChangelogContext(
      process: StreamPhysicalProcessTableFunction,
      inputChangelogModes: List[ChangelogMode],
      outputChangelogMode: ChangelogMode): ChangelogContext = {
    val udfCall = StreamPhysicalProcessTableFunction.toUdfCall(process.getCall)
    val inputTimeColumns = StreamPhysicalProcessTableFunction.toInputTimeColumns(process.getCall)
    val callContext = StreamPhysicalProcessTableFunction.toCallContext(
      udfCall,
      inputTimeColumns,
      inputChangelogModes,
      outputChangelogMode)

    // Expose a simplified context to let users focus on important characteristics.
    // If necessary, we can expose the full CallContext in the future.
    new ChangelogContext {
      override def getTableChangelogMode(pos: Int): ChangelogMode = {
        val tableSemantics = callContext.getTableSemantics(pos).orElse(null)
        if (tableSemantics == null) {
          return null
        }
        tableSemantics.changelogMode().orElse(null)
      }

      override def getRequiredChangelogMode: ChangelogMode = {
        callContext.getOutputChangelogMode.orElse(null)
      }
    }
  }

  private def queryPtfChangelogMode[T](
      process: StreamPhysicalProcessTableFunction,
      children: List[StreamPhysicalRel],
      requiredChangelogMode: ChangelogMode,
      toTraitSet: ChangelogMode => T,
      defaultTraitSet: T): T = {
    val call = process.getCall
    val definition = ShortcutUtils.unwrapFunctionDefinition(call)
    definition match {
      case changelogFunction: ChangelogFunction =>
        val inputChangelogModes = children.map(toChangelogMode(_, None, None))
        val changelogContext =
          toPtfChangelogContext(process, inputChangelogModes, requiredChangelogMode)
        val changelogMode = changelogFunction.getChangelogMode(changelogContext)
        if (!changelogMode.containsOnly(RowKind.INSERT)) {
          verifyPtfTableArgsForUpdates(call)
        }
        toTraitSet(changelogMode)
      case _ =>
        defaultTraitSet
    }
  }

  private def verifyPtfTableArgsForUpdates(call: RexCall): Unit = {
    StreamPhysicalProcessTableFunction
      .getProvidedInputArgs(call)
      .map(_.e)
      .foreach {
        tableArg =>
          if (tableArg.is(StaticArgumentTrait.TABLE_AS_ROW)) {
            throw new ValidationException(
              s"PTFs that take table arguments with row semantics don't support updating output. " +
                s"Table argument '${tableArg.getName}' of function '${call.getOperator.toString}' " +
                s"must use set semantics.")
          }
      }
  }
}
