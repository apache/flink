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

package org.apache.flink.table.planner.plan.rules.common

import org.apache.flink.table.api.TableException
import org.apache.flink.table.connector.source.LookupTableSource
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalLegacyTableSourceScan, FlinkLogicalRel, FlinkLogicalSnapshot, FlinkLogicalTableSourceScan}
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamExecLookupJoin, StreamExecTemporalJoin}
import org.apache.flink.table.planner.plan.schema.{LegacyTableSourceTable, TableSourceTable, TimeIndicatorRelDataType}
import org.apache.flink.table.sources.LookupableTableSource

import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.logical.{LogicalProject, LogicalTableScan}
import org.apache.calcite.rex.{RexCorrelVariable, RexFieldAccess}

/**
  * Base implementation that matches temporal join node.
  *
  * <p> The initial temporal table join (FOR SYSTEM_TIME AS OF) is a Correlate, rewrite it into
  * a Join to make join condition can be pushed-down. The join will be translated into
  * [[StreamExecLookupJoin]] in physical or translated into [[StreamExecTemporalJoin]].
  */
trait CommonTemporalTableJoinRule {

  protected def matches(snapshot: FlinkLogicalSnapshot): Boolean = {

    // period specification check
    snapshot.getPeriod match {
      // it should be left table's field and is a time attribute
      case r: RexFieldAccess
        if r.getType.isInstanceOf[TimeIndicatorRelDataType] &&
          r.getReferenceExpr.isInstanceOf[RexCorrelVariable] => // pass
      case _ =>
        throw new TableException("Temporal table join currently only supports " +
          "'FOR SYSTEM_TIME AS OF' left table's time attribute field.")
    }

    true
  }

  protected def canConvertToLookupJoin(
      snapshot: FlinkLogicalSnapshot,
      snapshotInput: FlinkLogicalRel): Boolean = {
    val isProcessingTime = snapshot.getPeriod.getType match {
      case t: TimeIndicatorRelDataType if !t.isEventTime => true
      case _ => false
    }

    val tableScan = getTableScan(snapshotInput)
    val snapshotOnLookupSource = tableScan match {
      case Some(scan) => isTableSourceScan(scan) && isLookupTableSource(scan)
      case _ => false
    }

    isProcessingTime && snapshotOnLookupSource
  }

  private def getTableScan(snapshotInput: RelNode): Option[TableScan] = {
    snapshotInput match {
      case tableScan: TableScan
      => Some(tableScan)
      // computed column on lookup table
      case project: LogicalProject if trimHep(project.getInput).isInstanceOf[TableScan]
      => Some(trimHep(project.getInput).asInstanceOf[TableScan])
      case _ => None
    }
  }

  private def isTableSourceScan(relNode: RelNode): Boolean = {
    relNode match {
      case r: LogicalTableScan =>
        val table = r.getTable
        table match {
          case _: LegacyTableSourceTable[Any] | _: TableSourceTable => true
          case _ => false
        }
      case _: FlinkLogicalLegacyTableSourceScan | _: FlinkLogicalTableSourceScan => true
      case _ => false
    }
  }

  private def isLookupTableSource(relNode: RelNode): Boolean = relNode match {
    case scan: FlinkLogicalLegacyTableSourceScan =>
      scan.tableSource.isInstanceOf[LookupableTableSource[_]]
    case scan: FlinkLogicalTableSourceScan =>
      scan.tableSource.isInstanceOf[LookupTableSource]
    case scan: LogicalTableScan =>
      scan.getTable match {
        case table: LegacyTableSourceTable[_] =>
          table.tableSource.isInstanceOf[LookupableTableSource[_]]
        case table: TableSourceTable =>
          table.tableSource.isInstanceOf[LookupTableSource]
        case _ => false
      }
    case _ => false
  }

  /** Trim out the HepRelVertex wrapper and get current relational expression. */
  protected def trimHep(node: RelNode): RelNode = {
    node match {
      case hepRelVertex: HepRelVertex =>
        hepRelVertex.getCurrentRel
      case _ => node
    }
  }
}
