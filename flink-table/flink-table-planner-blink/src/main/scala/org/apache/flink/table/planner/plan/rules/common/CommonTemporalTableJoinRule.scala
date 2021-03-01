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
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalRel, FlinkLogicalSnapshot}
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalLookupJoin, StreamPhysicalTemporalJoin}
import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType
import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil

import org.apache.calcite.rex.{RexCorrelVariable, RexFieldAccess}

/**
  * Base implementation that matches temporal join node.
  *
  * <p> The initial temporal table join (FOR SYSTEM_TIME AS OF) is a Correlate, rewrite it into
  * a Join to make join condition can be pushed-down. The join will be translated into
  * [[StreamPhysicalLookupJoin]] in physical or translated into [[StreamPhysicalTemporalJoin]].
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

    val tableScan = TemporalJoinUtil.getTableScan(snapshotInput)
    val snapshotOnLookupSource = tableScan match {
      case Some(scan) =>
        TemporalJoinUtil.isTableSourceScan(scan) && TemporalJoinUtil.isLookupTableSource(scan)
      case _ => false
    }

    isProcessingTime && snapshotOnLookupSource
  }
}
