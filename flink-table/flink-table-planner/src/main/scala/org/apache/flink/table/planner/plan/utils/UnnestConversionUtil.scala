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
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.functions.BuiltInFunctionDefinitions
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.utils.ShortcutUtils
import org.apache.flink.table.runtime.functions.table.UnnestRowsFunction
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils.toRowType

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Uncollect
import org.apache.calcite.rel.logical._

import java.util.Collections

/** Unnest conversion utils. */
trait UnnestConversionUtil {

  protected def getRel(rel: RelNode): RelNode = {
    rel match {
      case vertex: HepRelVertex => vertex.getCurrentRel
      case _ => rel
    }
  }

  protected def convert(
      relNode: RelNode,
      cluster: RelOptCluster,
      traitSet: RelTraitSet): RelNode = {
    relNode match {
      case rs: HepRelVertex =>
        convert(getRel(rs), cluster, traitSet)

      case f: LogicalProject =>
        f.copy(f.getTraitSet, ImmutableList.of(convert(getRel(f.getInput), cluster, traitSet)))

      case f: LogicalFilter =>
        f.copy(f.getTraitSet, ImmutableList.of(convert(getRel(f.getInput), cluster, traitSet)))

      case uc: Uncollect =>
        // convert Uncollect into TableFunctionScan
        val typeFactory = ShortcutUtils.unwrapTypeFactory(cluster)
        val relDataType = uc.getInput.getRowType.getFieldList.get(0).getValue
        val logicalType = FlinkTypeFactory.toLogicalType(relDataType)

        val sqlFunction =
          BridgingSqlFunction.of(cluster, BuiltInFunctionDefinitions.INTERNAL_UNNEST_ROWS)

        val rexCall = cluster.getRexBuilder.makeCall(
          typeFactory.createFieldTypeFromLogicalType(
            toRowType(UnnestRowsFunction.getUnnestedType(logicalType))),
          sqlFunction,
          getRel(uc.getInput).asInstanceOf[LogicalProject].getProjects
        )

        new LogicalTableFunctionScan(
          cluster,
          traitSet,
          Collections.emptyList(),
          rexCall,
          null,
          rexCall.getType,
          null)
    }
  }
}
