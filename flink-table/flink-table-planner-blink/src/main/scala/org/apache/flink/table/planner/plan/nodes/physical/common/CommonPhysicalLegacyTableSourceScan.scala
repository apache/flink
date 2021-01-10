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

package org.apache.flink.table.planner.plan.nodes.physical.common

import org.apache.flink.api.dag.Transformation
import org.apache.flink.table.planner.plan.schema.LegacyTableSourceTable
import org.apache.flink.table.sources.TableSource

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan

import scala.collection.JavaConverters._

/**
  * Base physical RelNode to read data from an external source defined by a [[TableSource]].
  */
abstract class CommonPhysicalLegacyTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relOptTable: LegacyTableSourceTable[_])
  extends TableScan(cluster, traitSet, relOptTable) {

  // cache table source transformation.
  protected var sourceTransform: Transformation[_] = _

  protected val tableSourceTable: LegacyTableSourceTable[_] =
    relOptTable.unwrap(classOf[LegacyTableSourceTable[_]])

  protected[flink] val tableSource: TableSource[_] = tableSourceTable.tableSource

  override def deriveRowType(): RelDataType = {
    // TableScan row type should always keep same with its
    // interval RelOptTable's row type.
    relOptTable.getRowType
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("fields", getRowType.getFieldNames.asScala.mkString(", "))
  }
}
