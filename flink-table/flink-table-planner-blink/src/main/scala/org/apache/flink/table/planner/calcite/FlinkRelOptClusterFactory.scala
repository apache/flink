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

package org.apache.flink.table.planner.calcite

import org.apache.flink.table.planner.plan.metadata.{FlinkDefaultRelMetadataProvider, FlinkRelMetadataQuery}

import org.apache.calcite.plan.{RelOptCluster, RelOptPlanner}
import org.apache.calcite.rel.metadata.{DefaultRelMetadataProvider, RelMetadataQuery}
import org.apache.calcite.rex.RexBuilder

import java.util.function.Supplier

/**
  * The utility class is to create special [[RelOptCluster]] instance which use
  * [[FlinkDefaultRelMetadataProvider]] instead of [[DefaultRelMetadataProvider]].
  */
object FlinkRelOptClusterFactory {

  def create(planner: RelOptPlanner, rexBuilder: RexBuilder): RelOptCluster = {
    val cluster = RelOptCluster.create(planner, rexBuilder)
    cluster.setMetadataProvider(FlinkDefaultRelMetadataProvider.INSTANCE)
    cluster.setMetadataQuerySupplier(new Supplier[RelMetadataQuery]() {
      def get: FlinkRelMetadataQuery = FlinkRelMetadataQuery.instance()
    })
    cluster
  }

}
