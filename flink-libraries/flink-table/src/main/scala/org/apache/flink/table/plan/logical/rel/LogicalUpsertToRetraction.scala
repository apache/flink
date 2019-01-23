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

package org.apache.flink.table.plan.logical.rel

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelNode, SingleRel}

/**
  * Represent a relnode which convert upsert to retraction.
  */
class LogicalUpsertToRetraction(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    child: RelNode,
    val upsertKeyNames: Array[String])
  extends SingleRel(cluster, traitSet, child) {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new LogicalUpsertToRetraction(cluster, traitSet, inputs.get(0), upsertKeyNames)
  }
}

object LogicalUpsertToRetraction {

  def create(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    child: RelNode,
    keyIndexes: Array[String]): LogicalUpsertToRetraction = {

    new LogicalUpsertToRetraction(cluster, traitSet, child, keyIndexes)
  }
}

