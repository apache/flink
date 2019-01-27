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

package org.apache.flink.table.plan.nodes.calcite

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelNode, SingleRel}

/**
  * The LastRow node is used to select the last row for every key partition order by rowtime
  * or nothing. If order by nothing, the result is not deterministic. It is also used to
  * change an append stream source to an update stream.
  */
class LogicalLastRow(
   cluster: RelOptCluster,
   traits: RelTraitSet,
   inputNode: RelNode,
   val uniqueKeys: util.List[String])
  extends SingleRel(cluster, traits, inputNode){

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new LogicalLastRow(cluster, traitSet, inputs.get(0), uniqueKeys)
  }
}
