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

package org.apache.flink.table.plan.nodes.physical.stream

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.calcite.rel.core.Aggregate

/**
  * Base stream physical RelNode for unbounded group aggregate.
  *
  * <P>There are two differences between stream group aggregate and [[Aggregate]]:
  * 1. stream group aggregate supports two-stage aggregation to reduce shuffle data:
  * local-aggregation and global-aggregation.
  * local-aggregation produces a partial result for each group before shuffle,
  * and then global-aggregation produces final result based on shuffled partial result.
  * Two-stage aggregation is enabled only if all aggregate functions are splittable.
  * (e.g. SUM, AVG, MAX) see [[org.apache.calcite.sql.SqlSplittableAggFunction]] for more info.
  * 2. stream group aggregate supports partial-final aggregation to resolve data-skew for
  * distinct agg. partial-aggregation produces a partial distinct agg result for each bucket group,
  * and then final-aggregation produces final result based on partial result.
  * Both partial-aggregation and final-aggregation need to shuffle data.
  * partial-final aggregation is enabled only if all distinct aggregate calls are mergeable.
  *
  * <p>NOTES: partial-aggregation has local-global mode, so does final-aggregation.
  */
abstract class StreamExecGroupAggregateBase(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode)
  extends SingleRel(cluster, traitSet, inputRel)
  with StreamPhysicalRel {

}
