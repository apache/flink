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

package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.rel.{RelNode, SingleRel}

/**
  * Base stream physical RelNode for unbounded group aggregate.
  *
  * <P>There are two differences between stream group aggregate and [[Aggregate]]:
  * 1. This node supports two-stage aggregation to reduce data-shuffling:
  * local-aggregation and global-aggregation.
  * local-aggregation produces a partial result for each group before shuffle in stage 1,
  * and then the partially aggregated results are shuffled by group key to global-aggregation
  * which produces the final result in stage 2.
  * local-global aggregation is enabled only if all aggregate functions are mergeable.
  * (e.g. SUM, AVG, MAX)
  * 2. stream group aggregate supports partial-final aggregation to resolve data-skew for
  * distinct agg.
  * partial-aggregation produces a partial distinct aggregated result based on
  * group key and bucket number (which means the data is shuffled by group key and bucket number),
  * and then the partially distinct aggregated result are shuffled by group key only
  * to final-aggregation which produces final result.
  * partial-final aggregation is enabled only if all distinct aggregate functions are splittable.
  * (e.g. DISTINCT SUM, DISTINCT AVG, DISTINCT MAX)
  *
  * <p>NOTES: partial-aggregation supports local-global mode, so does final-aggregation.
  */
abstract class StreamExecGroupAggregateBase(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode)
  extends SingleRel(cluster, traitSet, inputRel)
  with StreamPhysicalRel {

}
