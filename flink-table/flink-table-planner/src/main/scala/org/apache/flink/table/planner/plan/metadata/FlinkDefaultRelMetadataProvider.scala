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

package org.apache.flink.table.planner.plan.metadata

import com.google.common.collect.ImmutableList
import org.apache.calcite.rel.metadata._

object FlinkDefaultRelMetadataProvider {

  val INSTANCE: RelMetadataProvider = ChainedRelMetadataProvider.of(
    ImmutableList.of(
      FlinkRelMdPercentageOriginalRows.SOURCE,
      FlinkRelMdNonCumulativeCost.SOURCE,
      FlinkRelMdCumulativeCost.SOURCE,
      FlinkRelMdRowCount.SOURCE,
      FlinkRelMdSize.SOURCE,
      FlinkRelMdSelectivity.SOURCE,
      FlinkRelMdDistinctRowCount.SOURCE,
      FlinkRelMdColumnInterval.SOURCE,
      FlinkRelMdFilteredColumnInterval.SOURCE,
      FlinkRelMdDistribution.SOURCE,
      FlinkRelMdColumnNullCount.SOURCE,
      FlinkRelMdColumnOriginNullCount.SOURCE,
      FlinkRelMdPopulationSize.SOURCE,
      FlinkRelMdColumnUniqueness.SOURCE,
      FlinkRelMdUniqueKeys.SOURCE,
      FlinkRelMdUpsertKeys.SOURCE,
      FlinkRelMdUniqueGroups.SOURCE,
      FlinkRelMdModifiedMonotonicity.SOURCE,
      RelMdColumnOrigins.SOURCE,
      RelMdMaxRowCount.SOURCE,
      RelMdMinRowCount.SOURCE,
      RelMdPredicates.SOURCE,
      FlinkRelMdCollation.SOURCE,
      RelMdExplainVisibility.SOURCE,
      FlinkRelMdWindowProperties.SOURCE
    )
  )
}
