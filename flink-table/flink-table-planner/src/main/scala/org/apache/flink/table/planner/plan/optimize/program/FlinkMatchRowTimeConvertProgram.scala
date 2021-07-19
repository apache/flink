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

package org.apache.flink.table.planner.plan.optimize.program

import org.apache.flink.table.planner.calcite.MatchRowTimeConverter
import org.apache.flink.util.Preconditions

import org.apache.calcite.rel.RelNode

/**
 * A FlinkOptimizeProgram that deals with MATCH_ROWTIME which used to access a event time
 * attribute with TIMESTAMP_LTZ type from MATCH_RECOGNIZE.
 *
 * @tparam OC OptimizeContext
 */
class FlinkMatchRowTimeConvertProgram[OC <: FlinkOptimizeContext] extends FlinkOptimizeProgram[OC] {

  override def optimize(input: RelNode, context: OC): RelNode = {
    val rexBuilder = Preconditions.checkNotNull(context.getRexBuilder)
    MatchRowTimeConverter.convert(input, rexBuilder)
  }

}
