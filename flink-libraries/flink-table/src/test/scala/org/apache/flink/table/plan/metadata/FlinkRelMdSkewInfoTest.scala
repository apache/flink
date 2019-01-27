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

package org.apache.flink.table.plan.metadata

import org.apache.flink.table.plan.nodes.logical.FlinkLogicalExpand
import org.apache.flink.table.plan.rules.logical.DecomposeGroupingSetsRule._
import org.apache.flink.table.plan.stats.SkewInfoInternal

import com.google.common.collect.ImmutableList
import org.apache.calcite.util.ImmutableBitSet
import org.junit.Assert._
import org.junit.Test

class FlinkRelMdSkewInfoTest extends FlinkRelMdHandlerTestBase {

  @Test
  def test(): Unit = {
    val ts = relBuilder.scan("student").build()
    val expandOutputType = buildExpandRowType(
      ts.getCluster.getTypeFactory, ts.getRowType, Array.empty[Integer])
    val expandProjects = createExpandProjects(
      ts.getCluster.getRexBuilder,
      ts.getRowType,
      expandOutputType,
      ImmutableBitSet.of(0, 1, 2, 3),
      ImmutableList.of(
        ImmutableBitSet.of(0),
        ImmutableBitSet.of(1),
        ImmutableBitSet.of(2),
        ImmutableBitSet.of(3)
      ), Array.empty[Integer])
    val expand = new FlinkLogicalExpand(
      ts.getCluster, ts.getTraitSet, ts, expandOutputType, expandProjects, 4)
    val expect = SkewInfoInternal(Map(
      0 -> Seq(null), 1 -> Seq(null), 2 -> Seq(null), 3 -> Seq(null)))
    assertEquals(expect, mq.getSkewInfo(expand))
  }
}
