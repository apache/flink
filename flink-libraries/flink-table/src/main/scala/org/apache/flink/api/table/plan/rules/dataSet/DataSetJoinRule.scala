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

package org.apache.flink.api.table.plan.rules.dataSet

import org.apache.calcite.plan.{RelOptRuleCall, Convention, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.flink.api.java.operators.join.JoinType
import org.apache.flink.api.table.plan.nodes.dataset.{DataSetJoin, DataSetConvention}
import scala.collection.JavaConversions._

class DataSetJoinRule
  extends ConverterRule(
      classOf[LogicalJoin],
      Convention.NONE,
      DataSetConvention.INSTANCE,
      "DataSetJoinRule")
  {

  /**
   * Only translate INNER joins for now
   */
  override def matches(call: RelOptRuleCall): Boolean = {
    val join: LogicalJoin = call.rel(0).asInstanceOf[LogicalJoin]
    join.getJoinType.equals(JoinRelType.INNER)
  }

    def convert(rel: RelNode): RelNode = {

      val join: LogicalJoin = rel.asInstanceOf[LogicalJoin]
      val traitSet: RelTraitSet = rel.getTraitSet.replace(DataSetConvention.INSTANCE)
      val convLeft: RelNode = RelOptRule.convert(join.getInput(0), DataSetConvention.INSTANCE)
      val convRight: RelNode = RelOptRule.convert(join.getInput(1), DataSetConvention.INSTANCE)
      val joinInfo = join.analyzeCondition

        new DataSetJoin(
          rel.getCluster,
          traitSet,
          convLeft,
          convRight,
          rel.getRowType,
          join.getCondition,
          join.getRowType,
          joinInfo,
          joinInfo.pairs.toList,
          JoinType.INNER,
          null,
          description)
    }
  }

object DataSetJoinRule {
  val INSTANCE: RelOptRule = new DataSetJoinRule
}
