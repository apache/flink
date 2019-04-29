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

package org.apache.flink.table.plan.nodes

import java.util

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.calcite.FlinkTypeFactory

import scala.collection.JavaConversions._

trait CommonTableAggregate extends CommonAggregate {

  protected def deriveTableAggRowType(
    cluster: RelOptCluster,
    child: RelNode,
    groupSet: ImmutableBitSet,
    aggCalls: util.List[AggregateCall]): RelDataType = {

    val typeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val builder = typeFactory.builder

    // group key fields
    groupSet.asList().foreach(e => {
      val field = child.getRowType.getFieldList.get(e)
      builder.add(field)
    })

    // agg fields
    aggCalls.get(0).`type`.getFieldList.foreach(builder.add)
    builder.build()
  }
}
