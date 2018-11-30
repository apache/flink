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

package org.apache.flink.table.api.stream.sql

import java.util

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.{RelMultipleTrait, RelOptPlanner, RelTrait}
import org.apache.calcite.rel.RelDistribution.Type
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelDistribution, RelDistributionTraitDef}
import org.apache.calcite.util.mapping.{Mapping, Mappings}
import org.apache.calcite.util.{ImmutableIntList, Util}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalExchange
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}
import org.hamcrest.CoreMatchers.{is, not}
import org.junit.Assert.assertThat
import org.junit.Test

/**
  * See {@link org.apache.calcite.test.RelMetadataTest}.
  * Unit test LogicalExchange.
  */
class ExchangeTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c.rowtime, 'proctime.proctime)

  @Test
  def testDistributionHashEmpty(): Unit = {
    val sqlQuery = "SELECT * FROM MyTable"
    val resultTable: Table = streamUtil.tableEnv.sqlQuery(sqlQuery);
    val relNode = resultTable.getRelNode
    val mq: RelMetadataQuery  = RelMetadataQuery.instance
    val dist = mq.getDistribution(relNode)

    val list = new util.ArrayList[Integer]()
    val impl = new RelDistributionImpl(RelDistribution.Type.HASH_DISTRIBUTED, list);
    val distribution: RelDistribution = RelDistributionTraitDef.INSTANCE.canonize(impl)
    val exchange = new FlinkLogicalExchange(relNode.getCluster,
      relNode.getTraitSet.replace(FlinkConventions.DATASTREAM),
      relNode.getInput(0),
      distribution
    )

    val changeDist = mq.getDistribution(exchange)
    assertThat(dist, not(distribution))
    assertThat(changeDist, is(distribution))
  }

  @Test
  def testDistributionHash(): Unit = {
    val sqlQuery = "SELECT * FROM MyTable"
    val resultTable: Table = streamUtil.tableEnv.sqlQuery(sqlQuery);
    val relNode = resultTable.getRelNode
    val mq: RelMetadataQuery  = RelMetadataQuery.instance
    val dist = mq.getDistribution(relNode)

    val list = ImmutableList.of[Integer](2)
    val impl = new RelDistributionImpl(RelDistribution.Type.HASH_DISTRIBUTED, list);
    val distribution: RelDistribution = RelDistributionTraitDef.INSTANCE.canonize(impl)
    val exchange = new FlinkLogicalExchange(relNode.getCluster,
      relNode.getTraitSet.replace(FlinkConventions.DATASTREAM),
      relNode.getInput(0),
      distribution
    )

    val changeDist = mq.getDistribution(exchange)
    assertThat(dist, not(distribution))
    assertThat(changeDist, is(distribution))
  }

  @Test
  def testDistributionSingleton(): Unit = {
    val sqlQuery = "SELECT * FROM MyTable"
    val resultTable: Table = streamUtil.tableEnv.sqlQuery(sqlQuery);
    val relNode = resultTable.getRelNode
    val mq: RelMetadataQuery  = RelMetadataQuery.instance
    val dist = mq.getDistribution(relNode)

    val distribution: RelDistribution = SINGLETON
    val exchange = new FlinkLogicalExchange(relNode.getCluster,
      relNode.getTraitSet.replace(FlinkConventions.DATASTREAM),
      relNode.getInput(0),
      distribution
    )

    val changeDist = mq.getDistribution(exchange)
    assertThat(dist, not(distribution))
    assertThat(changeDist, is(distribution))
  }


  val EMPTY = new util.ArrayList[Integer]()
  val SINGLETON = new RelDistributionImpl(RelDistribution.Type.SINGLETON, EMPTY)
  val RANDOM_DISTRIBUTED = new RelDistributionImpl(RelDistribution.Type.RANDOM_DISTRIBUTED, EMPTY)

  class RelDistributionImpl(val inType: RelDistribution.Type,val inKeys: util.List[Integer])
    extends RelDistribution {

    override def getType: RelDistribution.Type = inType

    override def getKeys: util.List[Integer] = inKeys

    override def apply(mapping: Mappings.TargetMapping): RelDistribution = {
      if(inKeys.isEmpty) this
      getTraitDef().canonize(new RelDistributionImpl(inType,
        ImmutableIntList.copyOf(Mappings.apply(mapping.asInstanceOf[Mapping], inKeys))))
    }

    override def isTop: Boolean = inType eq Type.ANY;

    override def getTraitDef: RelDistributionTraitDef = RelDistributionTraitDef.INSTANCE

    override def satisfies(inTrait: RelTrait): Boolean = {
      if (inTrait == this ||
        inTrait == new RelDistributionImpl(RelDistribution.Type.ANY, EMPTY)) { true }
      if (inTrait.isInstanceOf[RelDistributionImpl]) {
        val distribution: RelDistributionImpl = inTrait.asInstanceOf[RelDistributionImpl]
        if (inType.equals(distribution.inType)) {
          inType match {
            case Type.HASH_DISTRIBUTED =>
              // The "leading edge" property of Range does not apply to Hash.
              // Only Hash[x, y] satisfies Hash[x, y].
              inKeys.equals(distribution.inKeys)
            case Type.RANGE_DISTRIBUTED =>
              // Range[x, y] satisfies Range[x, y, z] but not Range[x]
              Util.startsWith(distribution.inKeys, inKeys)
            case _ =>
              true
          }
        }
      }
      if (inTrait eq RANDOM_DISTRIBUTED) {
        // we've already checked RANDOM
        (inType eq Type.HASH_DISTRIBUTED) ||
          (inType eq Type.ROUND_ROBIN_DISTRIBUTED) ||
          (inType eq Type.RANGE_DISTRIBUTED)
      }
      false
    }

    override def register(planner: RelOptPlanner): Unit = {}

    override def compareTo(o: RelMultipleTrait): Int = {
      val distribution = o.asInstanceOf[RelDistribution];
      inType.compareTo(distribution.getType());
    }
  }
}
