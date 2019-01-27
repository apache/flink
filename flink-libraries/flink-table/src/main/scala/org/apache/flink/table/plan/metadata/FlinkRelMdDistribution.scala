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

import org.apache.flink.table.api.{TableConfig, TableConfigOptions}
import org.apache.flink.table.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.plan.metadata.FlinkMetadata.FlinkDistribution

import org.apache.calcite.rel._
import org.apache.calcite.rel.core.{Calc, Sort, TableScan}
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.util.mapping.Mappings

import java.util

import scala.collection.JavaConversions._

/**
  * FlinkRelMdDistribution supplies a default implementation of
  * [[FlinkRelMetadataQuery.flinkDistribution]] for the standard logical algebra.
  */
class FlinkRelMdDistribution private extends MetadataHandler[FlinkDistribution] {

  override def getDef: MetadataDef[FlinkDistribution] = FlinkDistribution.DEF

  def flinkDistribution(rel: RelNode, mq: RelMetadataQuery): FlinkRelDistribution = {
    getFlinkDistribution(rel)
  }

  def flinkDistribution(scan: TableScan, mq: RelMetadataQuery): FlinkRelDistribution = {
    val statisticDistribution = scan.getTable.getDistribution
    if (statisticDistribution != null &&
      statisticDistribution.getTraitDef.equals(FlinkRelDistributionTraitDef.INSTANCE)) {
      statisticDistribution.asInstanceOf[FlinkRelDistribution]
    } else {
      getFlinkDistribution(scan)
    }
  }

  def flinkDistribution(calc: Calc, mq: RelMetadataQuery): FlinkRelDistribution = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val input = calc.getInput
    val distribution = fmq.flinkDistribution(input)
    val mapInToOutPos = new util.HashMap[Integer, Integer]
    val projExprs = calc.getProgram.getProjectList.map(calc.getProgram.expandLocalRef)
    // Build an input to output position map.
    projExprs.zipWithIndex.foreach {
      case (projExpr, i) =>
        projExpr match {
          case ref: RexInputRef => mapInToOutPos.put(ref.getIndex, i)
          case _ => // ignore
        }
    }
    //FIXME transmit one possible distribution.
    // for exmaple "select a, a, sum(b) group by a"， here only transmit hash[1], not hash[0].
    val mapping = Mappings
      .target(mapInToOutPos, input.getRowType.getFieldCount, calc.getRowType.getFieldCount)
    distribution.apply(mapping)
  }

  def flinkDistribution(sort: Sort, mq: RelMetadataQuery): FlinkRelDistribution = {
    val conf = sort.getCluster.getPlanner.getContext.unwrap(classOf[TableConfig])
    val enableRangeSort = conf.getConf.getBoolean(TableConfigOptions.SQL_EXEC_SORT_RANGE_ENABLED)
    if ((sort.getCollation.getFieldCollations.nonEmpty &&
      sort.fetch == null && sort.offset == null) && enableRangeSort) {
      //If Sort is global sort, and the table config allows the range partition.
      //Then the Sort's required traits will are range distribution and sort collation.
      FlinkRelDistribution.range(sort.getCollation.getFieldCollations)
    } else {
      FlinkRelDistribution.SINGLETON
    }
  }

  private def getFlinkDistribution(relNode: RelNode): FlinkRelDistribution = {
    relNode.getTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
  }
}

object FlinkRelMdDistribution {

  private val INSTANCE = new FlinkRelMdDistribution

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    FlinkMetadata.FlinkDistribution.METHOD, INSTANCE)

}
