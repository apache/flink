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

package org.apache.flink.table.planner.plan.nodes.calcite

import org.apache.flink.table.planner.calcite.FlinkTypeFactory

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFieldImpl}
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}

import scala.collection.JavaConversions._

/**
  * Relational operator that generates [[org.apache.flink.streaming.api.watermark.Watermark]].
  */
abstract class WatermarkAssigner(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    inputRel: RelNode,
    val rowtimeFieldIndex: Option[Int],
    val watermarkDelay: Option[Long])
  extends SingleRel(cluster, traits, inputRel) {

  override def deriveRowType(): RelDataType = {
    val inputRowType = inputRel.getRowType
    val typeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]

    val newFieldList = inputRowType.getFieldList.map { f =>
      rowtimeFieldIndex match {
        case Some(index) if f.getIndex == index =>
          val rowtimeIndicatorType = typeFactory.createRowtimeIndicatorType(f.getType.isNullable)
          new RelDataTypeFieldImpl(f.getName, f.getIndex, rowtimeIndicatorType)
        case _ => f
      }
    }

    val builder = typeFactory.builder
    builder.addAll(newFieldList)
    builder.build()
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("fields", getRowType.getFieldNames.mkString(", "))
      .itemIf("rowtimeField", getRowType.getFieldNames.get(rowtimeFieldIndex.getOrElse(0)),
        rowtimeFieldIndex.isDefined)
      .itemIf("watermarkDelay", watermarkDelay.getOrElse(0L), watermarkDelay.isDefined)
  }
}
