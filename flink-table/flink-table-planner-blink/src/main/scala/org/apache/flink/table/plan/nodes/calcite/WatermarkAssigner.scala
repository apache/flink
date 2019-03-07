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

package org.apache.flink.table.plan.nodes.calcite

import org.apache.flink.table.calcite.FlinkTypeFactory

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
    inputNode: RelNode,
    val rowtimeField: String,
    val watermarkOffset: Long)
  extends SingleRel(cluster, traits, inputNode) {

  override def deriveRowType(): RelDataType = {
    val inputRowType = inputNode.getRowType
    val typeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]

    val newFieldList = inputRowType.getFieldList.map { f =>
      if (f.getName.equals(rowtimeField)) {
        val rowtimeIndicatorType = typeFactory.createRowtimeIndicatorType()
        new RelDataTypeFieldImpl(rowtimeField, f.getIndex, rowtimeIndicatorType)
      } else {
        f
      }
    }

    val builder = typeFactory.builder
    builder.addAll(newFieldList)
    builder.build()
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("fields", getRowType.getFieldNames)
      .item("rowtimeField", rowtimeField)
      .item("watermarkOffset", watermarkOffset)
  }
}
