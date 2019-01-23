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

package org.apache.flink.table.plan.nodes.datastream

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment}
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.types.CRow

import scala.collection.JavaConversions._

/**
  * Flink RelNode for ingesting upsert stream from source.
  */
class DataStreamUpsertToRetraction(
   cluster: RelOptCluster,
   traitSet: RelTraitSet,
   input: RelNode,
   inputSchema: RowSchema,
   schema: RowSchema,
   val keyNames: Seq[String])
  extends SingleRel(cluster, traitSet, input)
  with DataStreamRel{

  lazy val keyIndexes = getRowType.getFieldNames.zipWithIndex
    .filter(e => keyNames.contains(e._1))
    .map(_._2).toArray

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new DataStreamUpsertToRetraction(
      cluster,
      traitSet,
      inputs.get(0),
      inputSchema,
      schema,
      keyNames)
  }

  override def explainTerms(pw: RelWriter): RelWriter ={
    super.explainTerms(pw)
      .itemIf("keys", keyNames.mkString(", "), keyNames.nonEmpty)
      .item("select", input.getRowType.getFieldNames.toArray.mkString(", "))
  }

  override def toString: String = {
    s"UpsertToRetractionConverter(${
      if (keyNames.nonEmpty) {
        s"keys:(${keyNames.mkString(", ")}), "
      } else {
        ""
      }
    }select:(${input.getRowType.getFieldNames.toArray.mkString(", ")}))"
  }

  override def producesUpdates: Boolean = true

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {

    // todo: return inputDS for plan test. The detailed code will be ready in the next commit
    throw new NotImplementedError
  }
}
