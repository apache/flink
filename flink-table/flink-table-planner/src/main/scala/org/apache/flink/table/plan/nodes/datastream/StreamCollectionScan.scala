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

import org.apache.calcite.plan.{RelOptCluster, RelOptSchema, RelTraitSet}
import org.apache.calcite.prepare.RelOptTableImpl
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.planner.StreamPlanner
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.types.Row

import java.util
import java.util.{List => JList}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


/**
  * Stream physical RelNode to read data from an external source defined by a
  * java [[util.Collection]].
  */
class StreamCollectionScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    schema: RelOptSchema,
    elements: JList[JList[_]],
    dataType: DataType,
    rowSchema: RowSchema)
  extends TableScan(
    cluster,
    traitSet,
    RelOptTableImpl.create(schema, rowSchema.relDataType, List[String](), null))
  with StreamScan {


  override def deriveRowType(): RelDataType = rowSchema.relDataType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamCollectionScan(
      cluster,
      traitSet,
      schema,
      elements,
      dataType,
      rowSchema)
  }

  override def translateToPlan(
      planner: StreamPlanner,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {

    val config = planner.getConfig
    val typeInfo =
      TypeConversions.fromDataTypeToLegacyInfo(dataType).asInstanceOf[TypeInformation[Any]]

    val rows = elements.map(_.toSeq).map(r => Row.of(r.asInstanceOf[Seq[Object]]:_*))
    val inputDataStream: DataStream[Any] =
      planner.getExecutionEnvironment.fromCollection(
        rows.asJava.asInstanceOf[JList[Any]], typeInfo)

    inputDataStream.getTransformation.setMaxParallelism(1)

    val fieldCnt = rowSchema.relDataType.getFieldCount

    convertToInternalRow(
      rowSchema,
      inputDataStream,
      Array.range(0, fieldCnt),
      config,
      Option.empty)
  }
}
