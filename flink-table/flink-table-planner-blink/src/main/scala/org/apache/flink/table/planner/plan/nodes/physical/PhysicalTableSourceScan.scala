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

package org.apache.flink.table.planner.plan.nodes.physical

import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.dag.Transformation
import org.apache.flink.core.io.InputSplit
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.planner.plan.schema.TableSourceTable
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter.fromDataTypeToTypeInfo
import org.apache.flink.table.sources.{InputFormatTableSource, StreamTableSource, TableSource}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan

import scala.collection.JavaConverters._

/**
  * Base physical RelNode to read data from an external source defined by a [[TableSource]].
  */
abstract class PhysicalTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relOptTable: TableSourceTable[_])
  extends TableScan(cluster, traitSet, relOptTable) {

  // cache table source transformation.
  protected var sourceTransform: Transformation[_] = _

  protected val tableSourceTable: TableSourceTable[_] =
    relOptTable.unwrap(classOf[TableSourceTable[_]])

  protected[flink] val tableSource: TableSource[_] = tableSourceTable.tableSource

  override def deriveRowType(): RelDataType = {
    // TableScan row type should always keep same with its
    // interval RelOptTable's row type.
    relOptTable.getRowType
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("fields", getRowType.getFieldNames.asScala.mkString(", "))
  }

  def createInput[IN](
      env: StreamExecutionEnvironment,
      format: InputFormat[IN, _ <: InputSplit],
      t: TypeInformation[IN]): Transformation[IN]

  def getSourceTransformation(env: StreamExecutionEnvironment): Transformation[_] = {
    if (sourceTransform == null) {
      sourceTransform = tableSource match {
        case format: InputFormatTableSource[_] =>
          // we don't use InputFormatTableSource.getDataStream, because in here we use planner
          // type conversion to support precision of Varchar and something else.
          val typeInfo = fromDataTypeToTypeInfo(format.getProducedDataType)
              .asInstanceOf[TypeInformation[Any]]
          createInput(
            env,
            format.getInputFormat.asInstanceOf[InputFormat[Any, _ <: InputSplit]],
            typeInfo.asInstanceOf[TypeInformation[Any]])
        case s: StreamTableSource[_] => s.getDataStream(env).getTransformation
      }
    }
    sourceTransform
  }
}
