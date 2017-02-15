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

package org.apache.flink.table.plan.nodes.dataset

import org.apache.calcite.plan._
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.CommonScan
import org.apache.flink.table.plan.schema.FlinkTable
import org.apache.flink.types.Row

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

abstract class BatchScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable)
  extends TableScan(cluster, traitSet, table)
  with CommonScan
  with DataSetRel {

  override def toString: String = {
    s"Source(from: (${getRowType.getFieldNames.asScala.toList.mkString(", ")}))"
  }

  override def computeSelfCost (planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {

    val rowCnt = metadata.getRowCount(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, 0)
  }

  protected def convertToInternalRow(
      input: DataSet[Any],
      flinkTable: FlinkTable[_],
      config: TableConfig)
    : DataSet[Row] = {

    val inputType = input.getType

    val internalType = FlinkTypeFactory.toInternalRowTypeInfo(getRowType)

    // conversion
    if (needsConversion(inputType, internalType)) {

      val mapFunc = getConversionMapper(
        config,
        inputType,
        internalType,
        "DataSetSourceConversion",
        getRowType.getFieldNames,
        Some(flinkTable.fieldIndexes))

      val opName = s"from: (${getRowType.getFieldNames.asScala.toList.mkString(", ")})"

      input.map(mapFunc).name(opName)
    }
    // no conversion necessary, forward
    else {
      input.asInstanceOf[DataSet[Row]]
    }
  }
}
