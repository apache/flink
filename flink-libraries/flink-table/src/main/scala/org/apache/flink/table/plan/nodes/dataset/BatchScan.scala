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
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.typeutils.PojoTypeInfo
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.plan.schema.FlinkTable
import org.apache.flink.table.typeutils.TypeConverter.determineReturnType

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

abstract class BatchScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable)
  extends TableScan(cluster, traitSet, table)
  with DataSetRel {

  override def toString: String = {
    s"Source(from: (${getRowType.getFieldNames.asScala.toList.mkString(", ")}))"
  }

  override def computeSelfCost (planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {

    val rowCnt = metadata.getRowCount(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, 0)
  }

  protected def convertToExpectedType(
      input: DataSet[Any],
      flinkTable: FlinkTable[_],
      expectedType: Option[TypeInformation[Any]],
      config: TableConfig): DataSet[Any] = {

    val inputType = input.getType

    expectedType match {

      // special case:
      // if efficient type usage is enabled and no expected type is set
      // we can simply forward the DataSet to the next operator.
      // however, we cannot forward PojoTypes as their fields don't have an order
      case None if config.getEfficientTypeUsage && !inputType.isInstanceOf[PojoTypeInfo[_]] =>
        input

      case _ =>
        val determinedType = determineReturnType(
          getRowType,
          expectedType,
          config.getNullCheck,
          config.getEfficientTypeUsage)

        // conversion
        if (determinedType != inputType) {

          val mapFunc = getConversionMapper(
            config,
            nullableInput = false,
            inputType,
            determinedType,
            "DataSetSourceConversion",
            getRowType.getFieldNames,
            Some(flinkTable.fieldIndexes))

          val opName = s"from: (${getRowType.getFieldNames.asScala.toList.mkString(", ")})"

          input.map(mapFunc).name(opName)
        }
        // no conversion necessary, forward
        else {
          input
        }
    }
  }
}
