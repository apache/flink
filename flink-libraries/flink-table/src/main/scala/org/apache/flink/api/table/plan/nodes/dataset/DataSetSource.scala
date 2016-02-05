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

package org.apache.flink.api.table.plan.nodes.dataset

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.table.TableConfig
import org.apache.flink.api.table.codegen.CodeGenerator
import org.apache.flink.api.table.plan.TypeConverter.determineReturnType
import org.apache.flink.api.table.plan.schema.DataSetTable
import org.apache.flink.api.table.runtime.MapRunner

/**
  * Flink RelNode which matches along with DataSource.
  */
class DataSetSource(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    rowType: RelDataType)
  extends TableScan(cluster, traitSet, table)
  with DataSetRel {

  val dataSetTable: DataSetTable[Any] = table.unwrap(classOf[DataSetTable[Any]])

  override def deriveRowType() = rowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetSource(
      cluster,
      traitSet,
      table,
      rowType
    )
  }

  override def translateToPlan(
      config: TableConfig,
      expectedType: Option[TypeInformation[Any]])
    : DataSet[Any] = {

    val inputDataSet: DataSet[Any] = dataSetTable.dataSet
    val inputType = inputDataSet.getType

    // special case:
    // if efficient type usage is enabled and no expected type is set
    // we can simply forward the DataSet to the next operator
    expectedType match {
      case None if config.getEfficientTypeUsage =>
        inputDataSet

      case _ =>
        val determinedType = determineReturnType(
          getRowType,
          expectedType,
          config.getNullCheck,
          config.getEfficientTypeUsage)

        // conversion
        if (determinedType != inputType) {
          val generator = new CodeGenerator(config, inputDataSet.getType)
          val conversion = generator.generateConverterResultExpression(determinedType)

          val body =
            s"""
              |${conversion.code}
              |return ${conversion.resultTerm};
              |""".stripMargin

          val genFunction = generator.generateFunction(
            "DataSetSourceConversion",
            classOf[MapFunction[Any, Any]],
            body,
            determinedType)

          val mapFunc = new MapRunner[Any, Any](
            genFunction.name,
            genFunction.code,
            genFunction.returnType)

          inputDataSet.map(mapFunc)
        }
        // no conversion necessary, forward
        else {
          inputDataSet
        }
    }
  }

}
