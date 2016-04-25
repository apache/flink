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

package org.apache.flink.api.table.plan.nodes.datastream

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.PojoTypeInfo
import org.apache.flink.api.table.StreamTableEnvironment
import org.apache.flink.api.table.codegen.CodeGenerator
import org.apache.flink.api.table.plan.schema.TableSourceTable
import org.apache.flink.api.table.runtime.MapRunner
import org.apache.flink.api.table.sources.StreamTableSource
import org.apache.flink.api.table.typeutils.TypeConverter.determineReturnType
import org.apache.flink.streaming.api.datastream.DataStream

import scala.collection.JavaConversions._

/** Flink RelNode to read data from an external source defined by a [[StreamTableSource]]. */
class StreamTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    rowType: RelDataType)
  extends TableScan(cluster, traitSet, table)
  with DataStreamRel {

  val tableSourceTable = table.unwrap(classOf[TableSourceTable])

  val tableSource = tableSourceTable.tableSource.asInstanceOf[StreamTableSource[_]]

  override def deriveRowType() = rowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamTableSourceScan(
      cluster,
      traitSet,
      table,
      rowType
    )
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      expectedType: Option[TypeInformation[Any]])
    : DataStream[Any] = {

    val config = tableEnv.getConfig

    val inputDataStream: DataStream[Any] = tableSource
      .getDataStream(tableEnv.execEnv).asInstanceOf[DataStream[Any]]
    val inputType = inputDataStream.getType

    expectedType match {

      // special case:
      // if efficient type usage is enabled and no expected type is set
      // we can simply forward the DataStream to the next operator.
      // however, we cannot forward PojoTypes as their fields don't have an order
      case None if config.getEfficientTypeUsage && !inputType.isInstanceOf[PojoTypeInfo[_]] =>
        inputDataStream

      case _ =>
        val determinedType = determineReturnType(
          getRowType,
          expectedType,
          config.getNullCheck,
          config.getEfficientTypeUsage)

        // conversion
        if (determinedType != inputType) {
          val generator = new CodeGenerator(
            config,
            inputDataStream.getType,
            tableSourceTable.fieldIndexes)

          val conversion = generator.generateConverterResultExpression(
            determinedType,
            getRowType.getFieldNames)

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

          inputDataStream.map(mapFunc)
        }
        // no conversion necessary, forward
        else {
          inputDataStream
        }
    }
  }

}
