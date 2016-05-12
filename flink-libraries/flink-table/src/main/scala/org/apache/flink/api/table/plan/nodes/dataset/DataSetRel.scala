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

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.table.codegen.CodeGenerator
import org.apache.flink.api.table.{BatchTableEnvironment, TableConfig, TableEnvironment}
import org.apache.flink.api.table.plan.nodes.FlinkRel
import org.apache.flink.api.table.runtime.MapRunner

import scala.collection.JavaConversions._

trait DataSetRel extends RelNode with FlinkRel {

  /**
    * Translates the [[DataSetRel]] node into a [[DataSet]] operator.
    *
    * @param tableEnv [[org.apache.flink.api.table.BatchTableEnvironment]] of the translated Table.
    * @param expectedType specifies the type the Flink operator should return. The type must
    *                     have the same arity as the result. For instance, if the
    *                     expected type is a RowTypeInfo this method will return a DataSet of
    *                     type Row. If the expected type is Tuple2, the operator will return
    *                     a Tuple2 if possible. Row otherwise.
    * @return DataSet of type expectedType or RowTypeInfo
    */
  def translateToPlan(
     tableEnv: BatchTableEnvironment,
     expectedType: Option[TypeInformation[Any]] = None) : DataSet[Any]

  private[flink] def estimateRowSize(rowType: RelDataType): Double = {

    rowType.getFieldList.map(_.getType.getSqlTypeName).foldLeft(0) { (s, t) =>
      t match {
        case SqlTypeName.TINYINT => s + 1
        case SqlTypeName.SMALLINT => s + 2
        case SqlTypeName.INTEGER => s + 4
        case SqlTypeName.BIGINT => s + 8
        case SqlTypeName.BOOLEAN => s + 1
        case SqlTypeName.FLOAT => s + 4
        case SqlTypeName.DOUBLE => s + 8
        case SqlTypeName.VARCHAR => s + 12
        case SqlTypeName.CHAR => s + 1
        case _ => throw new IllegalArgumentException("Unsupported data type encountered")
      }
    }

  }

  private[dataset] def getConversionMapper(
      config: TableConfig,
      inputType: TypeInformation[Any],
      expectedType: TypeInformation[Any],
      conversionOperatorName: String,
      fieldNames: Seq[String],
      inputPojoFieldMapping: Option[Array[Int]] = None): MapFunction[Any, Any] = {

    val generator = new CodeGenerator(
      config,
      inputType,
      None,
      inputPojoFieldMapping)
    val conversion = generator.generateConverterResultExpression(expectedType, fieldNames)

    val body =
      s"""
         |${conversion.code}
         |return ${conversion.resultTerm};
         |""".stripMargin

    val genFunction = generator.generateFunction(
      conversionOperatorName,
      classOf[MapFunction[Any, Any]],
      body,
      expectedType)

    new MapRunner[Any, Any](
      genFunction.name,
      genFunction.code,
      genFunction.returnType)

  }

}
