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

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.table.codegen.CodeGenerator
import org.apache.flink.api.table.typeutils.TypeConverter
import TypeConverter._
import org.apache.flink.api.table.runtime.FlatMapRunner
import org.apache.flink.api.table.TableConfig
import org.apache.calcite.rex._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Flink RelNode which matches along with LogicalCalc.
  *
  */
class DataSetCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    rowType: RelDataType,
    calcProgram: RexProgram,
    opName: String,
    ruleDescription: String)
  extends SingleRel(cluster, traitSet, input)
  with DataSetRel {

  override def deriveRowType() = rowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetCalc(
      cluster,
      traitSet,
      inputs.get(0),
      rowType,
      calcProgram,
      opName,
      ruleDescription)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("name", opName)
  }

  override def toString = opName

  override def translateToPlan(config: TableConfig,
      expectedType: Option[TypeInformation[Any]]): DataSet[Any] = {

    val inputDS = input.asInstanceOf[DataSetRel].translateToPlan(config)

    val returnType = determineReturnType(
      getRowType,
      expectedType,
      config.getNullCheck,
      config.getEfficientTypeUsage)

    val generator = new CodeGenerator(config, inputDS.getType)

    val condition = calcProgram.getCondition
    val expandedExpressions = calcProgram.getProjectList.map(
       expr => calcProgram.expandLocalRef(expr))
    val projection = generator.generateResultExpression(
      returnType,
      rowType.getFieldNames,
      expandedExpressions)

    val body = {
      // only projection
      if (condition == null) {
        s"""
          |${projection.code}
          |${generator.collectorTerm}.collect(${projection.resultTerm});
          |""".stripMargin
      }
      else {
        val filterCondition = generator.generateExpression(
          calcProgram.expandLocalRef(calcProgram.getCondition))
        // only filter
        if (projection == null) {
          // conversion
          if (inputDS.getType != returnType) {
            val conversion = generator.generateConverterResultExpression(
              returnType,
              rowType.getFieldNames)

            s"""
              |${filterCondition.code}
              |if (${filterCondition.resultTerm}) {
              |  ${conversion.code}
              |  ${generator.collectorTerm}.collect(${conversion.resultTerm});
              |}
              |""".stripMargin
          }
          // no conversion
          else {
            s"""
              |${filterCondition.code}
              |if (${filterCondition.resultTerm}) {
              |  ${generator.collectorTerm}.collect(${generator.input1Term});
              |}
              |""".stripMargin
          }
        }
        // both filter and projection
        else {
          s"""
            |${filterCondition.code}
            |if (${filterCondition.resultTerm}) {
            |  ${projection.code}
            |  ${generator.collectorTerm}.collect(${projection.resultTerm});
            |}
            |""".stripMargin
        }
      }
    }

    val genFunction = generator.generateFunction(
      ruleDescription,
      classOf[FlatMapFunction[Any, Any]],
      body,
      returnType)

    val mapFunc = new FlatMapRunner[Any, Any](
      genFunction.name,
      genFunction.code,
      genFunction.returnType)

    val calcDesc = calcProgramToString()

    inputDS.flatMap(mapFunc).name(calcDesc)
  }

  private def calcProgramToString(): String = {

    val cond = calcProgram.getCondition
    val proj = calcProgram.getProjectList.asScala.toList
    val localExprs = calcProgram.getExprList.asScala.toList
    val inFields = calcProgram.getInputRowType.getFieldNames.asScala.toList
    val outFields = calcProgram.getInputRowType.getFieldNames.asScala.toList

    val projString = s"select: (${
      proj
        .map(getExpressionString(_, inFields, Some(localExprs)))
        .zip(outFields).map { case (e, o) => {
            if (e != o) {
              e + " AS " + o
            } else {
              e
            }
          }
        }
        .mkString(", ")
    })"
    if (cond != null) {
      val condString = s"where: (${getExpressionString(cond, inFields, Some(localExprs))})"

      condString + ", " + projString
    } else {
      projString
    }

  }

}
