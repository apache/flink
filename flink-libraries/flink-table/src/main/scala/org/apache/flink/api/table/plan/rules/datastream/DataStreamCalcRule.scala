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

package org.apache.flink.api.table.plan.rules.datastream

import org.apache.calcite.plan.{Convention, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.logical.LogicalCalc
import org.apache.flink.api.table.runtime.FlatMapRunner
import org.apache.flink.api.table.TableConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.codegen.CodeGenerator
import org.apache.flink.api.common.functions.FlatMapFunction
import scala.collection.JavaConversions._
import org.apache.flink.api.table.plan.nodes.datastream.DataStreamFlatMap
import org.apache.flink.api.table.plan.nodes.datastream.DataStreamConvention

class DataStreamCalcRule
  extends ConverterRule(
    classOf[LogicalCalc],
    Convention.NONE,
    DataStreamConvention.INSTANCE,
    "DataStreamCalcRule")
{

  def convert(rel: RelNode): RelNode = {
    val calc: LogicalCalc = rel.asInstanceOf[LogicalCalc]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(DataStreamConvention.INSTANCE)
    val convInput: RelNode = RelOptRule.convert(calc.getInput, DataStreamConvention.INSTANCE)

    val calcFunc = (
        config: TableConfig,
        inputType: TypeInformation[Any],
        returnType: TypeInformation[Any]) => {
      val generator = new CodeGenerator(config, inputType)

      val calcProgram = calc.getProgram
      val condition = calcProgram.getCondition
      val expandedExpressions = calcProgram.getProjectList.map(
          expr => calcProgram.expandLocalRef(expr))
      val projection = generator.generateResultExpression(
        returnType,
        calc.getRowType.getFieldNames,
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
            if (inputType != returnType) {
              val conversion = generator.generateConverterResultExpression(
                  returnType,
                  calc.getRowType.getFieldNames)

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
        description,
        classOf[FlatMapFunction[Any, Any]],
        body,
        returnType)

      new FlatMapRunner[Any, Any](
        genFunction.name,
        genFunction.code,
        genFunction.returnType)
    }

    new DataStreamFlatMap(
      rel.getCluster,
      traitSet,
      convInput,
      rel.getRowType,
      calc.toString,
      calcFunc)
  }
}

object DataStreamCalcRule {
  val INSTANCE: RelOptRule = new DataStreamCalcRule
}
