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

package org.apache.flink.api.table.plan.rules.dataset

import org.apache.calcite.plan.{RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.api.java.operators.join.JoinType
import org.apache.flink.api.table.plan.nodes.dataset.{DataSetConvention, DataSetJoin}
import org.apache.flink.api.table.plan.nodes.logical.{FlinkJoin, FlinkConvention}
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import org.apache.flink.api.table.plan.TypeConverter._
import org.apache.flink.api.table.runtime.FlatJoinRunner
import org.apache.flink.api.table.TableConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.codegen.CodeGenerator
import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.calcite.rel.core.JoinInfo
import org.apache.flink.api.table.TableException

class DataSetJoinRule
  extends ConverterRule(
    classOf[FlinkJoin],
    FlinkConvention.INSTANCE,
    DataSetConvention.INSTANCE,
    "DataSetJoinRule")
{

  def convert(rel: RelNode): RelNode = {
    val join: FlinkJoin = rel.asInstanceOf[FlinkJoin]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(DataSetConvention.INSTANCE)
    val convLeft: RelNode = RelOptRule.convert(join.getInput(0), DataSetConvention.INSTANCE)
    val convRight: RelNode = RelOptRule.convert(join.getInput(1), DataSetConvention.INSTANCE)

    // get the equality keys
    val joinInfo = join.analyzeCondition
    val keyPairs = joinInfo.pairs

    if (keyPairs.isEmpty) { // if no equality keys => not supported
      throw new TableException("Joins should have at least one equality condition")
    }
    else { // at least one equality expression => generate a join function
      val conditionType = join.getCondition.getType
      val func = getJoinFunction(join, joinInfo)
      val leftKeys = ArrayBuffer.empty[Int]
      val rightKeys = ArrayBuffer.empty[Int]

      keyPairs.foreach(pair => {
        leftKeys.add(pair.source)
        rightKeys.add(pair.target)}
      )

      new DataSetJoin(
        rel.getCluster,
        traitSet,
        convLeft,
        convRight,
        rel.getRowType,
        join.toString,
        leftKeys.toArray,
        rightKeys.toArray,
        JoinType.INNER,
        null,
        func)
    }
  }

  def getJoinFunction(join: FlinkJoin, joinInfo: JoinInfo):
      ((TableConfig, TypeInformation[Any], TypeInformation[Any], TypeInformation[Any]) =>
      FlatJoinFunction[Any, Any, Any]) = {

    val func = (
        config: TableConfig,
        leftInputType: TypeInformation[Any],
        rightInputType: TypeInformation[Any],
        returnType: TypeInformation[Any]) => {

      val generator = new CodeGenerator(config, leftInputType, Some(rightInputType))
      val conversion = generator.generateConverterResultExpression(returnType)
      var body = ""

      if (joinInfo.isEqui) {
        // only equality condition
        body = s"""
            |${conversion.code}
            |${generator.collectorTerm}.collect(${conversion.resultTerm});
            |""".stripMargin
      }
      else {
        val condition = generator.generateExpression(join.getCondition)
        body = s"""
            |${condition.code}
            |if (${condition.resultTerm}) {
            |  ${conversion.code}
            |  ${generator.collectorTerm}.collect(${conversion.resultTerm});
            |}
            |""".stripMargin
      }
      val genFunction = generator.generateFunction(
        description,
        classOf[FlatJoinFunction[Any, Any, Any]],
        body,
        returnType)

      new FlatJoinRunner[Any, Any, Any](
        genFunction.name,
        genFunction.code,
        genFunction.returnType)
    }
    func
  }
}

object DataSetJoinRule {
  val INSTANCE: RelOptRule = new DataSetJoinRule
}
