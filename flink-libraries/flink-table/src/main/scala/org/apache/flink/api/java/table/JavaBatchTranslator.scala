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

package org.apache.flink.api.java.table

import org.apache.calcite.plan.RelOptPlanner.CannotPlanException
import org.apache.calcite.plan.{RelTraitSet, RelOptUtil}
import org.apache.calcite.rel.{RelCollations, RelNode}
import org.apache.calcite.sql2rel.RelDecorrelator
import org.apache.calcite.tools.Programs
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.{DataSet => JavaDataSet}
import org.apache.flink.api.table.plan._
import org.apache.flink.api.table.{TableConfig, Table}
import org.apache.flink.api.table.plan.nodes.dataset.{DataSetConvention, DataSetRel}
import org.apache.flink.api.table.plan.rules.FlinkRuleSets
import org.apache.flink.api.table.plan.schema.DataSetTable

/**
 * [[PlanTranslator]] for creating [[Table]]s from Java [[org.apache.flink.api.java.DataSet]]s and
 * translating them back to Java [[org.apache.flink.api.java.DataSet]]s.
 */
class JavaBatchTranslator(config: TableConfig) extends PlanTranslator {

  type Representation[A] = JavaDataSet[A]

  override def createTable[A](
      repr: Representation[A],
      fieldIndexes: Array[Int],
      fieldNames: Array[String]): Table = {

    // create table representation from DataSet
    val dataSetTable = new DataSetTable[A](
      repr.asInstanceOf[JavaDataSet[A]],
      fieldIndexes,
      fieldNames
    )

    val tabName = TranslationContext.addDataSet(dataSetTable)
    val relBuilder = TranslationContext.getRelBuilder

    // create table scan operator
    relBuilder.scan(tabName)
    new Table(relBuilder.build(), relBuilder)
  }

  override def translate[A](lPlan: RelNode)(implicit tpe: TypeInformation[A]): JavaDataSet[A] = {

    // get the planner for the plan
    val planner = lPlan.getCluster.getPlanner


    println("-----------")
    println("Input Plan:")
    println("-----------")
    println(RelOptUtil.toString(lPlan))

    // decorrelate
    val decorPlan = RelDecorrelator.decorrelateQuery(lPlan)

    // optimize the logical Flink plan
    val optProgram = Programs.ofRules(FlinkRuleSets.DATASET_OPT_RULES)
    val flinkOutputProps = RelTraitSet.createEmpty()
      .plus(DataSetConvention.INSTANCE)
      .plus(RelCollations.of()).simplify()

    val dataSetPlan = try {
      optProgram.run(planner, decorPlan, flinkOutputProps)
    }
    catch {
      case e: CannotPlanException =>
        throw new PlanGenException(
          s"Cannot generate a valid execution plan for the given query: \n\n" +
          s"${RelOptUtil.toString(lPlan)}\n" +
          "Please consider filing a bug report.", e)
    }

    println("---------------")
    println("DataSet Plan:")
    println("---------------")
    println(RelOptUtil.toString(dataSetPlan))

    dataSetPlan match {
      case node: DataSetRel =>
        node.translateToPlan(
          config,
          Some(tpe.asInstanceOf[TypeInformation[Any]])
        ).asInstanceOf[JavaDataSet[A]]
      case _ => ???
    }

  }

}
