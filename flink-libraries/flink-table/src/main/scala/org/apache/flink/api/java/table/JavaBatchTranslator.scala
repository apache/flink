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

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelNode
import org.apache.calcite.tools.{RelBuilder, Frameworks}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.{DataSet => JavaDataSet}
import org.apache.flink.api.table.plan._
import org.apache.flink.api.table.plan.operators.DataSetTable
import org.apache.flink.api.table.Table

/**
 * [[PlanTranslator]] for creating [[Table]]s from Java [[org.apache.flink.api.java.DataSet]]s and
 * translating them back to Java [[org.apache.flink.api.java.DataSet]]s.
 */
class JavaBatchTranslator extends PlanTranslator {

  type Representation[A] = JavaDataSet[A]

  override def createTable[A](
      repr: Representation[A],
      fieldNames: Array[String]): Table = {

    // create table representation from DataSet
    val dataSetTable = new DataSetTable[A](
    repr.asInstanceOf[JavaDataSet[A]],
    fieldNames
    )

    // register table in Cascading schema
    val schema = Frameworks.createRootSchema(true)
    val tableName = repr.hashCode().toString
    schema.add(tableName, dataSetTable)

    // initialize RelBuilder
    val frameworkConfig = Frameworks
      .newConfigBuilder
      .defaultSchema(schema)
      .build
    val relBuilder = RelBuilder.create(frameworkConfig)

    // create table scan operator
    relBuilder.scan(tableName)
    new Table(relBuilder.build(), relBuilder)
  }

  override def translate[A](lPlan: RelNode)(implicit tpe: TypeInformation[A]): JavaDataSet[A] = {

    println(RelOptUtil.toString(lPlan))

    // TODO: optimize & translate:
    // - optimize RelNode plan
    // - translate to Flink RelNode plan
    // - generate DataSet program

    null
  }

}
