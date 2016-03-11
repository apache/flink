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

package org.apache.flink.api.table.plan

import java.util.concurrent.atomic.AtomicInteger

import org.apache.calcite.config.Lex
import org.apache.calcite.plan.ConventionTraitDef
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.{FrameworkConfig, Frameworks, RelBuilder}
import org.apache.flink.api.table.plan.schema.DataSetTable

object TranslationContext {

  private var frameworkConfig: FrameworkConfig = null
  private var relBuilder: RelBuilder = null
  private var tables: SchemaPlus = null
  private var tabNames: Map[AbstractTable, String] = null
  private val nameCntr: AtomicInteger = new AtomicInteger(0)

  reset()

  def reset(): Unit = {

    // register table in Cascading schema
    tables = Frameworks.createRootSchema(true)

    // TODO move this to TableConfig when we implement full SQL support
    // configure sql parser
    // we use Java lex because back ticks are easier than double quotes in programming
    // and cases are preserved
    val parserConfig = SqlParser.configBuilder().setLex(Lex.JAVA).build()

    // initialize RelBuilder
    frameworkConfig = Frameworks
      .newConfigBuilder
      .defaultSchema(tables)
      .parserConfig(parserConfig)
      .traitDefs(ConventionTraitDef.INSTANCE)
      .build

    tabNames = Map[AbstractTable, String]()

    relBuilder = RelBuilder.create(frameworkConfig)

    nameCntr.set(0)

  }

  def addDataSet(newTable: DataSetTable[_]): String = {

    // look up name
    val tabName = tabNames.get(newTable)

    tabName match {
      case Some(name) =>
        name
      case None =>
        val tabName = "DataSetTable_" + nameCntr.getAndIncrement()
        tabNames += (newTable -> tabName)
        tables.add(tabName, newTable)
        tabName
    }

  }

  def getUniqueName: String = {
    "TMP_" + nameCntr.getAndIncrement()
  }

  def getRelBuilder: RelBuilder = {
    relBuilder
  }

  def getFrameworkConfig: FrameworkConfig = {
    frameworkConfig
  }

}


