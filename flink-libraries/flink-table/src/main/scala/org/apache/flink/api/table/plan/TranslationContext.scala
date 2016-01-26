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

import org.apache.calcite.plan.ConventionTraitDef
import org.apache.calcite.schema.{Table, SchemaPlus}
import org.apache.calcite.tools.{Frameworks, RelBuilder}
import org.apache.flink.api.table.plan.schema.DataSetTable

object TranslationContext {

  private var relBuilder: RelBuilder = null
  private var tables: SchemaPlus = null

  reset()

  def reset(): Unit = {

    // register table in Cascading schema
    tables = Frameworks.createRootSchema(true)

    // initialize RelBuilder
    val frameworkConfig = Frameworks
      .newConfigBuilder
      .defaultSchema(tables)
      .traitDefs(ConventionTraitDef.INSTANCE)
      .build

    relBuilder = RelBuilder.create(frameworkConfig)

  }

  def addDataSet(tabName: String, newTable: DataSetTable[_]): Unit = {

    val registeredTable: Table = tables.getTable(tabName)

    if (registeredTable == null) {
      tables.add(tabName, newTable)
    }
    else if (registeredTable != newTable) {
      throw new RuntimeException("Different table for same name already in catalog registered.")
    }
  }

  def getRelBuilder: RelBuilder = {
    relBuilder
  }

}


