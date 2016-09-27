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

package org.apache.flink.api.table

import org.apache.calcite.sql.SqlOperatorTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.RuleSet
import org.apache.flink.util.Preconditions

/**
  * Builder for creating a Calcite configuration.
  */
class CalciteConfigBuilder {
  private var replaceRuleSet: Option[RuleSet] = None
  private var chainRuleSet: Option[RuleSet] = None

  private var replaceSqlOperatorTable: Option[SqlOperatorTable] = None
  private var chainSqlOperatorTable: Option[SqlOperatorTable] = None

  private var replaceSqlParserConfig: Option[SqlParser.Config] = None

  /**
    * Replaces the built-in rule set with the given rule set.
    */
  def replaceRuleSet(ruleSet: RuleSet): CalciteConfigBuilder = {
    Preconditions.checkNotNull(ruleSet)
    replaceRuleSet = Some(ruleSet)
    chainRuleSet = None
    this
  }

  /**
    * Appends the given rule set to the built-in rule set.
    */
  def addRuleSet(ruleSet: RuleSet): CalciteConfigBuilder = {
    Preconditions.checkNotNull(ruleSet)
    replaceRuleSet = None
    chainRuleSet = Some(ruleSet)
    this
  }

  /**
    * Replaces the built-in SQL operator table with the given table.
    */
  def replaceSqlOperatorTable(sqlOperatorTable: SqlOperatorTable): CalciteConfigBuilder = {
    Preconditions.checkNotNull(sqlOperatorTable)
    replaceSqlOperatorTable = Some(sqlOperatorTable)
    chainSqlOperatorTable = None
    this
  }

  /**
    * Appends the given table to the built-in SQL operator table.
    */
  def addSqlOperatorTable(sqlOperatorTable: SqlOperatorTable): CalciteConfigBuilder = {
    Preconditions.checkNotNull(sqlOperatorTable)
    replaceSqlOperatorTable = None
    chainSqlOperatorTable = Some(sqlOperatorTable)
    this
  }

  /**
    * Replaces the built-in SQL parser configuration with the given configuration.
    */
  def replaceSqlParserConfig(sqlParserConfig: SqlParser.Config): CalciteConfigBuilder = {
    Preconditions.checkNotNull(sqlParserConfig)
    replaceSqlParserConfig = Some(sqlParserConfig)
    this
  }

  private class CalciteConfigImpl(
      val getRuleSet: Option[RuleSet],
      val replacesRuleSet: Boolean,
      val getSqlOperatorTable: Option[SqlOperatorTable],
      val replacesSqlOperatorTable: Boolean,
      val getSqlParserConfig: Option[SqlParser.Config])
    extends CalciteConfig

  /**
    * Builds a new [[CalciteConfig]].
    */
  def build(): CalciteConfig = new CalciteConfigImpl(
    replaceRuleSet.orElse(chainRuleSet),
    replaceRuleSet.isDefined,
    replaceSqlOperatorTable.orElse(chainSqlOperatorTable),
    replaceSqlOperatorTable.isDefined,
    replaceSqlParserConfig)
}

/**
  * Calcite configuration for defining a custom Calcite configuration for Table and SQL API.
  */
trait CalciteConfig {
  /**
    * Returns whether this configuration replaces the built-in rule set.
    */
  def replacesRuleSet: Boolean

  /**
    * Returns a custom rule set.
    */
  def getRuleSet: Option[RuleSet]

  /**
    * Returns whether this configuration replaces the built-in SQL operator table.
    */
  def replacesSqlOperatorTable: Boolean

  /**
    * Returns a custom SQL operator table.
    */
  def getSqlOperatorTable: Option[SqlOperatorTable]

  /**
    * Returns a custom SQL parser configuration.
    */
  def getSqlParserConfig: Option[SqlParser.Config]
}

object CalciteConfig {

  val DEFAULT = createBuilder().build()

  /**
    * Creates a new builder for constructing a [[CalciteConfig]].
    */
  def createBuilder(): CalciteConfigBuilder = {
    new CalciteConfigBuilder
  }
}
