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

package org.apache.flink.table.calcite

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.sql.SqlOperatorTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.util.ChainedSqlOperatorTable
import org.apache.calcite.tools.{RuleSet, RuleSets}
import org.apache.flink.util.Preconditions

import scala.collection.JavaConverters._

/**
  * Builder for creating a Calcite configuration.
  */
class CalciteConfigBuilder {
  private var replaceRules: Boolean = false
  private var ruleSets: List[RuleSet] = Nil

  private var replaceOperatorTable: Boolean = false
  private var operatorTables: List[SqlOperatorTable] = Nil

  private var replaceSqlParserConfig: Option[SqlParser.Config] = None

  /**
    * Replaces the built-in rule set with the given rule set.
    */
  def replaceRuleSet(replaceRuleSet: RuleSet): CalciteConfigBuilder = {
    Preconditions.checkNotNull(replaceRuleSet)
    ruleSets = List(replaceRuleSet)
    replaceRules = true
    this
  }

  /**
    * Appends the given rule set to the built-in rule set.
    */
  def addRuleSet(addedRuleSet: RuleSet): CalciteConfigBuilder = {
    Preconditions.checkNotNull(addedRuleSet)
    ruleSets = addedRuleSet :: ruleSets
    this
  }

  /**
    * Replaces the built-in SQL operator table with the given table.
    */
  def replaceSqlOperatorTable(replaceSqlOperatorTable: SqlOperatorTable): CalciteConfigBuilder = {
    Preconditions.checkNotNull(replaceSqlOperatorTable)
    operatorTables = List(replaceSqlOperatorTable)
    replaceOperatorTable = true
    this
  }

  /**
    * Appends the given table to the built-in SQL operator table.
    */
  def addSqlOperatorTable(addedSqlOperatorTable: SqlOperatorTable): CalciteConfigBuilder = {
    Preconditions.checkNotNull(addedSqlOperatorTable)
    this.operatorTables = addedSqlOperatorTable :: this.operatorTables
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
        ruleSets match {
      case Nil => None
      case h :: Nil => Some(h)
      case _ =>
        // concat rule sets
        val concatRules = ruleSets.foldLeft(Nil: Iterable[RelOptRule])( (c, r) => r.asScala ++ c)
        Some(RuleSets.ofList(concatRules.asJava))
    },
    this.replaceRules,
    operatorTables match {
      case Nil => None
      case h :: Nil => Some(h)
      case _ =>
        // chain operator tables
        Some(operatorTables.reduce( (x, y) => ChainedSqlOperatorTable.of(x, y)))
    },
    this.replaceOperatorTable,
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
