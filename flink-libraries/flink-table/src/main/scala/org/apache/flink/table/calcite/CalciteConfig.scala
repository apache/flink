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

import java.util.Properties

import org.apache.calcite.config.{CalciteConnectionConfig, CalciteConnectionConfigImpl, CalciteConnectionProperty}
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

  /**
    * Defines the normalization rule set. Normalization rules are dedicated for rewriting
    * predicated logical plan before volcano optimization.
    */
  private var replaceNormRules: Boolean = false
  private var normRuleSets: List[RuleSet] = Nil

  /**
    * Defines the logical optimization rule set.
    */
  private var replaceLogicalOptRules: Boolean = false
  private var logicalOptRuleSets: List[RuleSet] = Nil

  /**
    * Defines the physical optimization rule set.
    */
  private var replacePhysicalOptRules: Boolean = false
  private var physicalOptRuleSets: List[RuleSet] = Nil

  /**
    * Defines the decoration rule set. Decoration rules are dedicated for rewriting predicated
    * logical plan after volcano optimization.
    */
  private var replaceDecoRules: Boolean = false
  private var decoRuleSets: List[RuleSet] = Nil

  /**
    * Defines the SQL operator tables.
    */
  private var replaceOperatorTable: Boolean = false
  private var operatorTables: List[SqlOperatorTable] = Nil

  /**
    * Defines a SQL parser configuration.
    */
  private var replaceSqlParserConfig: Option[SqlParser.Config] = None

  /**
    * Replaces the built-in normalization rule set with the given rule set.
    */
  def replaceNormRuleSet(replaceRuleSet: RuleSet): CalciteConfigBuilder = {
    Preconditions.checkNotNull(replaceRuleSet)
    normRuleSets = List(replaceRuleSet)
    replaceNormRules = true
    this
  }

  /**
    * Appends the given normalization rule set to the built-in rule set.
    */
  def addNormRuleSet(addedRuleSet: RuleSet): CalciteConfigBuilder = {
    Preconditions.checkNotNull(addedRuleSet)
    normRuleSets = addedRuleSet :: normRuleSets
    this
  }

  /**
    * Replaces the built-in optimization rule set with the given rule set.
    */
  def replaceLogicalOptRuleSet(replaceRuleSet: RuleSet): CalciteConfigBuilder = {
    Preconditions.checkNotNull(replaceRuleSet)
    logicalOptRuleSets = List(replaceRuleSet)
    replaceLogicalOptRules = true
    this
  }

  /**
    * Appends the given optimization rule set to the built-in rule set.
    */
  def addLogicalOptRuleSet(addedRuleSet: RuleSet): CalciteConfigBuilder = {
    Preconditions.checkNotNull(addedRuleSet)
    logicalOptRuleSets = addedRuleSet :: logicalOptRuleSets
    this
  }

  /**
    * Replaces the built-in optimization rule set with the given rule set.
    */
  def replacePhysicalOptRuleSet(replaceRuleSet: RuleSet): CalciteConfigBuilder = {
    Preconditions.checkNotNull(replaceRuleSet)
    physicalOptRuleSets = List(replaceRuleSet)
    replacePhysicalOptRules = true
    this
  }

  /**
    * Appends the given optimization rule set to the built-in rule set.
    */
  def addPhysicalOptRuleSet(addedRuleSet: RuleSet): CalciteConfigBuilder = {
    Preconditions.checkNotNull(addedRuleSet)
    physicalOptRuleSets = addedRuleSet :: physicalOptRuleSets
    this
  }

  /**
    * Replaces the built-in decoration rule set with the given rule set.
    *
    * The decoration rules are applied after the cost-based optimization phase.
    * The decoration phase allows to rewrite the optimized plan and is not cost-based.
    *
    */
  def replaceDecoRuleSet(replaceRuleSet: RuleSet): CalciteConfigBuilder = {
    Preconditions.checkNotNull(replaceRuleSet)
    decoRuleSets = List(replaceRuleSet)
    replaceDecoRules = true
    this
  }

  /**
    * Appends the given decoration rule set to the built-in rule set.
    *
    * The decoration rules are applied after the cost-based optimization phase.
    * The decoration phase allows to rewrite the optimized plan and is not cost-based.
    */
  def addDecoRuleSet(addedRuleSet: RuleSet): CalciteConfigBuilder = {
    Preconditions.checkNotNull(addedRuleSet)
    decoRuleSets = addedRuleSet :: decoRuleSets
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
      val getNormRuleSet: Option[RuleSet],
      val replacesNormRuleSet: Boolean,
      val getLogicalOptRuleSet: Option[RuleSet],
      val replacesLogicalOptRuleSet: Boolean,
      val getPhysicalOptRuleSet: Option[RuleSet],
      val replacesPhysicalOptRuleSet: Boolean,
      val getDecoRuleSet: Option[RuleSet],
      val replacesDecoRuleSet: Boolean,
      val getSqlOperatorTable: Option[SqlOperatorTable],
      val replacesSqlOperatorTable: Boolean,
      val getSqlParserConfig: Option[SqlParser.Config])
    extends CalciteConfig


  /**
    * Convert the [[RuleSet]] List to [[Option]] type
    */
  private def getRuleSet(inputRuleSet: List[RuleSet]): Option[RuleSet] = {
    inputRuleSet match {
      case Nil => None
      case h :: Nil => Some(h)
      case _ =>
        // concat rule sets
        val concatRules =
          inputRuleSet.foldLeft(Nil: Iterable[RelOptRule])((c, r) => r.asScala ++ c)
        Some(RuleSets.ofList(concatRules.asJava))
    }
  }

  /**
    * Builds a new [[CalciteConfig]].
    */
  def build(): CalciteConfig = new CalciteConfigImpl(
    getRuleSet(normRuleSets),
    replaceNormRules,
    getRuleSet(logicalOptRuleSets),
    replaceLogicalOptRules,
    getRuleSet(physicalOptRuleSets),
    replacePhysicalOptRules,
    getRuleSet(decoRuleSets),
    replaceDecoRules,
    operatorTables match {
      case Nil => None
      case h :: Nil => Some(h)
      case _ =>
        // chain operator tables
        Some(operatorTables.reduce((x, y) => ChainedSqlOperatorTable.of(x, y)))
    },
    this.replaceOperatorTable,
    replaceSqlParserConfig)
}

/**
  * Calcite configuration for defining a custom Calcite configuration for Table and SQL API.
  */
trait CalciteConfig {

  /**
    * Returns whether this configuration replaces the built-in normalization rule set.
    */
  def replacesNormRuleSet: Boolean

  /**
    * Returns a custom normalization rule set.
    */
  def getNormRuleSet: Option[RuleSet]

  /**
    * Returns whether this configuration replaces the built-in logical optimization rule set.
    */
  def replacesLogicalOptRuleSet: Boolean

  /**
    * Returns a custom logical optimization rule set.
    */
  def getLogicalOptRuleSet: Option[RuleSet]

  /**
    * Returns whether this configuration replaces the built-in physical optimization rule set.
    */
  def replacesPhysicalOptRuleSet: Boolean

  /**
    * Returns a custom physical optimization rule set.
    */
  def getPhysicalOptRuleSet: Option[RuleSet]

  /**
    * Returns whether this configuration replaces the built-in decoration rule set.
    */
  def replacesDecoRuleSet: Boolean

  /**
    * Returns a custom decoration rule set.
    */
  def getDecoRuleSet: Option[RuleSet]

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

  val DEFAULT: CalciteConfig = createBuilder().build()

  /**
    * Creates a new builder for constructing a [[CalciteConfig]].
    */
  def createBuilder(): CalciteConfigBuilder = {
    new CalciteConfigBuilder
  }

  def connectionConfig(parserConfig : SqlParser.Config): CalciteConnectionConfig = {
    val prop = new Properties()
    prop.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName,
      String.valueOf(parserConfig.caseSensitive))
    new CalciteConnectionConfigImpl(prop)
  }
}
