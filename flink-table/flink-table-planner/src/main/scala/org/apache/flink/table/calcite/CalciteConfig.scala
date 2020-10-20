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

import org.apache.flink.annotation.Internal
import org.apache.flink.table.api.PlannerConfig
import org.apache.flink.util.Preconditions

import org.apache.calcite.config.{CalciteConnectionConfig, CalciteConnectionConfigImpl, CalciteConnectionProperty}
import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.sql.SqlOperatorTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.util.SqlOperatorTables
import org.apache.calcite.sql2rel.SqlToRelConverter
import org.apache.calcite.tools.{RuleSet, RuleSets}

import java.util.Properties

import scala.collection.JavaConverters._

/**
  * Builder for creating a Calcite configuration.
  */
@Internal
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
    * Defines the logical rewrite rule set.
    */
  private var replaceLogicalRewriteRules: Boolean = false
  private var logicalRewriteRuleSets: List[RuleSet] = Nil

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
    * Defines a configuration for SqlToRelConverter.
    */
  private var replaceSqlToRelConverterConfig: Option[SqlToRelConverter.Config] = None

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
    * Replaces the built-in logical rewrite rule set with the given rule set.
    */
  def replaceLogicalRewriteRuleSet(replaceRuleSet: RuleSet): CalciteConfigBuilder = {
    Preconditions.checkNotNull(replaceRuleSet)
    logicalRewriteRuleSets = List(replaceRuleSet)
    replaceLogicalRewriteRules = true
    this
  }

  /**
    * Appends the given logical rewrite rule set to the built-in rule set.
    */
  def addLogicalRewriteRuleSet(addedRuleSet: RuleSet): CalciteConfigBuilder = {
    Preconditions.checkNotNull(addedRuleSet)
    logicalRewriteRuleSets = addedRuleSet :: logicalRewriteRuleSets
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

  /**
    * Replaces the built-in SqlToRelConverter configuration with the given configuration.
    */
  def replaceSqlToRelConverterConfig(config: SqlToRelConverter.Config)
  : CalciteConfigBuilder = {
    Preconditions.checkNotNull(config)
    replaceSqlToRelConverterConfig = Some(config)
    this
  }

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
  def build(): CalciteConfig = new CalciteConfig(
    getRuleSet(normRuleSets),
    replaceNormRules,
    getRuleSet(logicalOptRuleSets),
    replaceLogicalOptRules,
    getRuleSet(logicalRewriteRuleSets),
    replaceLogicalRewriteRules,
    getRuleSet(physicalOptRuleSets),
    replacePhysicalOptRules,
    getRuleSet(decoRuleSets),
    replaceDecoRules,
    operatorTables match {
      case Nil => None
      case h :: Nil => Some(h)
      case _ =>
        // chain operator tables
        Some(operatorTables.reduce((x, y) => SqlOperatorTables.chain(x, y)))
    },
    this.replaceOperatorTable,
    replaceSqlParserConfig,
    replaceSqlToRelConverterConfig)
}

/**
  * Calcite configuration for defining a custom Calcite configuration for Table and SQL API.
  */
@Internal
class CalciteConfig(
  /** A custom normalization rule set. */
  val normRuleSet: Option[RuleSet],
  /** Whether this configuration replaces the built-in normalization rule set. */
  val replacesNormRuleSet: Boolean,
  /** A custom logical optimization rule set. */
  val logicalOptRuleSet: Option[RuleSet],
  /** Whether this configuration replaces the built-in logical optimization rule set. */
  val replacesLogicalOptRuleSet: Boolean,
  /** A custom logical rewrite rule set. */
  val logicalRewriteRuleSet: Option[RuleSet],
  /** Whether this configuration replaces the built-in logical rewrite rule set.  */
  val replacesLogicalRewriteRuleSet: Boolean,
  /** A custom physical optimization rule set. */
  val physicalOptRuleSet: Option[RuleSet],
  /** Whether this configuration replaces the built-in physical optimization rule set. */
  val replacesPhysicalOptRuleSet: Boolean,
  /** A custom decoration rule set. */
  val decoRuleSet: Option[RuleSet],
  /** Whether this configuration replaces the built-in decoration rule set. */
  val replacesDecoRuleSet: Boolean,
  /** A custom SQL operator table. */
  val sqlOperatorTable: Option[SqlOperatorTable],
  /** Whether this configuration replaces the built-in SQL operator table. */
  val replacesSqlOperatorTable: Boolean,
  /** A custom SQL parser configuration. */
  val sqlParserConfig: Option[SqlParser.Config],
  /** A custom configuration for SqlToRelConverter. */
  val sqlToRelConverterConfig: Option[SqlToRelConverter.Config]) extends PlannerConfig

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
