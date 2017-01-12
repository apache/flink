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
import org.apache.calcite.tools.{RuleSet, RuleSets}
import org.apache.flink.util.Preconditions

import scala.collection.JavaConverters._

/**
  * Builder for creating a RuleSet configuration.
  */
class RuleSetConfigBuilder {
  private var replaceNormRules: Boolean = false
  private var normRuleSets: List[RuleSet] = Nil

  private var replaceOptRules: Boolean = false
  private var optRuleSets: List[RuleSet] = Nil

  /**
    * Replaces the built-in normalization rule set with the given rule set.
    */
  def replaceNormRuleSet(replaceRuleSet: RuleSet): RuleSetConfigBuilder = {
    Preconditions.checkNotNull(replaceRuleSet)
    normRuleSets = List(replaceRuleSet)
    replaceNormRules = true
    this
  }

  /**
    * Appends the given normalization rule set to the built-in rule set.
    */
  def addNormRuleSet(addedRuleSet: RuleSet): RuleSetConfigBuilder = {
    Preconditions.checkNotNull(addedRuleSet)
    normRuleSets = addedRuleSet :: normRuleSets
    this
  }

  /**
    * Replaces the built-in optimization rule set with the given rule set.
    */
  def replaceOptRuleSet(replaceRuleSet: RuleSet): RuleSetConfigBuilder = {
    Preconditions.checkNotNull(replaceRuleSet)
    optRuleSets = List(replaceRuleSet)
    replaceOptRules = true
    this
  }

  /**
    * Appends the given optimization rule set to the built-in rule set.
    */
  def addOptRuleSet(addedRuleSet: RuleSet): RuleSetConfigBuilder = {
    Preconditions.checkNotNull(addedRuleSet)
    optRuleSets = addedRuleSet :: optRuleSets
    this
  }

  private class RuleSetConfigImpl(
    val replacesNormRuleSet: Boolean,
    val getNormRuleSet: Option[RuleSet],
    val replacesOptRuleSet: Boolean,
    val getOptRuleSet: Option[RuleSet])
    extends RuleSetConfig

  /**
    * Builds a new [[RuleSetConfig]].
    */
  def build(): RuleSetConfig = new RuleSetConfigImpl(
    replaceNormRules,
    normRuleSets match {
      case Nil => None
      case h :: Nil => Some(h)
      case _ =>
        // concat rule sets
        val concatRules =
          normRuleSets.foldLeft(Nil: Iterable[RelOptRule])((c, r) => r.asScala ++ c)
        Some(RuleSets.ofList(concatRules.asJava))
    },
    replaceOptRules,
    optRuleSets match {
      case Nil => None
      case h :: Nil => Some(h)
      case _ =>
        // concat rule sets
        val concatRules =
          optRuleSets.foldLeft(Nil: Iterable[RelOptRule])((c, r) => r.asScala ++ c)
        Some(RuleSets.ofList(concatRules.asJava))
    })
}

trait RuleSetConfig {
  /**
    * Returns whether this configuration replaces the built-in normalization rule set.
    */
  def replacesNormRuleSet: Boolean

  /**
    * Returns a custom normalization rule set.
    */
  def getNormRuleSet: Option[RuleSet]

  /**
    * Returns whether this configuration replaces the built-in optimization rule set.
    */
  def replacesOptRuleSet: Boolean

  /**
    * Returns a custom optimization rule set.
    */
  def getOptRuleSet: Option[RuleSet]
}

object RuleSetConfig {

  val DEFAULT = createBuilder().build()

  /**
    * Creates a new builder for constructing a [[RuleSetConfig]].
    */
  def createBuilder(): RuleSetConfigBuilder = {
    new RuleSetConfigBuilder
  }
}
