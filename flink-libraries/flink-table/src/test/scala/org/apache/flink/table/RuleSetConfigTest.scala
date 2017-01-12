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

package org.apache.flink.table

import org.apache.calcite.rel.rules.{CalcMergeRule, CalcSplitRule, FilterMergeRule,
ReduceExpressionsRule}
import org.apache.calcite.tools.RuleSets
import org.apache.flink.table.calcite.{RuleSetConfig, RuleSetConfigBuilder}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

class RuleSetConfigTest {

  @Test
  def testDefaultRules(): Unit = {

    val rc: RuleSetConfig = new RuleSetConfigBuilder()
      .build()

    assertFalse(rc.replacesNormRuleSet)
    assertFalse(rc.getNormRuleSet.isDefined)

    assertFalse(rc.replacesOptRuleSet)
    assertFalse(rc.getOptRuleSet.isDefined)
  }

  @Test
  def testRules(): Unit = {

    val rc: RuleSetConfig = new RuleSetConfigBuilder()
      .addNormRuleSet(RuleSets.ofList(ReduceExpressionsRule.FILTER_INSTANCE))
      .replaceOptRuleSet(RuleSets.ofList(FilterMergeRule.INSTANCE))
      .build()

    assertFalse(rc.replacesNormRuleSet)
    assertTrue(rc.getNormRuleSet.isDefined)

    assertTrue(rc.replacesOptRuleSet)
    assertTrue(rc.getOptRuleSet.isDefined)
  }

  @Test
  def testReplaceNormalizationRules(): Unit = {

    val rc: RuleSetConfig = new RuleSetConfigBuilder()
      .replaceNormRuleSet(RuleSets.ofList(ReduceExpressionsRule.FILTER_INSTANCE))
      .build()

    assertEquals(true, rc.replacesNormRuleSet)
    assertTrue(rc.getNormRuleSet.isDefined)
    val cSet = rc.getNormRuleSet.get.iterator().asScala.toSet
    assertEquals(1, cSet.size)
    assertTrue(cSet.contains(ReduceExpressionsRule.FILTER_INSTANCE))
  }

  @Test
  def testReplaceNormalizationAddRules(): Unit = {

    val rc: RuleSetConfig = new RuleSetConfigBuilder()
      .replaceNormRuleSet(RuleSets.ofList(ReduceExpressionsRule.FILTER_INSTANCE))
      .addNormRuleSet(RuleSets.ofList(ReduceExpressionsRule.PROJECT_INSTANCE))
      .build()

    assertEquals(true, rc.replacesNormRuleSet)
    assertTrue(rc.getNormRuleSet.isDefined)
    val cSet = rc.getNormRuleSet.get.iterator().asScala.toSet
    assertEquals(2, cSet.size)
    assertTrue(cSet.contains(ReduceExpressionsRule.FILTER_INSTANCE))
    assertTrue(cSet.contains(ReduceExpressionsRule.PROJECT_INSTANCE))
  }

  @Test
  def testAddNormalizationRules(): Unit = {

    val rc: RuleSetConfig = new RuleSetConfigBuilder()
      .addNormRuleSet(RuleSets.ofList(ReduceExpressionsRule.FILTER_INSTANCE))
      .build()

    assertEquals(false, rc.replacesNormRuleSet)
    assertTrue(rc.getNormRuleSet.isDefined)
    val cSet = rc.getNormRuleSet.get.iterator().asScala.toSet
    assertEquals(1, cSet.size)
    assertTrue(cSet.contains(ReduceExpressionsRule.FILTER_INSTANCE))
  }

  @Test
  def testAddAddNormalizationRules(): Unit = {

    val rc: RuleSetConfig = new RuleSetConfigBuilder()
      .addNormRuleSet(RuleSets.ofList(ReduceExpressionsRule.FILTER_INSTANCE))
      .addNormRuleSet(RuleSets.ofList(ReduceExpressionsRule.PROJECT_INSTANCE,
                                      ReduceExpressionsRule.CALC_INSTANCE))
      .build()

    assertEquals(false, rc.replacesNormRuleSet)
    assertTrue(rc.getNormRuleSet.isDefined)
    val cList = rc.getNormRuleSet.get.iterator().asScala.toList
    assertEquals(3, cList.size)
    assertEquals(cList.head, ReduceExpressionsRule.FILTER_INSTANCE)
    assertEquals(cList(1), ReduceExpressionsRule.PROJECT_INSTANCE)
    assertEquals(cList(2), ReduceExpressionsRule.CALC_INSTANCE)
  }

  @Test
  def testReplaceOptimizationRules(): Unit = {

    val rc: RuleSetConfig = new RuleSetConfigBuilder()
      .replaceOptRuleSet(RuleSets.ofList(FilterMergeRule.INSTANCE))
      .build()

    assertEquals(true, rc.replacesOptRuleSet)
    assertTrue(rc.getOptRuleSet.isDefined)
    val cSet = rc.getOptRuleSet.get.iterator().asScala.toSet
    assertEquals(1, cSet.size)
    assertTrue(cSet.contains(FilterMergeRule.INSTANCE))
  }

  @Test
  def testReplaceOptimizationAddRules(): Unit = {

    val rc: RuleSetConfig = new RuleSetConfigBuilder()
      .replaceOptRuleSet(RuleSets.ofList(FilterMergeRule.INSTANCE))
      .addOptRuleSet(RuleSets.ofList(CalcMergeRule.INSTANCE, CalcSplitRule.INSTANCE))
      .build()

    assertEquals(true, rc.replacesOptRuleSet)
    assertTrue(rc.getOptRuleSet.isDefined)
    val cSet = rc.getOptRuleSet.get.iterator().asScala.toSet
    assertEquals(3, cSet.size)
    assertTrue(cSet.contains(FilterMergeRule.INSTANCE))
    assertTrue(cSet.contains(CalcMergeRule.INSTANCE))
    assertTrue(cSet.contains(CalcSplitRule.INSTANCE))
  }

  @Test
  def testAddOptimizationRules(): Unit = {

    val rc: RuleSetConfig = new RuleSetConfigBuilder()
      .addOptRuleSet(RuleSets.ofList(FilterMergeRule.INSTANCE))
      .build()

    assertEquals(false, rc.replacesOptRuleSet)
    assertTrue(rc.getOptRuleSet.isDefined)
    val cSet = rc.getOptRuleSet.get.iterator().asScala.toSet
    assertEquals(1, cSet.size)
    assertTrue(cSet.contains(FilterMergeRule.INSTANCE))
  }

  @Test
  def testAddAddOptimizationRules(): Unit = {

    val rc: RuleSetConfig = new RuleSetConfigBuilder()
      .addOptRuleSet(RuleSets.ofList(FilterMergeRule.INSTANCE))
      .addOptRuleSet(RuleSets.ofList(CalcMergeRule.INSTANCE, CalcSplitRule.INSTANCE))
      .build()

    assertEquals(false, rc.replacesOptRuleSet)
    assertTrue(rc.getOptRuleSet.isDefined)
    val cSet = rc.getOptRuleSet.get.iterator().asScala.toSet
    assertEquals(3, cSet.size)
    assertTrue(cSet.contains(FilterMergeRule.INSTANCE))
    assertTrue(cSet.contains(CalcMergeRule.INSTANCE))
    assertTrue(cSet.contains(CalcSplitRule.INSTANCE))
  }

}
