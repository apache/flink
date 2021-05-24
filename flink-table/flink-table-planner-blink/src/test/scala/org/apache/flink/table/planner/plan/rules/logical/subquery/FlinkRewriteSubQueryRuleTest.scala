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

package org.apache.flink.table.planner.plan.rules.logical.subquery

import org.apache.flink.api.scala._
import org.apache.flink.table.api._

import org.junit.{Before, Test}

/**
  * Test for [[org.apache.flink.table.planner.plan.rules.logical.FlinkRewriteSubQueryRule]].
  */
class FlinkRewriteSubQueryRuleTest extends SubQueryTestBase {

  @Before
  def setup(): Unit = {
    util.addTableSource[(Int, Long, String)]("x", 'a, 'b, 'c)
    util.addTableSource[(Int, Long, String)]("y", 'd, 'e, 'f)
  }

  @Test
  def testNotCountStarInScalarQuery(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE (SELECT COUNT(e) FROM y WHERE d > 10) > 0")
  }

  @Test
  def testNotEmptyGroupByInScalarQuery(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE (SELECT COUNT(*) FROM y WHERE d > 10 GROUP BY f) > 0")
  }

  @Test
  def testUnsupportedConversionWithUnexpectedComparisonNumber(): Unit = {
    // without correlation
    util.verifyRelPlanNotExpected(
      "SELECT * FROM x WHERE (SELECT COUNT(*) FROM y WHERE d > 10) > 1", "joinType=[semi]")
    util.verifyRelPlanNotExpected(
      "SELECT * FROM x WHERE (SELECT COUNT(*) FROM y WHERE d > 10) >= 0", "joinType=[semi]")
    util.verifyRelPlanNotExpected(
      "SELECT * FROM x WHERE (SELECT COUNT(*) FROM y WHERE d > 10) > -1", "joinType=[semi]")
    util.verifyRelPlanNotExpected(
      "SELECT * FROM x WHERE 0 <= (SELECT COUNT(*) FROM y WHERE d > 10)", "joinType=[semi]")
    util.verifyRelPlanNotExpected(
      "SELECT * FROM x WHERE -1 < (SELECT COUNT(*) FROM y WHERE d > 10)", "joinType=[semi]")

    // with correlation
    util.verifyRelPlanNotExpected(
      "SELECT * FROM x WHERE (SELECT COUNT(*) FROM y WHERE a = d) > 1", "joinType=[semi]")
    util.verifyRelPlanNotExpected(
      "SELECT * FROM x WHERE (SELECT COUNT(*) FROM y WHERE a = d) >= 0", "joinType=[semi]")
    util.verifyRelPlanNotExpected(
      "SELECT * FROM x WHERE 1 < (SELECT COUNT(*) FROM y WHERE a = d)", "joinType=[semi]")
    util.verifyRelPlanNotExpected(
      "SELECT * FROM x WHERE 0 <= (SELECT COUNT(*) FROM y WHERE a = d)", "joinType=[semi]")
  }

  @Test
  def testSupportedConversionWithoutCorrelation1(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE (SELECT COUNT(*) FROM y WHERE d > 10) > 0")
  }

  @Test
  def testSupportedConversionWithoutCorrelation2(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE (SELECT COUNT(*) FROM y WHERE d > 10) > 0.9")
  }

  @Test
  def testSupportedConversionWithoutCorrelation3(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE (SELECT COUNT(*) FROM y WHERE d > 10) >= 1")
  }

  @Test
  def testSupportedConversionWithoutCorrelation4(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE (SELECT COUNT(*) FROM y WHERE d > 10) >= 0.1")
  }

  @Test
  def testSupportedConversionWithoutCorrelation5(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE 0 < (SELECT COUNT(*) FROM y WHERE d > 10)")
  }

  @Test
  def testSupportedConversionWithoutCorrelation6(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE 0.99 < (SELECT COUNT(*) FROM y WHERE d > 10)")
  }

  @Test
  def testSupportedConversionWithoutCorrelation7(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE 1 <= (SELECT COUNT(*) FROM y WHERE d > 10)")
  }

  @Test
  def testSupportedConversionWithoutCorrelation8(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE 0.01 <= (SELECT COUNT(*) FROM y WHERE d > 10)")
  }

  @Test
  def testSupportedConversionWithCorrelation1(): Unit = {
    // with correlation
    util.verifyRelPlan("SELECT * FROM x WHERE (SELECT COUNT(*) FROM y WHERE a = d) > 0")
  }

  @Test
  def testSupportedConversionWithCorrelation2(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE (SELECT COUNT(*) FROM y WHERE a = d) > 0.9")
  }

  @Test
  def testSupportedConversionWithCorrelation3(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE (SELECT COUNT(*) FROM y WHERE a = d) >= 1")
  }

  @Test
  def testSupportedConversionWithCorrelation4(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE (SELECT COUNT(*) FROM y WHERE a = d) >= 0.1")
  }

  @Test
  def testSupportedConversionWithCorrelation5(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE 0 < (SELECT COUNT(*) FROM y WHERE a = d)")
  }

  @Test
  def testSupportedConversionWithCorrelation6(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE 0.99 < (SELECT COUNT(*) FROM y WHERE a = d)")
  }

  @Test
  def testSupportedConversionWithCorrelation7(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE 1 <= (SELECT COUNT(*) FROM y WHERE a = d)")
  }

  @Test
  def testSupportedConversionWithCorrelation8(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE 0.01 <= (SELECT COUNT(*) FROM y WHERE a = d)")
  }

  @Test
  def testSqlFromTpcDsQ41(): Unit = {
    util.addTableSource[(Int, String, String, String, String, String, String)]("item",
      'i_manufact_id, 'i_manufact, 'i_product_name, 'i_category, 'i_color, 'i_units, 'i_size)
    val sqlQuery =
      """
        |SELECT DISTINCT (i_product_name)
        |FROM item i1
        |WHERE i_manufact_id BETWEEN 738 AND 738 + 40
        |  AND (SELECT count(*) AS item_cnt
        |FROM item
        |WHERE (i_manufact = i1.i_manufact AND
        |  ((i_category = 'Women' AND
        |    (i_color = 'powder' OR i_color = 'khaki') AND
        |    (i_units = 'Ounce' OR i_units = 'Oz') AND
        |    (i_size = 'medium' OR i_size = 'extra large')
        |  ) OR
        |    (i_category = 'Women' AND
        |      (i_color = 'brown' OR i_color = 'honeydew') AND
        |      (i_units = 'Bunch' OR i_units = 'Ton') AND
        |      (i_size = 'N/A' OR i_size = 'small')
        |    ) OR
        |    (i_category = 'Men' AND
        |      (i_color = 'floral' OR i_color = 'deep') AND
        |      (i_units = 'N/A' OR i_units = 'Dozen') AND
        |      (i_size = 'petite' OR i_size = 'large')
        |    ) OR
        |    (i_category = 'Men' AND
        |      (i_color = 'light' OR i_color = 'cornflower') AND
        |      (i_units = 'Box' OR i_units = 'Pound') AND
        |      (i_size = 'medium' OR i_size = 'extra large')
        |    ))) OR
        |  (i_manufact = i1.i_manufact AND
        |    ((i_category = 'Women' AND
        |      (i_color = 'midnight' OR i_color = 'snow') AND
        |      (i_units = 'Pallet' OR i_units = 'Gross') AND
        |      (i_size = 'medium' OR i_size = 'extra large')
        |    ) OR
        |      (i_category = 'Women' AND
        |        (i_color = 'cyan' OR i_color = 'papaya') AND
        |        (i_units = 'Cup' OR i_units = 'Dram') AND
        |        (i_size = 'N/A' OR i_size = 'small')
        |      ) OR
        |      (i_category = 'Men' AND
        |        (i_color = 'orange' OR i_color = 'frosted') AND
        |        (i_units = 'Each' OR i_units = 'Tbl') AND
        |        (i_size = 'petite' OR i_size = 'large')
        |      ) OR
        |      (i_category = 'Men' AND
        |        (i_color = 'forest' OR i_color = 'ghost') AND
        |        (i_units = 'Lb' OR i_units = 'Bundle') AND
        |        (i_size = 'medium' OR i_size = 'extra large')
        |      )))) > 0
        |ORDER BY i_product_name
        |LIMIT 100
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

}
