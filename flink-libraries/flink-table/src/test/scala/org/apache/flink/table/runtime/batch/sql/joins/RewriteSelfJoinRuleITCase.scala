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

package org.apache.flink.table.runtime.batch.sql.joins

import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.row
import org.apache.flink.table.runtime.batch.sql.TestData.{nullablesOfPersonData, personData, personType}
import org.junit.{Before, Ignore, Test}
import org.scalatest.prop.PropertyChecks

class RewriteSelfJoinRuleITCase extends BatchTestBase with PropertyChecks {

  @Before
  def before(): Unit = {
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_EXEC_SORT_DEFAULT_LIMIT, -1)
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_OPTIMIZER_JOIN_REORDER_ENABLED, true)

    registerCollection(
      "person",
      personData,
      personType,
      "id, age, name, height, sex",
      nullablesOfPersonData)
  }

  @Test
  def testWholeGroupWithSimpleView(): Unit = {
    val sqlQuery =
      """
        |select * from person where age = (select max(age) from person p)
      """.stripMargin
    checkResult(sqlQuery, Seq(row(8, 34, "stef", 170, "m")))
  }

  @Test @Ignore
  def testNonPkCorrelation(): Unit = {
    val sqlQuery =
      """
        |with
        |tmp as
        |(select max(age) as ma, height from person where sex = 'f' group by height)
        |SELECT person.*,tmp.ma
        |from tmp, person
        |where person.age = tmp.ma and person.height = tmp.height
      """.stripMargin
    checkResult(sqlQuery, Seq())
  }

  @Test
  def testSameFiltersOnPKWithoutCorrelation(): Unit = {
    val sqlQuery =
      """
        |select * from person
        |where age = (select max(age) from person
        |             where name = 'benji')
        |and name = 'benji'
      """.stripMargin
    checkResult(sqlQuery, Seq(row(10, 28, "benji", 165, "m")))
  }

  @Test
  def testNonPkCorrelationWithSameFilter(): Unit = {
    val sqlQuery =
      """
        |select * from person
        |where age = (select max(age) from person p
        |             where name = 'emma' and p.height = person.height)
        |and name = 'emma'
      """.stripMargin
    checkResult(sqlQuery, Seq(row(9, 32, "emma", 171, "f")))
  }

  @Test @Ignore
  def testPKCorrelation(): Unit = {
    val sqlQuery =
      """
        |select * from person
        |where age = (select max(age)
        |             from person p
        |             where p.name = person.name)
      """.stripMargin
    checkResult(sqlQuery, Seq())
  }

  @Test @Ignore
  def testPKCorrelationWithNonAggFilter(): Unit = {
    // Non agg side filter condition be more strict, correlate var is PK.
    val sqlQuery =
      """
        |select * from person where
        |age = (select max(age) from person p where p.name = person.name)
        |and height = 175
      """.stripMargin
    checkResult(sqlQuery, Seq())
  }
}

