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

package org.apache.flink.table.api.scala.batch.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.table.api.{TableEnvironment, TableException, ValidationException}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit._

class CalcValidationTest {

  @Test(expected = classOf[ValidationException])
  def testSelectInvalidFieldFields(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      // must fail. Field 'foo does not exist
      .select('a, 'foo)
  }

  @Test(expected = classOf[ValidationException])
  def testSelectAmbiguousRenaming(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      // must fail. 'a and 'b are both renamed to 'foo
      .select('a + 1 as 'foo, 'b + 2 as 'foo).toDataSet[Row].print()
  }

  @Test(expected = classOf[ValidationException])
  def testSelectAmbiguousRenaming2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      // must fail. 'a and 'b are both renamed to 'a
      .select('a, 'b as 'a).toDataSet[Row].print()
  }

  @Test(expected = classOf[ValidationException])
  def testFilterInvalidFieldName(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)

    // must fail. Field 'foo does not exist
    ds.filter( 'foo === 2 )
  }

  @Test(expected = classOf[ValidationException])
  @throws[Exception]
  def testSelectInvalidField() {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tableEnv, 'a, 'b, 'c)

    // Must fail. Field foo does not exist
    ds.select("a + 1, foo + 2")
  }

  @Test(expected = classOf[ValidationException])
  @throws[Exception]
  def testSelectAmbiguousFieldNames() {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tableEnv, 'a, 'b, 'c)

    // Must fail. Field foo does not exist
    ds.select("a + 1 as foo, b + 2 as foo")
  }

  @Test(expected = classOf[ValidationException])
  @throws[Exception]
  def testFilterInvalidField() {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val table = CollectionDataSets.get3TupleDataSet(env).toTable(tableEnv, 'a, 'b, 'c)

    // Must fail. Field foo does not exist.
    table.filter("foo = 17")
  }

  @Test
  def testAliasStarException(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    try {
      CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, '*, 'b, 'c)
      fail("TableException expected")
    } catch {
      case _: TableException => //ignore
    }

    try {
      CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
        .select('_1 as '*, '_2 as 'b, '_1 as 'c)
      fail("ValidationException expected")
    } catch {
      case _: ValidationException => //ignore
    }

    try {
      CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).as('*, 'b, 'c)
      fail("ValidationException expected")
    } catch {
      case _: ValidationException => //ignore
    }
try {
      CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c).select('*, 'b)
      fail("ValidationException expected")
    } catch {
      case _: ValidationException => //ignore
    }
  }
}
