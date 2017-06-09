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
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableEnvironment, TableException, ValidationException}
import org.junit._

class JoinValidationTest {

  @Test(expected = classOf[ValidationException])
  def testJoinNonExistingKey(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. Field 'foo does not exist
      .where('foo === 'e)
      .select('c, 'g)
  }

  @Test(expected = classOf[ValidationException])
  def testJoinWithNonMatchingKeyTypes(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. Field 'a is Int, and 'g is String
      .where('a === 'g)
      .select('c, 'g).collect()
  }

  @Test(expected = classOf[ValidationException])
  def testJoinWithAmbiguousFields(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'c)

    ds1.join(ds2)
      // must fail. Both inputs share the same field 'c
      .where('a === 'd)
      .select('c, 'g)
  }

  @Test(expected = classOf[TableException])
  def testNoEqualityJoinPredicate1(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. No equality join predicate
      .where('d === 'f)
      .select('c, 'g).collect()
  }

  @Test(expected = classOf[TableException])
  def testNoEqualityJoinPredicate2(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. No equality join predicate
      .where('a < 'd)
      .select('c, 'g).collect()
  }

  @Test(expected = classOf[ValidationException])
  def testJoinTablesFromDifferentEnvs(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv1 = TableEnvironment.getTableEnvironment(env)
    val tEnv2 = TableEnvironment.getTableEnvironment(env)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv1, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv2, 'd, 'e, 'f, 'g, 'h)

    // Must fail. Tables are bound to different TableEnvironments.
    ds1.join(ds2).where('b === 'e).select('c, 'g)
  }

  @Test(expected = classOf[ValidationException])
  def testNoJoinCondition(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.getConfig.setNullCheck(true)

    val ds1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    ds2.leftOuterJoin(ds1, 'b === 'd && 'b < 3).select('c, 'g)
  }

  @Test(expected = classOf[ValidationException])
  def testNoEquiJoin(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.getConfig.setNullCheck(true)

    val ds1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    ds2.leftOuterJoin(ds1, 'b < 'd).select('c, 'g)
  }

  @Test(expected = classOf[ValidationException])
  @throws[Exception]
  def testJoinNonExistingKeyJava() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    val in1 = tableEnv.fromDataSet(ds1, 'a, 'b, 'c)
    val in2 = tableEnv.fromDataSet(ds2, 'd, 'e, 'f, 'g, 'h)
    // Must fail. Field foo does not exist.
    in1.join(in2).where("foo === e").select("c, g")
  }

  @Test(expected = classOf[ValidationException])
  @throws[Exception]
  def testJoinWithNonMatchingKeyTypesJava() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    val in1 = tableEnv.fromDataSet(ds1, 'a, 'b, 'c)
    val in2 = tableEnv.fromDataSet(ds2, 'd, 'e, 'f, 'g, 'c)
    in1.join(in2)
      // Must fail. Types of join fields are not compatible (Integer and String)
    .where("a === g").select("c, g")
  }

  @Test(expected = classOf[ValidationException])
  @throws[Exception]
  def testJoinWithAmbiguousFieldsJava() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    val in1 = tableEnv.fromDataSet(ds1, 'a, 'b, 'c)
    val in2 = tableEnv.fromDataSet(ds2, 'd, 'e, 'f, 'g, 'c)
    // Must fail. Join input have overlapping field names.
    in1.join(in2).where("a === d").select("c, g")
  }

  @Test(expected = classOf[ValidationException])
  def testJoinTablesFromDifferentEnvsJava() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv1 = TableEnvironment.getTableEnvironment(env)
    val tEnv2 = TableEnvironment.getTableEnvironment(env)
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    val in1 = tEnv1.fromDataSet(ds1, 'a, 'b, 'c)
    val in2 = tEnv2.fromDataSet(ds2, 'd, 'e, 'f, 'g, 'c)
    // Must fail. Tables are bound to different TableEnvironments.
    in1.join(in2).where("a === d").select("g.count")
  }
}
