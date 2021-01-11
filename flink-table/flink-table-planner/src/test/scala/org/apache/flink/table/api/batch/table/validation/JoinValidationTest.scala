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

package org.apache.flink.table.api.batch.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.{TableException, ValidationException, _}
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.types.Row

import org.junit._

class JoinValidationTest extends TableTestBase {

  @Test
  def testJoinNonExistingKey(): Unit = {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. Field 'foo does not exist
      .where('foo === 'e)
      .select('c, 'g)
        }
    }

  @Test
  def testJoinWithNonMatchingKeyTypes(): Unit = {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. Field 'a is Int, and 'g is String
      .where('a === 'g)
      .select('c, 'g)

        }
    }

  @Test
  def testJoinWithAmbiguousFields(): Unit = {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'c)

    ds1.join(ds2)
      // must fail. Both inputs share the same field 'c
      .where('a === 'd)
      .select('c, 'g)
        }
    }

  @Test
  def testNoEqualityJoinPredicate1(): Unit = {
        assertThrows[TableException] {
                val util = batchTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. No equality join predicate
      .where('d === 'f)
      .select('c, 'g)
      .toDataSet[Row]
        }
    }

  @Test
  def testNoEqualityJoinPredicate2(): Unit = {
        assertThrows[TableException] {
                val util = batchTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. No equality join predicate
      .where('a < 'd)
      .select('c, 'g)
      .toDataSet[Row]
        }
    }

  @Test
  def testLeftJoinNoEquiJoinPredicate(): Unit = {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    ds2.leftOuterJoin(ds1, 'b < 'd).select('c, 'g)
        }
    }

  @Test
  def testRightJoinNoEquiJoinPredicate(): Unit = {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    ds2.rightOuterJoin(ds1, 'b < 'd).select('c, 'g)
        }
    }

  @Test
  def testFullJoinNoEquiJoinPredicate(): Unit = {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    ds2.fullOuterJoin(ds1, 'b < 'd).select('c, 'g)
        }
    }

  @Test
  def testJoinTablesFromDifferentEnvs(): Unit = {
        assertThrows[ValidationException] {
                val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv1 = BatchTableEnvironment.create(env)
    val tEnv2 = BatchTableEnvironment.create(env)
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    val in1 = tEnv1.fromDataSet(ds1, 'a, 'b, 'c)
    val in2 = tEnv2.fromDataSet(ds2, 'd, 'e, 'f, 'g, 'c)

    // Must fail. Tables are bound to different TableEnvironments.
    in1.join(in2).where('b === 'e).select('c, 'g)
        }
    }

  @Test
  def testJoinTablesFromDifferentEnvsJava() {
        assertThrows[ValidationException] {
                val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv1 = BatchTableEnvironment.create(env)
    val tEnv2 = BatchTableEnvironment.create(env)
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    val in1 = tEnv1.fromDataSet(ds1, 'a, 'b, 'c)
    val in2 = tEnv2.fromDataSet(ds2, 'd, 'e, 'f, 'g, 'c)
    // Must fail. Tables are bound to different TableEnvironments.
    in1.join(in2).where($"a" === $"d").select($"g".count)
        }
    }
}
