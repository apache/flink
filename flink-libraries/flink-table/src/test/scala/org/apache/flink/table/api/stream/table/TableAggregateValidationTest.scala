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

package org.apache.flink.table.api.stream.table

import java.sql.Timestamp

import org.apache.flink.api.scala._
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.{EmptyTableAggFunc, TableTestBase}
import org.junit.Test

class TableAggregateValidationTest extends TableTestBase {

  @Test(expected = classOf[ValidationException])
  def testInvalidParameterNumber(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc
    table
      .groupBy('c)
      // must fail. func take 2 parameters
      .flatAggregate(func('a))
      .select('_1, '_2, '_3)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidParameterType(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc
    table
      .groupBy('c)
      // must fail. func take 2 parameters of type Long and Timestamp
      .flatAggregate(func('a, 'c))
      .select('_1, '_2, '_3)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidParameterWithAgg(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, Timestamp)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc
    table
      .groupBy('b)
      // must fail. func take agg function as input
      .flatAggregate(func('a.sum, 'c))
      .select('_1, '_2, '_3)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidAliasWithWrongNumber(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, Timestamp)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc
    table
      .groupBy('b)
      // must fail. alias with wrong number of fields
      .flatAggregate(func('a, 'b) as ('a, 'b))
      .select('*)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidAliasWithNonUnresolvedFieldReference(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, Timestamp)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc
    table
      .groupBy('b)
      // must fail. alias with wrong number of fields
      .flatAggregate(func('a, 'b) as ('a, 'b, 'c + 1))
      .select('*)
  }
}
