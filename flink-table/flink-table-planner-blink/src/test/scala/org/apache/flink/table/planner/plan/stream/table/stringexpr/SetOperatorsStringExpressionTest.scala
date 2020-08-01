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

package org.apache.flink.table.planner.plan.stream.table.stringexpr

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

class SetOperatorsStringExpressionTest extends TableTestBase {

  @Test
  def testUnionAll(): Unit = {
    val util = streamTestUtil()
    val t1 = util.addTableSource[(Int, Long, String)]('int, 'long, 'string)
    val t2 = util.addTableSource[(Int, Long, String)]('int, 'long, 'string)

    val resScala = t1.unionAll(t2).select('int)
    val resJava = t1.unionAll(t2).select("int")
    verifyTableEquals(resJava, resScala)
  }

  @Test
  def testUnionAllWithFilter(): Unit = {
    val util = streamTestUtil()
    val t1 = util.addTableSource[(Int, Long, String)]('int, 'long, 'string)
    val t2 = util.addTableSource[(Int, Long, Double, String)]('int, 'long, 'double, 'string)

    val resScala = t1.unionAll(t2.select('int, 'long, 'string)).filter('int < 2).select('int)
    val resJava = t1.unionAll(t2.select("int, long, string")).filter("int < 2").select("int")
    verifyTableEquals(resJava, resScala)
  }
}
