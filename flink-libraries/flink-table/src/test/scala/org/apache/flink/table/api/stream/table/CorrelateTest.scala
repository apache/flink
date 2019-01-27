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

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils.Func13
import org.apache.flink.table.util._
import org.junit.Test

class CorrelateTest extends TableTestBase {

  @Test
  def testCrossJoin(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new TableFunc1
    util.addFunction("func1", function)

    val result = table.join(function('c) as 's).select('c, 's)
    
    util.verifyPlan(result)
  }

  @Test
  def testCrossJoinWithOverloading(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new TableFunc1
    util.addFunction("func1", function)
    
    val result = table.join(function('c, "$") as 's).select('c, 's)
    
    util.verifyPlan(result)
  }

  @Test
  def testLeftOuterJoinWithLiteralTrue(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new TableFunc1
    util.addFunction("func1", function)

    val result = table.leftOuterJoin(function('c) as 's, true).select('c, 's)

    util.verifyPlan(result)
  }

  @Test
  def testCustomType(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new TableFunc2
    val scalarFunc = new Func13("pre")
    util.addFunction("func2", function)

    val result = table.join(function(scalarFunc('c)) as ('name, 'len)).select('c, 'name, 'len)

    util.verifyPlan(result)
  }

  @Test
  def testHierarchyType(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new HierarchyTableFunction
    util.addFunction("hierarchy", function)

    val result = table.join(function('c) as ('name, 'adult, 'len))

    util.verifyPlan(result)
  }

  @Test
  def testPojoType(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new PojoTableFunc
    util.addFunction("pojo", function)

    val result = table.join(function('c))

    util.verifyPlan(result)
  }

  @Test
  def testFilter(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new TableFunc2
    util.addFunction("func2", function)

    val result = table
      .join(function('c) as ('name, 'len))
      .select('c, 'name, 'len)
      .filter('len > 2)

    util.verifyPlan(result)
  }

  @Test
  def testScalarFunction(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new TableFunc1
    util.addFunction("func1", function)

    val result = table.join(function('c.substring(2)) as 's)

    util.verifyPlan(result)
  }

  @Test
  def testDynamicType(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val funcDyn = new UDTFWithDynamicType
    util.addFunction("funcDyn", funcDyn)
    val result = table
        .join(funcDyn('c, 1) as 'name)
        .select('c, 'name)
    util.verifyPlan(result)
  }

  @Test
  def testDynamicType1(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val funcDyn = new UDTFWithDynamicType
    util.addFunction("funcDyn", funcDyn)
    val result1 = table
        .join(funcDyn('c, 2) as ('name, 'len0))
        .select('c, 'name, 'len0)
    util.verifyPlan(result1)
  }

  @Test
  def testDynamicType2(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val funcDyn = new UDTFWithDynamicType
    util.addFunction("funcDyn", funcDyn)
    val result2 = table
        .join(funcDyn('c, 3) as ('name, 'len0, 'len1))
        .select('c, 'name, 'len0, 'len1)
    util.verifyPlan(result2)
  }

  @Test
  def testDynamicType3(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val funcDyn = new UDTFWithDynamicType
    util.addFunction("funcDyn", funcDyn)
    val result3 = table
        .join(funcDyn('c, 3) as ('name, 'len0, 'len1))
        .select('c, 'name, 'len0, 'len1)
        .join(funcDyn('c, 2) as ('name1, 'len10))
        .select('c, 'name, 'len0, 'len1, 'name1, 'len10)
    util.verifyPlan(result3)
  }

  @Test
  def testDynamicType4(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val funcDyn1 = new UDTFWithDynamicType1
    val result4 = table.join(funcDyn1("string") as 'col)
        .select('col)
    util.verifyPlan(result4)
  }

  @Test
  def testDynamicType5(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val funcDyn1 = new UDTFWithDynamicType1
    util.addFunction("funcDyn1", funcDyn1)
    val result5 = table.join(funcDyn1("int") as 'col)
        .select('col)
    util.verifyPlan(result5)
  }

  @Test
  def testDynamicType6(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val funcDyn1 = new UDTFWithDynamicType1
    util.addFunction("funcDyn1", funcDyn1)
    val result6 = table.join(funcDyn1("double") as 'col)
        .select('col)
    util.verifyPlan(result6)
  }

  @Test
  def testDynamicType7(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val funcDyn1 = new UDTFWithDynamicType1
    util.addFunction("funcDyn1", funcDyn1)
    val result7 = table.join(funcDyn1("boolean") as 'col)
        .select('col)
    util.verifyPlan(result7)
  }

  @Test
  def testDynamicType8(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val funcDyn1 = new UDTFWithDynamicType1
    util.addFunction("funcDyn1", funcDyn1)
    val result8 = table.join(funcDyn1("timestamp") as 'col)
        .select('col)
    util.verifyPlan(result8)
  }
}
