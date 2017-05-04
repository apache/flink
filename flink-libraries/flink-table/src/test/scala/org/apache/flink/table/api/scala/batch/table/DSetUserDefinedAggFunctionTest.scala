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
package org.apache.flink.table.api.scala.batch.table

import org.apache.flink.api.java.{DataSet => JDataSet, ExecutionEnvironment => JavaExecutionEnv}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment => ScalaExecutionEnv}
import org.apache.flink.table.api.java.utils.UserDefinedAggFunctions.WeightedAvg
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.functions.aggfunctions.CountAggFunction
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.types.Row
import org.junit._
import org.mockito.Mockito.{mock, when}


class DSetUserDefinedAggFunctionTest extends TableTestBase {

  @Test
  def testJavaScalaTableAPIEquality(): Unit = {
    // mock
    val ds = mock(classOf[DataSet[Row]])
    val jDs = mock(classOf[JDataSet[Row]])
    val typeInfo = new RowTypeInfo(Seq(Types.INT, Types.LONG, Types.STRING): _*)
    when(ds.javaSet).thenReturn(jDs)
    when(jDs.getType).thenReturn(typeInfo)

    // Scala environment
    val env = mock(classOf[ScalaExecutionEnv])
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val in1 = ds.toTable(tableEnv).as("int, long, string")
    // Java environment
    val javaEnv = mock(classOf[JavaExecutionEnv])
    val javaTableEnv = TableEnvironment.getTableEnvironment(javaEnv)
    val in2 = javaTableEnv.fromDataSet(jDs).as("int, long, string")

    // Register function for Java environment
    javaTableEnv.registerFunction("myCountFun", new CountAggFunction)
    javaTableEnv.registerFunction("weightAvgFun", new WeightedAvg)
    // Register function for Scala environment
    val myCountFun = new CountAggFunction
    val weightAvgFun = new WeightedAvg

    // Java API
    val javaTable = in2
      .groupBy("string")
      .select(
        "string, " +
          "myCountFun(string), " +
          "int.sum, " +
          "weightAvgFun(long, int), " +
          "weightAvgFun(int, int) * 2")
    // Scala API
    val scalaTable = in1
      .groupBy('string)
      .select(
        'string,
        myCountFun('string),
        'int.sum,
        weightAvgFun('long, 'int),
        weightAvgFun('int, 'int) * 2)
    verifyTableEquals(scalaTable, javaTable)
  }
}
