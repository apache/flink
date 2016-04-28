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

package org.apache.flink.api.scala.expression.utils

import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.`type`.SqlTypeName.VARCHAR
import org.apache.calcite.tools.{Frameworks, RelBuilder}
import org.apache.flink.api.common.functions.{Function, MapFunction}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.{DataSet => JDataSet}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.table.codegen.{CodeGenerator, GeneratedFunction}
import org.apache.flink.api.table.expressions.Expression
import org.apache.flink.api.table.runtime.FunctionCompiler
import org.apache.flink.api.table.{TableConfig, TableEnvironment}
import org.mockito.Mockito._

/**
  * Utility to translate and evaluate an RexNode or Table API expression to a String.
  */
object ExpressionEvaluator {

  // TestCompiler that uses current class loader
  class TestCompiler[T <: Function] extends FunctionCompiler[T] {
    def compile(genFunc: GeneratedFunction[T]): Class[T] =
      compile(getClass.getClassLoader, genFunc.name, genFunc.code)
  }

  private def prepareTable(
    typeInfo: TypeInformation[Any]): (String, RelBuilder, TableEnvironment) = {

    // create DataSetTable
    val dataSetMock = mock(classOf[DataSet[Any]])
    val jDataSetMock = mock(classOf[JDataSet[Any]])
    when(dataSetMock.javaSet).thenReturn(jDataSetMock)
    when(jDataSetMock.getType).thenReturn(typeInfo)

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val tableName = "myTable"
    tEnv.registerDataSet(tableName, dataSetMock)

    // prepare RelBuilder
    val relBuilder = tEnv.getRelBuilder
    relBuilder.scan(tableName)

    (tableName, relBuilder, tEnv)
  }

  def evaluate(data: Any, typeInfo: TypeInformation[Any], sqlExpr: String): String = {
    // create DataSetTable
    val table = prepareTable(typeInfo)

    // create RelNode from SQL expression
    val planner = Frameworks.getPlanner(table._3.getFrameworkConfig)
    val parsed = planner.parse("SELECT " + sqlExpr + " FROM " + table._1)
    val validated = planner.validate(parsed)
    val converted = planner.rel(validated)

    val expr: RexNode = converted.rel.asInstanceOf[LogicalProject].getChildExps.get(0)

    evaluate(data, typeInfo, table._2, expr)
  }

  def evaluate(data: Any, typeInfo: TypeInformation[Any], expr: Expression): String = {
    val relBuilder = prepareTable(typeInfo)._2
    evaluate(data, typeInfo, relBuilder, expr.toRexNode(relBuilder))
  }

  def evaluate(
      data: Any,
      typeInfo: TypeInformation[Any],
      relBuilder: RelBuilder,
      rexNode: RexNode): String = {
    // generate code for Mapper
    val config = new TableConfig()
    val generator = new CodeGenerator(config, typeInfo)
    val genExpr = generator.generateExpression(relBuilder.cast(rexNode, VARCHAR)) // cast to String
    val bodyCode =
      s"""
        |${genExpr.code}
        |return ${genExpr.resultTerm};
        |""".stripMargin
    val genFunc = generator.generateFunction[MapFunction[Any, String]](
      "TestFunction",
      classOf[MapFunction[Any, String]],
      bodyCode,
      STRING_TYPE_INFO.asInstanceOf[TypeInformation[Any]])

    // compile and evaluate
    val clazz = new TestCompiler[MapFunction[Any, String]]().compile(genFunc)
    val mapper = clazz.newInstance()
    mapper.map(data)
  }

}
