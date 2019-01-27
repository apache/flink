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

package org.apache.flink.table.expressions

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.types.{DataTypes, DecimalType, TypeConverters}
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.typeutils.TypeUtils
import org.apache.flink.types.Row

import org.junit.{Assert, Before, Test}

import scala.collection.Seq

/**
 * Test type inference of expressions with different argument types.
 */
class TypeInferenceTest extends BatchTestBase {

  private val TABLE_NAME = "TABLE_TypeInferenceTest"

  private val i = Types.INT
  private val l = Types.LONG
  private val m = BasicTypeInfo.BIG_DEC_TYPE_INFO
  private val f = Types.FLOAT
  private val d = Types.DOUBLE

  private val columns: Seq[(String,TypeInformation[_])]
    = Seq(("i" ,i), ("l", l), ("m", m), ("f", f), ("d", d))

  private val columnNames: Seq[String] = columns.map(_._1)
  private val columnTypes = columns.map(_._2)

  @Before
  def before(): Unit = {
    val fieldNames = columnNames.mkString(",")
    val rowTypeInfo = new RowTypeInfo(columnTypes:_*)
    registerCollection(TABLE_NAME, Seq[Row](), rowTypeInfo, fieldNames)
  }

  def checkType(expr: String, expectedType: TypeInformation[_]*): Unit = {
    val sql = s"select $expr from $TABLE_NAME"
    val returnTypes = parseQuery(sql).getSchema.getTypes
    // in these tests, we do not care about detailed precision/scale of decimals.
    val compareTypes = returnTypes.map {
      case _: DecimalType => BasicTypeInfo.BIG_DEC_TYPE_INFO
      case t => TypeConverters.createExternalTypeInfoFromDataType(t)
    }
    Assert.assertEquals(expectedType.toSeq, compareTypes.toSeq)
  }
  def checkType(op: String, expr: String, expectedType: TypeInformation[_]*): Unit =
    checkType(expr.replace("~", op), expectedType:_*)

  @Test
  def testBasics(): Unit = {
    checkType("i, l, m, f, d", i, l, m, f, d)

    checkType("+i, +l, +m, +f, +d", i, l, m, f, d)
    checkType("-i, -l, -m, -f, -d", i, l, m, f, d)
  }

  @Test
  def testPlusMinusMultiply(): Unit = {
    Seq("+", "-", "*").foreach(op => {
      checkType(op, "i~i, i~l, i~m, i~f, i~d", i, l, m, f, d)
      checkType(op, "l~i, l~l, l~m, l~f, l~d", l, l, m, f, d)
      checkType(op, "m~i, m~l, m~m, m~f, m~d", m, m, m, d, d)
      checkType(op, "f~i, f~l, f~m, f~f, f~d", f, f, d, f, d)
      checkType(op, "d~i, d~l, d~m, d~f, d~d", d, d, d, d, d)
    })
  }
  
  @Test 
  def testDivide(): Unit = {
    checkType("i/i, i/l, i/m, i/f, i/d", d, d, m, d, d)
    checkType("l/i, l/l, l/m, l/f, l/d", d, d, m, d, d)
    checkType("m/i, m/l, m/m, m/f, m/d", m, m, m, d, d)
    checkType("f/i, f/l, f/m, f/f, f/d", d, d, d, d, d)
    checkType("d/i, d/l, d/m, d/f, d/d", d, d, d, d, d)
  }

  @Test
  def testModDiv(): Unit = {
    // mod(T0, T1) => T1
    checkType("mod", "~(i,i), ~(i,l), ~(i,m)", i, l, m)
    checkType("mod", "~(l,i), ~(l,l), ~(l,m)", i, l, m)
    checkType("mod", "~(m,i), ~(m,l), ~(m,m)", i, l, m)

    // div(T0, T1) => see DivCallGen
    checkType("div", "~(i,i), ~(i,l), ~(i,m)", i, i, m)
    checkType("div", "~(l,i), ~(l,l), ~(l,m)", l, l, m)
    checkType("div", "~(m,i), ~(m,l), ~(m,m)", m, m, m)
  }

  @Test
  def testMathFunctions(): Unit = {
    // return type of arg0
    Seq("sign", "abs", "floor", "ceil", "round").foreach(op =>
      checkType(op, "~(i), ~(l), ~(m), ~(f), ~(d)", i, l, m, f, d))

    // ROUND(T, n) => T
    checkType("round", "~(i,1), ~(l,1), ~(m,1), ~(f,1), ~(d,1)", i, l, m, f, d)

    // return `double` for all arg types. no need to list all such functions.
    Seq("log", "exp", "sin", "cos", "degrees", "radians").foreach(op =>
        columnNames.foreach(col => checkType(op, s"~($col)", d)))

    // POWER(Ta, Tb) => double
    columnNames.foreach(a => columnNames.foreach(b =>
      checkType(s"power($a, $b)", d)))
  }

  @Test
  def testAggs(): Unit = {

    checkType("count(*), count(i)", l, l)

    // return type of arg0
    Seq("min", "max", "sum", "first_value", "last_value").foreach(func =>
        for ((col, colType) <- columns){
          checkType(s"$func($col) over()", colType)
        })

    // AVG() etc.
    Seq("avg", "stddev_pop", "stddev_samp", "var_pop", "var_samp").foreach(op =>
      checkType(op, "~(i), ~(l), ~(m), ~(f), ~(d)", d, d, m, d, d))

  }
}
