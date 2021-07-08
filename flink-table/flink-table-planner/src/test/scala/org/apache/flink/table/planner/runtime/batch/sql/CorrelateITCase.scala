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

package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO
import org.apache.flink.api.common.typeinfo.{LocalTimeTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.{PojoField, PojoTypeInfo, RowTypeInfo, TypeExtractor}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.data.StringData
import org.apache.flink.table.functions.{ScalarFunction, TableFunction}
import org.apache.flink.table.planner.expressions.utils.{Func1, Func18, RichFunc2}
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions.JavaTableFunc0
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.table.planner.runtime.utils.UserDefinedFunctionTestUtils.{MyPojo, MyPojoFunc}
import org.apache.flink.table.planner.utils.DateTimeTestUtil._
import org.apache.flink.table.planner.utils.{HierarchyTableFunction, PojoTableFunc, RichTableFunc1, TableFunc0, TableFunc1, TableFunc2, TableFunc3, VarArgsFunc0}
import org.apache.flink.table.runtime.typeutils.StringDataTypeInfo
import org.apache.flink.types.Row

import org.junit.{Before, Test}

import scala.collection.JavaConversions._
import scala.collection.Seq

class CorrelateITCase extends BatchTestBase {

  @Before
  override def before(): Unit = {
    super.before()
    registerCollection("inputT", TableFunctionITCase.testData, type3, "a, b, c")
    registerCollection("inputTWithNull", TableFunctionITCase.testDataWithNull, type3, "a, b, c")
    registerCollection("SmallTable3", smallData3, type3, "a, b, c")
  }

  @Test
  def testTableFunction(): Unit = {
    registerFunction("func", new TableFunc1)
    checkResult(
      "select c, s from inputT, LATERAL TABLE(func(c)) as T(s)",
      Seq(
        row("Jack#22", "Jack"),
        row("Jack#22", "22"),
        row("John#19", "John"),
        row("John#19", "19"),
        row("Anna#44", "Anna"),
        row("Anna#44", "44")
      ))
  }

  @Test
  def testLeftOuterJoin(): Unit = {
    registerFunction("func", new TableFunc2)
    checkResult(
      "select c, s, l from inputT LEFT JOIN LATERAL TABLE(func(c)) as T(s, l) ON TRUE",
      Seq(
        row("Jack#22", "Jack", 4),
        row("Jack#22", "22", 2),
        row("John#19", "John", 4),
        row("John#19", "19", 2),
        row("Anna#44", "Anna", 4),
        row("Anna#44", "44", 2),
        row("nosharp", null, null)))
  }

  @Test
  def testWithFilter(): Unit = {
    registerFunction("func", new TableFunc0)
    checkResult(
      "select c, name, age from inputT, LATERAL TABLE(func(c)) as T(name, age) WHERE T.age > 20",
      Seq(
        row("Jack#22", "Jack", 22),
        row("Anna#44", "Anna", 44)))
  }

  @Test
  def testHierarchyType(): Unit = {
    registerFunction("func", new HierarchyTableFunction)
    checkResult(
      "select c, name, adult, len from inputT, LATERAL TABLE(func(c)) as T(name, adult, len)",
      Seq(
        row("Jack#22", "Jack", true, 22),
        row("John#19", "John", false, 19),
        row("Anna#44", "Anna", true, 44)))
  }

  /**
    * T(age, name) must have the right order with TypeInfo of PojoUser.
    */
  @Test
  def testPojoType(): Unit = {
    registerFunction("func", new PojoTableFunc)
    checkResult(
      "select c, name, age from inputT, LATERAL TABLE(func(c)) as T(age, name) WHERE T.age > 20",
      Seq(
        row("Jack#22", "Jack", 22),
        row("Anna#44", "Anna", 44)))
  }

  @Test
  def testUserDefinedTableFunctionWithScalarFunction(): Unit = {
    registerFunction("func", new TableFunc1)
    checkResult(
      "select c, s from inputT, LATERAL TABLE(func(SUBSTRING(c, 2))) as T(s)",
      Seq(
        row("Jack#22", "ack"),
        row("Jack#22", "22"),
        row("John#19", "ohn"),
        row("John#19", "19"),
        row("Anna#44", "nna"),
        row("Anna#44", "44")
      ))
  }

  @Test
  def testUserDefinedTableFunctionWithScalarFunctionInCondition(): Unit = {
    registerFunction("func", new TableFunc0)
    registerFunction("func18", Func18)
    registerFunction("func1", Func1)
    checkResult(
      "select c, name, age from inputT, LATERAL TABLE(func(c)) as T(name, age) " +
        "where func18(name, 'J') and func1(a) < 3 and func1(age) > 20",
      Seq(
        row("Jack#22", "Jack", 22)
      ))
  }

  @Test
  def testLongAndTemporalTypes(): Unit = {
    registerCollection("myT", Seq(
      row(localDate("1990-10-14"), 1000L, localDateTime("1990-10-14 12:10:10"))),
      new RowTypeInfo(LocalTimeTypeInfo.LOCAL_DATE,
        LONG_TYPE_INFO, LocalTimeTypeInfo.LOCAL_DATE_TIME),
      "x, y, z")
    registerFunction("func", new JavaTableFunc0)
    checkResult(
      "select s from myT, LATERAL TABLE(func(x, y, z)) as T(s)",
      Seq(
        row(1000L),
        row(655906210000L),
        row(7591L)
      ))
  }

  @Test
  def testUserDefinedTableFunctionWithParameter(): Unit = {
    registerFunction("func", new RichTableFunc1)
    val conf = new Configuration()
    conf.setString("word_separator", "#")
    env.getConfig.setGlobalJobParameters(conf)
    checkResult(
      "select a, s from inputT, LATERAL TABLE(func(c)) as T(s)",
      Seq(
        row(1, "Jack"),
        row(1, "22"),
        row(2, "John"),
        row(2, "19"),
        row(3, "Anna"),
        row(3, "44")))
  }

  @Test
  def testUserDefinedTableFunctionWithScalarFunctionWithParameters(): Unit = {
    registerFunction("func", new RichTableFunc1)
    registerFunction("func2", new RichFunc2)
    val conf = new Configuration()
    conf.setString("word_separator", "#")
    conf.setString("string.value", "test")
    env.getConfig.setGlobalJobParameters(conf)
    checkResult(
      "select a, s from SmallTable3, LATERAL TABLE(func(func2(c))) as T(s)",
      Seq(
        row(1, "Hi"),
        row(1, "test"),
        row(2, "Hello"),
        row(2, "test"),
        row(3, "Hello world"),
        row(3, "test")))
  }

  @Test
  def testTableFunctionConstructorWithParams(): Unit = {
    registerFunction("func30", new TableFunc3(null))
    registerFunction("func31", new TableFunc3("OneConf_"))
    registerFunction("func32", new TableFunc3("TwoConf_"))
    checkResult(
      "select c, d, f, h, e, g, i from inputT, " +
        "LATERAL TABLE(func30(c)) as T0(d, e), " +
        "LATERAL TABLE(func31(c)) as T1(f, g)," +
        "LATERAL TABLE(func32(c)) as T2(h, i)",
      Seq(
        row("Anna#44", "Anna", "OneConf_Anna", "TwoConf_Anna", 44, 44, 44),
        row("Jack#22", "Jack", "OneConf_Jack", "TwoConf_Jack", 22, 22, 22),
        row("John#19", "John", "OneConf_John", "TwoConf_John", 19, 19, 19)
      ))
  }

  @Test
  def testTableFunctionWithVariableArguments(): Unit = {
    registerFunction("func", new VarArgsFunc0)
    checkResult(
      "select c, d from inputT, LATERAL TABLE(func('1', '2', c)) as T0(d) where c = 'Jack#22'",
      Seq(
        row("Jack#22", 1),
        row("Jack#22", 2),
        row("Jack#22", "Jack#22")
      ))
  }

  @Test
  def testPojoField(): Unit = {
    val data = Seq(
      row(new MyPojo(5, 105)),
      row(new MyPojo(6, 11)),
      row(new MyPojo(7, 12)))
    registerCollection("MyTable", data,
      new RowTypeInfo(TypeExtractor.createTypeInfo(classOf[MyPojo])), "a")

    //1. external type for udtf parameter
    registerFunction("pojoTFunc", new MyPojoTableFunc)
    checkResult(
      "select s from MyTable, LATERAL TABLE(pojoTFunc(a)) as T(s)",
      Seq(row(105), row(11), row(12)))

    //2. external type return in udtf
    registerFunction("pojoFunc", MyPojoFunc)
    registerFunction("toPojoTFunc", new MyToPojoTableFunc)
    checkResult(
      "select b from MyTable, LATERAL TABLE(toPojoTFunc(pojoFunc(a))) as T(b, c)",
      Seq(row(105), row(11), row(12)))
  }

// TODO support dynamic type
//  @Test
//  def testDynamicTypeWithSQL(): Unit = {
//    registerFunction("funcDyna0", new UDTFWithDynamicType0)
//    registerFunction("funcDyna1", new UDTFWithDynamicType0)
//    checkResult(
//      "SELECT c,name,len0,len1,name1,len10 FROM inputT JOIN " +
//        "LATERAL TABLE(funcDyna0(c, 'string,int,int')) AS T1(name,len0,len1) ON TRUE JOIN " +
//        "LATERAL TABLE(funcDyna1(c, 'string,int')) AS T2(name1,len10) ON TRUE " +
//        "where c = 'Anna#44'",
//      Seq(
//        row("Anna#44,44,2,2,44,2"),
//        row("Anna#44,44,2,2,Anna,4"),
//        row("Anna#44,Anna,4,4,44,2"),
//        row("Anna#44,Anna,4,4,Anna,4")
//      ))
//  }
//
//  @Test
//  def testDynamicTypeWithSQLAndVariableArgs(): Unit = {
//    registerFunction("funcDyna0", new UDTFWithDynamicTypeAndVariableArgs)
//    registerFunction("funcDyna1", new UDTFWithDynamicTypeAndVariableArgs)
//    checkResult(
//      "SELECT c,name,len0,len1,name1,len10 FROM inputT JOIN " +
//        "LATERAL TABLE(funcDyna0(c, 'string,int,int', 'a', 'b', 'c')) " +
//        "AS T1(name,len0,len1) ON TRUE JOIN " +
//        "LATERAL TABLE(funcDyna1(c, 'string,int', 'a', 'b', 'c')) AS T2(name1,len10) ON TRUE " +
//        "where c = 'Anna#44'",
//      Seq(
//        row("Anna#44,44,2,2,44,2"),
//        row("Anna#44,44,2,2,Anna,4"),
//        row("Anna#44,Anna,4,4,44,2"),
//        row("Anna#44,Anna,4,4,Anna,4")
//      ))
//  }
//
//  @Test
//  def testDynamicTypeWithSQLAndVariableArgsWithMultiEval(): Unit = {
//    val funcDyna0 = new UDTFWithDynamicTypeAndVariableArgs
//    registerFunction("funcDyna0", funcDyna0)
//    checkResult(
//      "SELECT a, b, c, d, e FROM inputT JOIN " +
//        "LATERAL TABLE(funcDyna0(a)) AS T1(d, e) ON TRUE " +
//        "where c = 'Anna#44'",
//      Seq(
//        row("3,2,Anna#44,3,3"),
//        row("3,2,Anna#44,3,3")
//      ))
//  }
}

@SerialVersionUID(1L)
object StringUdFunc extends ScalarFunction {
  def eval(s: String): String = s
}

object TableFunctionITCase {
  lazy val testData = Seq(
    row(1, 1L, "Jack#22"),
    row(2, 2L, "John#19"),
    row(3, 2L, "Anna#44"),
    row(4, 3L, "nosharp")
  )

  lazy val testDataWithNull = Seq(
    row(1, 1L, "Jack#22"),
    row(2, 2L, null),
    row(3, 2L, ""),
    row(4, 3L, "nosharp")
  )
}

@SerialVersionUID(1L)
class MyPojoTableFunc extends TableFunction[Int] {
  def eval(s: MyPojo): Unit = collect(s.f2)

  override def getParameterTypes(signature: Array[Class[_]]) = {
    val cls = classOf[MyPojo]
    Array[TypeInformation[_]](new PojoTypeInfo[MyPojo](classOf[MyPojo], Seq(
      new PojoField(cls.getDeclaredField("f1"), Types.INT),
      new PojoField(cls.getDeclaredField("f2"), Types.INT))))
  }
}

@SerialVersionUID(1L)
class MyToPojoTableFunc extends TableFunction[MyPojo] {
  def eval(s: Int): Unit = collect(new MyPojo(s, s))

  override def getResultType: TypeInformation[MyPojo] = {
    val cls = classOf[MyPojo]
    new PojoTypeInfo[MyPojo](classOf[MyPojo], Seq(
      new PojoField(cls.getDeclaredField("f1"), Types.INT),
      new PojoField(cls.getDeclaredField("f2"), Types.INT)))
  }
}

@SerialVersionUID(1L)
class GenericTableFunc[T](t: TypeInformation[T]) extends TableFunction[T] {
  def eval(s: Int): Unit = {
    if (t == Types.STRING) {
      collect(s.toString.asInstanceOf[T])
    } else if (t == Types.INT) {
      collect(s.asInstanceOf[T])
    } else {
      throw new RuntimeException
    }
  }

  override def getResultType: TypeInformation[T] = t
}

@SerialVersionUID(1L)
class BinaryStringTableFunc extends TableFunction[Row] {
  def eval(s: StringData, cons: StringData): Unit = collect(Row.of(s, cons))
  override def getResultType: TypeInformation[Row] = {
    new RowTypeInfo(StringDataTypeInfo.INSTANCE, StringDataTypeInfo.INSTANCE)
  }
}
