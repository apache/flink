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

package org.apache.flink.table.runtime.batch.sql

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo
import org.apache.flink.api.java.typeutils.{RowTypeInfo, TypeExtractor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.functions.{ScalarFunction, TableFunction}
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.dataformat.BinaryString
import org.apache.flink.table.expressions.utils.{Func1, Func18, RichFunc2}
import org.apache.flink.table.plan.batch.sql.StringSplit
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.row
import org.apache.flink.table.runtime.batch.sql.TestData._
import org.apache.flink.table.runtime.utils.JavaUserDefinedTableFunctions.JavaTableFunc0
import org.apache.flink.table.util.DateTimeTestUtil._
import org.apache.flink.table.util._
import org.apache.flink.types.Row
import org.junit.{Before, Test}

import scala.collection.Seq

class TableFunctionITCase extends BatchTestBase {

  @Before
  def before(): Unit = {
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)
    registerCollection("inputT", TableFunctionITCase.testData, type3, 'a, 'b, 'c)
    registerCollection("inputTWithNull", TableFunctionITCase.testDataWithNull, type3, 'a, 'b, 'c)
    registerCollection("SmallTable3", smallData3, type3, 'a, 'b, 'c)
  }

  @Test
  def testTableFunction(): Unit = {
    tEnv.registerFunction("func", new TableFunc1)
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
    tEnv.registerFunction("func", new TableFunc2)
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
    tEnv.registerFunction("func", new TableFunc0)
    checkResult(
      "select c, name, age from inputT, LATERAL TABLE(func(c)) as T(name, age) WHERE T.age > 20",
      Seq(
        row("Jack#22", "Jack", 22),
        row("Anna#44", "Anna", 44)))
  }

  @Test
  def testHierarchyType(): Unit = {
    tEnv.registerFunction("func", new HierarchyTableFunction)
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
    tEnv.registerFunction("func", new PojoTableFunc)
    checkResult(
      "select c, name, age from inputT, LATERAL TABLE(func(c)) as T(age, name) WHERE T.age > 20",
      Seq(
        row("Jack#22", "Jack", 22),
        row("Anna#44", "Anna", 44)))
  }

  @Test
  def testUserDefinedTableFunctionWithScalarFunction(): Unit = {
    tEnv.registerFunction("func", new TableFunc1)
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
    tEnv.registerFunction("func", new TableFunc0)
    tEnv.registerFunction("func18", Func18)
    tEnv.registerFunction("func1", Func1)
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
      row(UTCDate("1990-10-14"), 1000L, UTCTimestamp("1990-10-14 12:10:10"))),
      new RowTypeInfo(SqlTimeTypeInfo.DATE, LONG_TYPE_INFO, SqlTimeTypeInfo.TIMESTAMP),
      'x, 'y, 'z)
    tEnv.registerFunction("func", new JavaTableFunc0)
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
    tEnv.registerFunction("func", new RichTableFunc1)
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
    tEnv.registerFunction("func", new RichTableFunc1)
    tEnv.registerFunction("func2", new RichFunc2)
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
    tEnv.registerFunction("func30", new TableFunc3(null))
    tEnv.registerFunction("func31", new TableFunc3("OneConf_"))
    tEnv.registerFunction("func32", new TableFunc3("TwoConf_"))
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
    tEnv.registerFunction("func", new VarArgsFunc0)
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
    tEnv.registerCollection("MyTable", data,
      new RowTypeInfo(TypeExtractor.createTypeInfo(classOf[MyPojo])), 'a)

    //1. external type for udtf parameter
    tEnv.registerFunction("pojoTFunc", new MyPojoTableFunc)
    checkResult(
      "select s from MyTable, LATERAL TABLE(pojoTFunc(a)) as T(s)",
      Seq(row(105), row(11), row(12)))

    //2. external type return in udtf
    tEnv.registerFunction("pojoFunc", MyPojoFunc)
    tEnv.registerFunction("toPojoTFunc", new MyToPojoTableFunc)
    checkResult(
      "select b from MyTable, LATERAL TABLE(toPojoTFunc(pojoFunc(a))) as T(b, c)",
      Seq(row(105), row(11), row(12)))
  }

  @Test
  def testDynamicTypeWithSQL(): Unit = {
    tEnv.registerFunction("funcDyna0", new UDTFWithDynamicType0)
    tEnv.registerFunction("funcDyna1", new UDTFWithDynamicType0)
    checkResult(
      "SELECT c,name,len0,len1,name1,len10 FROM inputT JOIN " +
        "LATERAL TABLE(funcDyna0(c, 'string,int,int')) AS T1(name,len0,len1) ON TRUE JOIN " +
        "LATERAL TABLE(funcDyna1(c, 'string,int')) AS T2(name1,len10) ON TRUE " +
        "where c = 'Anna#44'",
      Seq(
        row("Anna#44,44,2,2,44,2"),
        row("Anna#44,44,2,2,Anna,4"),
        row("Anna#44,Anna,4,4,44,2"),
        row("Anna#44,Anna,4,4,Anna,4")
      ))
  }

  @Test
  def testDynamicTypeWithSQLAndVariableArgs(): Unit = {
    tEnv.registerFunction("funcDyna0", new UDTFWithDynamicTypeAndVariableArgs)
    tEnv.registerFunction("funcDyna1", new UDTFWithDynamicTypeAndVariableArgs)
    checkResult(
      "SELECT c,name,len0,len1,name1,len10 FROM inputT JOIN " +
        "LATERAL TABLE(funcDyna0(c, 'string,int,int', 'a', 'b', 'c')) " +
        "AS T1(name,len0,len1) ON TRUE JOIN " +
        "LATERAL TABLE(funcDyna1(c, 'string,int', 'a', 'b', 'c')) AS T2(name1,len10) ON TRUE " +
        "where c = 'Anna#44'",
      Seq(
        row("Anna#44,44,2,2,44,2"),
        row("Anna#44,44,2,2,Anna,4"),
        row("Anna#44,Anna,4,4,44,2"),
        row("Anna#44,Anna,4,4,Anna,4")
      ))
  }

  @Test
  def testDynamicTypeWithSQLAndVariableArgsWithMultiEval(): Unit = {
    val funcDyna0 = new UDTFWithDynamicTypeAndVariableArgs
    tEnv.registerFunction("funcDyna0", funcDyna0)
    checkResult(
      "SELECT a, b, c, d, e FROM inputT JOIN " +
        "LATERAL TABLE(funcDyna0(a)) AS T1(d, e) ON TRUE " +
        "where c = 'Anna#44'",
      Seq(
        row("3,2,Anna#44,3,3"),
        row("3,2,Anna#44,3,3")
      ))
  }

  @Test
  def testJavaGenericTableFunc(): Unit = {
    tEnv.registerFunction("func0", new GenericTableFunc[Int](DataTypes.INT))
    tEnv.registerFunction("func1", new GenericTableFunc[String](DataTypes.STRING))
    testGenericTableFunc()
  }

  @Test
  def testScalaGenericTableFunc(): Unit = {
    tEnv.registerFunction("func0", new GenericTableFunc[Int](DataTypes.INT))
    tEnv.registerFunction("func1", new GenericTableFunc[String](DataTypes.STRING))
    testGenericTableFunc()
  }

  def testGenericTableFunc(): Unit = {
    checkResult(
      "select a, s from inputT, LATERAL TABLE(func0(a)) as T(s)",
      Seq(
        row(1, 1),
        row(2, 2),
        row(3, 3),
        row(4, 4)
      ))

    checkResult(
      "select a, s from inputT, LATERAL TABLE(func1(a)) as T(s)",
      Seq(
        row(1, 1),
        row(2, 2),
        row(3, 3),
        row(4, 4)
      ))
  }

  @Test
  def testConstantTableFunc(): Unit = {
    tEnv.registerFunction("str_split", new StringSplit())
    tEnv.registerFunction("funcDyn1", new UDTFWithDynamicType0)
    tEnv.registerFunction("funcDyn2", new UDTFWithDynamicType0)

    checkResult(
      "SELECT * FROM LATERAL TABLE(str_split()) as T0(d)",
      Seq(row("a"), row("b"), row("c"))
    )

    checkResult(
      "SELECT * FROM LATERAL TABLE(str_split('Jack,John', ',')) as T0(d)",
      Seq(row("Jack"), row("John"))
    )

    checkResult(
      "SELECT * FROM " +
        "LATERAL TABLE(str_split(SUBSTRING('a,b,c', 2, 4), ',')) as T1(s), " +
        "LATERAL TABLE(str_split('a,b,c', ',')) as T2(x)",
      Seq(row("b", "a"), row("b", "b"), row("b", "c"),
        row("c", "a"), row("c", "b"), row("c", "c"))
    )

    checkResult(
      "SELECT * FROM " +
        "LATERAL TABLE(funcDyn1('test#Hello world#Hi', 'string,int,int')) AS T1(name0,len0,len1)," +
        "LATERAL TABLE(funcDyn2('abc#defijk', 'string,int')) AS T2(name1,len2)" +
        "WHERE len0 < 5 AND len2 < 4",
      Seq(row("Hi", 2, 2, "abc", 3), row("test", 4, 4, "abc", 3))
    )

    checkResult(
      "SELECT c, d FROM inputT, LATERAL TABLE(str_split()) AS T0(d)",
      Seq(
        row("Jack#22", "a"), row("Jack#22", "b"), row("Jack#22", "c"),
        row("John#19", "a"), row("John#19", "b"), row("John#19", "c"),
        row("Anna#44", "a"), row("Anna#44", "b"), row("Anna#44", "c"),
        row("nosharp", "a"), row("nosharp", "b"), row("nosharp", "c")
      ))

    checkResult(
      "SELECT c, d FROM inputT, LATERAL TABLE(str_split('Jack,John', ',')) AS T0(d)",
      Seq(
        row("Jack#22", "Jack"), row("Jack#22", "John"),
        row("John#19", "Jack"), row("John#19", "John"),
        row("Anna#44", "Jack"), row("Anna#44", "John"),
        row("nosharp", "Jack"), row("nosharp", "John")
      ))

    checkResult(
      "SELECT c, d FROM inputT, LATERAL TABLE(str_split('Jack,John', ',')) AS T0(d) " +
        "WHERE d = 'Jack'",
      Seq(
        row("Jack#22", "Jack"),
        row("John#19", "Jack"),
        row("Anna#44", "Jack"),
        row("nosharp", "Jack")
      ))

    checkResult(
      "SELECT c, d FROM inputT, LATERAL TABLE(str_split('Jack,John', ',', 1)) AS T0(d) " +
        "WHERE SUBSTRING(c, 1, 4) = d",
      Seq(row("John#19", "John"))
    )

    checkResult(
      "SELECT c, name0, len0, len1, name1, len2 FROM " +
        "LATERAL TABLE(funcDyn1('test#Hello world#Hi', 'string,int,int')) AS T1(name0,len0,len1)," +
        "LATERAL TABLE(funcDyn2('abc#defijk', 'string,int')) AS T2(name1,len2), " +
        "inputT WHERE a > 2 AND len1 < 5 AND len2 < 4",
      Seq(row("Anna#44", "Hi", 2, 2, "abc", 3),
        row("Anna#44", "test", 4, 4, "abc", 3),
        row("nosharp", "Hi", 2, 2, "abc", 3),
        row("nosharp", "test", 4, 4, "abc", 3)
      )
    )
  }

  /**
    * Test binaryString => string => binaryString => string => binaryString.
    */
  @Test
  def testUdfAfterUdtf(): Unit = {

    tEnv.registerFunction("str_split", new StringSplit())
    tEnv.registerFunction("func", StringUdFunc)

    checkResult(
      "select func(s) from inputT, LATERAL TABLE(str_split(c, '#')) as T(s)",
      Seq(row("Anna"), row("Jack"), row("John"), row("nosharp"),
        row("19"), row("22"), row("44")))
  }

  @Test
  def testLeftInputAllProjectWithEmptyOutput(): Unit = {

    tEnv.registerFunction("str_split", new StringSplit())

    checkResult(
      "select s from inputTWithNull, LATERAL TABLE(str_split(c, '#')) as T(s)",
      Seq(row("Jack"), row("nosharp"), row("22")))
  }

  @Test
  def testLeftJoinLeftInputAllProjectWithEmptyOutput(): Unit = {

    tEnv.registerFunction("str_split", new StringSplit())

    checkResult(
      "select s from inputTWithNull left join LATERAL TABLE(str_split(c, '#')) as T(s) ON TRUE",
      Seq(row("Jack"), row("nosharp"), row("22"), row(null), row(null)))
  }

  @Test
  def testLeftInputPartialProjectWithEmptyOutput(): Unit = {

    tEnv.registerFunction("str_split", new StringSplit())

    checkResult(
      "select a, s from inputTWithNull, LATERAL TABLE(str_split(c, '#')) as T(s)",
      Seq(row(1, "Jack"), row(4, "nosharp"), row(1, "22")))
  }

  @Test
  def testLeftJoinLeftInputPartialProjectWithEmptyOutput(): Unit = {

    tEnv.registerFunction("str_split", new StringSplit())

    checkResult(
      "select b, s from inputTWithNull left join LATERAL TABLE(str_split(c, '#')) as T(s) ON TRUE",
      Seq(row(1, "Jack"), row(3, "nosharp"), row(1, "22"), row(2, null), row(2, null)))
  }

  @Test
  def testLeftInputPartialProjectWithEmptyOutput2(): Unit = {

    tEnv.registerFunction("toPojo", new MyToPojoTableFunc())
    tEnv.registerFunction("func", StringUdFunc)

    checkResult(
      "select a, s2 from inputTWithNull, LATERAL TABLE(toPojo(a)) as T(s1,s2)" +
        " where a + s1 > 2",
      Seq(row(2, 2), row(3, 3), row(4, 4)))
  }

  @Test
  def testLeftJoinLeftInputPartialProjectWithEmptyOutput2(): Unit = {
    tEnv.registerFunction("toPojo", new MyToPojoTableFunc())
    tEnv.registerFunction("func", StringUdFunc)

    checkResult(
      "select b, s1 from inputTWithNull left join LATERAL TABLE(toPojo(a)) as T(s1,s2) ON TRUE" +
        " where a + s1 > 2",
      Seq(row(2, 2), row(2, 3), row(3, 4)))
  }

  @Test
  def testTableFunctionWithBinaryString(): Unit = {
    tEnv.registerFunction("func", new BinaryStringTableFunc)
    checkResult(
      "select c, s1, s2 from inputT, LATERAL TABLE(func(c, 'haha')) as T(s1, s2)",
      Seq(
        row("Jack#22", "Jack#22", "haha"),
        row("John#19", "John#19", "haha"),
        row("nosharp", "nosharp", "haha"),
        row("Anna#44", "Anna#44", "haha")
      ))
  }

}

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

class MyPojoTableFunc extends TableFunction[Int] {
  def eval(s: MyPojo): Unit = collect(s.f2)

  override def getParameterTypes(signature: Array[Class[_]]) =
    Array(DataTypes.pojoBuilder(classOf[MyPojo])
      .field("f1", DataTypes.INT)
      .field("f2", DataTypes.INT)
      .build())
}

class MyToPojoTableFunc extends TableFunction[MyPojo] {
  def eval(s: Int): Unit = collect(new MyPojo(s, s))

  override def getResultType(arguments: Array[AnyRef], argTypes: Array[Class[_]]): DataType =
    DataTypes.pojoBuilder(classOf[MyPojo])
      .field("f1", DataTypes.INT)
      .field("f2", DataTypes.INT)
      .build()
}

class GenericTableFunc[T](t: DataType) extends TableFunction[T] {
  def eval(s: Int): Unit = {
    if (t == DataTypes.STRING) {
      collect(s.toString.asInstanceOf[T])
    } else if (t == DataTypes.INT) {
      collect(s.asInstanceOf[T])
    } else {
      throw new RuntimeException
    }
  }

  override def getResultType(arguments: Array[AnyRef], argTypes: Array[Class[_]]): DataType = t
}

class BinaryStringTableFunc extends TableFunction[Row] {
  def eval(s: BinaryString, cons: BinaryString): Unit = collect(Row.of(s, cons))
  override def getResultType(arguments: Array[AnyRef], argTypes: Array[Class[_]]): DataType = {
    arguments(1).asInstanceOf[String].toLowerCase
    DataTypes.createRowType(DataTypes.STRING, DataTypes.STRING)
  }
}
