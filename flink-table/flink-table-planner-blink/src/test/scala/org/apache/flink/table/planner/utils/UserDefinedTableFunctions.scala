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
package org.apache.flink.table.planner.utils

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.functions.python.{PythonEnv, PythonFunction}
import org.apache.flink.table.functions.{FunctionContext, ScalarFunction, TableFunction}
import org.apache.flink.types.Row

import org.junit.Assert

import java.lang.Boolean

import scala.annotation.varargs


case class SimpleUser(name: String, age: Int)

@SerialVersionUID(1L)
class TableFunc0 extends TableFunction[SimpleUser] {
  // make sure input element's format is "<string>#<int>"
  def eval(user: String): Unit = {
    if (user.contains("#")) {
      val splits = user.split("#")
      collect(SimpleUser(splits(0), splits(1).toInt))
    }
  }
}

@SerialVersionUID(1L)
class TableFunc1 extends TableFunction[String] {
  def eval(str: String): Unit = {
    if (str.contains("#")){
      str.split("#").foreach(collect)
    }
  }

  def eval(str: String, prefix: String): Unit = {
    if (str.contains("#")) {
      str.split("#").foreach(s => collect(prefix + s))
    }
  }
}

@SerialVersionUID(1L)
class TableFunc2 extends TableFunction[Row] {
  def eval(str: String): Unit = {
    if (str.contains("#")) {
      str.split("#").foreach({ s =>
        val row = new Row(2)
        row.setField(0, s)
        row.setField(1, s.length)
        collect(row)
      })
    }
  }

  override def getResultType(): TypeInformation[Row] =
    new RowTypeInfo(Types.STRING, Types.INT)
}

@SerialVersionUID(1L)
class TableFunc3(data: String, conf: Map[String, String]) extends TableFunction[SimpleUser] {

  def this(data: String) {
    this(data, null)
  }

  def eval(user: String): Unit = {
    if (user.contains("#")) {
      val splits = user.split("#")
      if (null != data) {
        if (null != conf && conf.size > 0) {
          val it = conf.keys.iterator
          while (it.hasNext) {
            val key = it.next()
            val value = conf.get(key).get
            collect(
              SimpleUser(
                data.concat("_key=")
                .concat(key)
                .concat("_value=")
                .concat(value)
                .concat("_")
                .concat(splits(0)),
                splits(1).toInt))
          }
        } else {
          collect(SimpleUser(data.concat(splits(0)), splits(1).toInt))
        }
      } else {
        collect(SimpleUser(splits(0), splits(1).toInt))
      }
    }
  }
}

@SerialVersionUID(1L)
class MockPythonTableFunction extends TableFunction[Row] with PythonFunction {

  def eval(x: Int, y: Int) = ???

  override def getResultType: TypeInformation[Row] =
    new RowTypeInfo(Types.INT, Types.INT)

  override def getSerializedPythonFunction: Array[Byte] = Array[Byte](0)

  override def getPythonEnv: PythonEnv = null
}

//TODO support dynamic type
//class UDTFWithDynamicType extends TableFunction[Row] {
//
//  def eval(str: String, column: Int): Unit = {
//    if (str.contains("#")) {
//      str.split("#").foreach({ s =>
//        val row = new Row(column)
//        row.setField(0, s)
//        var i = 0
//        for (i <- 1 until column) {
//          row.setField(i, s.length)
//        }
//        collect(row)
//      })
//    }
//  }
//
//  override def getResultType(
//      arguments: Array[AnyRef],
//      typeInfos: Array[Class[_]]): TypeInformation[_] = {
//    assert(typeInfos(1).isPrimitive)
//    assert(typeInfos(1).equals(325.getClass))
//    val column = arguments(1).asInstanceOf[Int]
//    val basicTypeInfos = Array.fill[TypeInformation[_]](column)(Types.INT)
//    basicTypeInfos(0) = Types.STRING
//    new RowTypeInfo(basicTypeInfos: _*)
//  }
//}
//
//class UDTFWithDynamicType0 extends TableFunction[Row] {
//
//  def eval(str: String, cols: String): Unit = {
//    val columns = cols.split(",")
//
//    if (str.contains("#")) {
//      str.split("#").foreach({ s =>
//        val row = new Row(columns.length)
//        row.setField(0, s)
//        for (i <- 1 until columns.length) {
//          if (columns(i).equals("string")) {
//            row.setField(i, s.length.toString)
//          } else if (columns(i).equals("int")) {
//            row.setField(i, s.length)
//          }
//        }
//        collect(row)
//      })
//    }
//  }
//
//  override def getResultType(
//      arguments: Array[AnyRef],
//      typeInfos: Array[Class[_]]): TypeInformation[_] = {
//    assert(typeInfos(1).equals(Class.forName("java.lang.String")))
//    val columnStr = arguments(1).asInstanceOf[String]
//    val columns = columnStr.split(",")
//
//    val basicTypeInfos = for (c <- columns) yield c match {
//      case "string" => Types.STRING
//      case "int" => Types.INT
//    }
//    new RowTypeInfo(basicTypeInfos: _*)
//  }
//}
//
//class UDTFWithDynamicType1 extends TableFunction[Row] {
//
//  def eval(col: String): Unit = {
//    val row = new Row(1)
//    col match {
//      case "string" => row.setField(0, "string")
//      case "int" => row.setField(0, 4)
//      case "double" => row.setField(0, 3.25)
//      case "boolean" => row.setField(0, true)
//      case "timestamp" => row.setField(0, new Timestamp(325L))
//    }
//    collect(row)
//  }
//
//  override def getResultType(
//      arguments: Array[AnyRef],
//      typeInfos: Array[Class[_]]): DataType = {
//    assert(typeInfos(0).equals(Class.forName("java.lang.String")))
//    val columnStr = arguments(0).asInstanceOf[String]
//    columnStr match {
//      case "string" => new RowTypeInfo(Types.STRING)
//      case "int" => new RowTypeInfo(Types.INT)
//      case "double" => new RowTypeInfo(Types.DOUBLE)
//      case "boolean" => new RowTypeInfo(Types.BOOLEAN)
//      case "timestamp" => new RowTypeInfo(Types.TIMESTAMP)
//      case _ => new RowTypeInfo(Types.INT)
//    }
//  }
//}
//
//class UDTFWithDynamicTypeAndRexNodes extends TableFunction[Row] {
//
//  def eval(str: String, i: Int, si: Int, bi: Int, flt: Double, real: Double, d: Double,
//      b: Boolean, ts: Timestamp):
//  Unit = {
//    val row = new Row(9)
//    row.setField(0, str)
//    row.setField(1, i)
//    row.setField(2, si)
//    row.setField(3, bi)
//    row.setField(4, flt)
//    row.setField(5, real)
//    row.setField(6, d)
//    row.setField(7, b)
//    row.setField(8, ts)
//    collect(row)
//  }
//
//  override def getResultType(
//      arguments: Array[AnyRef],
//      typeInfos: Array[Class[_]]): DataType = {
//    // Test for the transformRexNodes()
//    // No assertion here, argument 0 is not literal
//    val str = arguments(0).asInstanceOf[String]
//    if (null != str) {
//      throw new RuntimeException("The first column should be null")
//    }
//
//    assert(typeInfos(1).isPrimitive)
//    assert(typeInfos(1).equals(325.getClass))
//    val i = arguments(1).asInstanceOf[Int]
//    if (i <= 0) {
//      throw new RuntimeException("The arguments should be greater than zero")
//    }
//
//    assert(typeInfos(2).isPrimitive)
//    assert(typeInfos(2).equals(325.getClass))
//    val si = arguments(2).asInstanceOf[Int]
//    if (si <= 0) {
//      throw new RuntimeException("The arguments should be greater than zero")
//    }
//
//    assert(typeInfos(3).isPrimitive)
//    assert(typeInfos(3).equals(325.getClass))
//    val bi = arguments(3).asInstanceOf[Int]
//    if (bi <= 0) {
//      throw new RuntimeException("The arguments should be greater than zero")
//    }
//
//    assert(typeInfos(4).isPrimitive)
//    assert(typeInfos(4).equals(3.25.getClass))
//    val float = arguments(4).asInstanceOf[Double]
//    if (float <= 0) {
//      throw new RuntimeException("The arguments should be greater than zero")
//    }
//
//    assert(typeInfos(5).isPrimitive)
//    assert(typeInfos(5).equals(3.25.getClass))
//    val real = arguments(5).asInstanceOf[Double]
//    if (real <= 0) {
//      throw new RuntimeException("The arguments should be greater than zero")
//    }
//
//    assert(typeInfos(6).isPrimitive)
//    assert(typeInfos(6).equals(3.25.getClass))
//    val d = arguments(6).asInstanceOf[Double]
//    if (d <= 0) {
//      throw new RuntimeException("The arguments should be greater than zero")
//    }
//
//    assert(typeInfos(7).equals(Class.forName("java.lang.Boolean")))
//    val b = arguments(7).asInstanceOf[Boolean]
//    if (!b) {
//      throw new RuntimeException("The arguments should be true")
//    }
//
//    assert(typeInfos(8).equals(Class.forName("java.sql.Timestamp")))
//    val ts = arguments(8).asInstanceOf[Timestamp]
//    if (ts.getTime <= 0) {
//      throw new RuntimeException("The arguments should be greater than zero")
//    }
//
//    new RowTypeInfo(
//      Types.STRING,
//      Types.INT,
//      Types.INT,
//      Types.INT,
//      Types.DOUBLE,
//      Types.DOUBLE,
//      Types.DOUBLE,
//      Types.BOOLEAN,
//      Types.TIMESTAMP
//    )
//  }
//}
//
//class UDTFWithDynamicTypeAndVariableArgs extends TableFunction[Row] {
//
//  def eval(value: Int): Unit = {
//    val v = new Integer(value)
//    collect(Row.of(v, v))
//    collect(Row.of(v, v))
//  }
//
//  @varargs
//  def eval(str: String, cols: String, fields: AnyRef*): Unit = {
//    val columns = cols.split(",")
//
//    if (str.contains("#")) {
//      str.split("#").foreach({ s =>
//        val row = new Row(columns.length)
//        row.setField(0, s)
//        for (i <- 1 until columns.length) {
//          if (columns(i).equals("string")) {
//            row.setField(i, s.length.toString)
//          } else if (columns(i).equals("int")) {
//            row.setField(i, s.length)
//          }
//        }
//        collect(row)
//      })
//    }
//  }
//
//  override def getResultType(
//      arguments: Array[AnyRef],
//      typeInfos: Array[Class[_]]): DataType = {
//    if (typeInfos.length == 1) {
//      new RowTypeInfo(Types.INT, Types.INT)
//    } else {
//      assert(typeInfos(1).equals(Class.forName("java.lang.String")))
//      val columnStr = arguments(1).asInstanceOf[String]
//      val columns = columnStr.split(",")
//
//      val basicTypeInfos = for (c <- columns) yield c match {
//        case "string" => Types.STRING
//        case "int" => Types.INT
//      }
//      new RowTypeInfo(basicTypeInfos: _*)
//    }
//  }
//}

@SerialVersionUID(1L)
class TableFunc4 extends TableFunction[Row] {
  def eval(b: Byte, s: Short, f: Float): Unit = {
    collect(Row.of("Byte=" + b, "Short=" + s, "Float=" + f))
  }

  override def getResultType: TypeInformation[Row] = {
    new RowTypeInfo(Types.STRING, Types.STRING, Types.STRING)
  }
}

@SerialVersionUID(1L)
class TableFunc6 extends TableFunction[Row] {
  def eval(row: Row): Unit = {
    collect(row)
  }

  override def getParameterTypes(signature: Array[Class[_]]): Array[TypeInformation[_]] =
    Array(new RowTypeInfo(Types.INT, Types.INT, Types.INT))

  override def getResultType: TypeInformation[Row] = {
    new RowTypeInfo(Types.INT, Types.INT, Types.INT)
  }
}

@SerialVersionUID(1L)
class TableFunc7 extends TableFunction[Row] {

  def eval(row: Row): Unit = {
  }

  def eval(row: java.util.List[Row]): Unit = {
  }
}

@SerialVersionUID(1L)
class RF extends ScalarFunction {

  def eval(x: Int): java.util.List[Row] = {
    java.util.Collections.emptyList()
  }
}

@SerialVersionUID(1L)
class VarArgsFunc0 extends TableFunction[String] {
  @varargs
  def eval(str: String*): Unit = {
    str.foreach(collect)
  }
}

@SerialVersionUID(1L)
class HierarchyTableFunction extends SplittableTableFunction[Boolean, Integer] {
  def eval(user: String) {
    if (user.contains("#")) {
      val splits = user.split("#")
      val age = splits(1).toInt
      collect(new Tuple3[String, Boolean, Integer](splits(0), age >= 20, age))
    }
  }
}

abstract class SplittableTableFunction[A, B] extends TableFunction[Tuple3[String, A, B]] {}

@SerialVersionUID(1L)
class PojoTableFunc extends TableFunction[PojoUser] {
  def eval(user: String) {
    if (user.contains("#")) {
      val splits = user.split("#")
      collect(new PojoUser(splits(0), splits(1).toInt))
    }
  }
}

class PojoUser() {
  var name: String = _
  var age: Int = 0

  def this(name: String, age: Int) {
    this()
    this.name = name
    this.age = age
  }
}

// ----------------------------------------------------------------------------------------------
// Invalid Table Functions
// ----------------------------------------------------------------------------------------------


// this is used to check whether scala object is forbidden
@SerialVersionUID(1L)
object ObjectTableFunction extends TableFunction[Integer] {
  def eval(a: Int, b: Int): Unit = {
    collect(a)
    collect(b)
  }
}

@SerialVersionUID(1L)
class RichTableFunc0 extends TableFunction[String] {
  var openCalled = false
  var closeCalled = false

  override def open(context: FunctionContext): Unit = {
    super.open(context)
    if (closeCalled) {
      Assert.fail("Close called before open.")
    }
    openCalled = true
  }

  def eval(str: String): Unit = {
    if (!openCalled) {
      Assert.fail("Open was not called before eval.")
    }
    if (closeCalled) {
      Assert.fail("Close called before eval.")
    }

    if (!str.contains("#")) {
      collect(str)
    }
  }

  override def close(): Unit = {
    super.close()
    if (!openCalled) {
      Assert.fail("Open was not called before close.")
    }
    closeCalled = true
  }
}

@SerialVersionUID(1L)
class RichTableFunc1 extends TableFunction[String] {
  var separator: Option[String] = None

  override def open(context: FunctionContext): Unit = {
    separator = Some(context.getJobParameter("word_separator", ""))
  }

  def eval(str: String): Unit = {
    if (str.contains(separator.getOrElse(throw new ValidationException(s"no separator")))) {
      str.split(separator.get).foreach(collect)
    }
  }

  override def close(): Unit = {
    separator = None
  }
}
