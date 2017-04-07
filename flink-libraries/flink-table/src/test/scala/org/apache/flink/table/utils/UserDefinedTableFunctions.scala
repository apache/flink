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
package org.apache.flink.table.utils

import java.lang.Boolean
import java.sql.Timestamp
import java.util

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, IntegerTypeInfo, SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.functions.{FunctionContext, TableFunction}
import org.apache.flink.types.Row
import org.junit.Assert

import scala.annotation.varargs


case class SimpleUser(name: String, age: Int)

class TableFunc0 extends TableFunction[SimpleUser] {
  // make sure input element's format is "<string>#<int>"
  def eval(user: String): Unit = {
    if (user.contains("#")) {
      val splits = user.split("#")
      collect(SimpleUser(splits(0), splits(1).toInt))
    }
  }
}

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

  override def getResultType(
      arguments: java.util.List[AnyRef],
      typeInfos: java.util.List[TypeInformation[_]]): TypeInformation[Row] = {
    new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO)
  }
}

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

class DynamicSchema extends TableFunction[Row] {

  def eval(str: String, column: Int): Unit = {
    if (str.contains("#")) {
      str.split("#").foreach({ s =>
        val row = new Row(column)
        row.setField(0, s)
        var i = 0
        for (i <- 1 until column) {
          row.setField(i, s.length)
        }
        collect(row)
      })
    }
  }

  override def getResultType(
      arguments: java.util.List[AnyRef],
      typeInfos: java.util.List[TypeInformation[_]]): TypeInformation[Row] = {
    if (!typeInfos.get(1).isInstanceOf[IntegerTypeInfo[_]]) {
      throw new RuntimeException("The second parameter should be integer")
    }
    val column = arguments.get(1).asInstanceOf[Int]
    val basicTypeInfos = Array.fill[TypeInformation[_]](column)(BasicTypeInfo.INT_TYPE_INFO)
    basicTypeInfos(0) = BasicTypeInfo.STRING_TYPE_INFO
    new RowTypeInfo(basicTypeInfos: _*)
  }
}

class DynamicSchema0 extends TableFunction[Row] {

  def eval(str: String, cols: String): Unit = {
    val columns = cols.split(",")

    if (str.contains("#")) {
      str.split("#").foreach({ s =>
        val row = new Row(columns.length)
        row.setField(0, s)
        for (i <- 1 until columns.length) {
          if (columns(i).equals("string")) {
            row.setField(i, s.length.toString)
          } else if (columns(i).equals("int")) {
            row.setField(i, s.length)
          }
        }
        collect(row)
      })
    }
  }

  override def getResultType(
      arguments: java.util.List[AnyRef],
      typeInfos: java.util.List[TypeInformation[_]]): TypeInformation[Row] = {
    val columnStr = arguments.get(1).asInstanceOf[String]
    val columns = columnStr.split(",")

    val basicTypeInfos = for (c <- columns) yield c match {
      case "string" => BasicTypeInfo.STRING_TYPE_INFO.asInstanceOf[TypeInformation[_]]
      case "int" => BasicTypeInfo.INT_TYPE_INFO.asInstanceOf[TypeInformation[_]]
    }
    new RowTypeInfo(basicTypeInfos: _*)
  }
}

class DynamicSchemaWithRexNodes extends TableFunction[Row] {

  def eval(str: String, i: Int, si: Int, bi: Int, flt: Double, real: Double, d: Double,
           b: Boolean, ts: Timestamp):
  Unit = {
    val row = new Row(9)
    row.setField(0, str)
    row.setField(1, i)
    row.setField(2, si)
    row.setField(3, bi)
    row.setField(4, flt)
    row.setField(5, real)
    row.setField(6, d)
    row.setField(7, b)
    row.setField(8, ts)
    collect(row)
  }

  override def getResultType(
      arguments: util.List[AnyRef],
      typeInfos: util.List[TypeInformation[_]]): TypeInformation[Row] = {
    // Test for the transformRexNodes()
    val str = arguments.get(0).asInstanceOf[String]
    if (null != str) {
      throw new RuntimeException("The first column should be null")
    }
    val i = arguments.get(1).asInstanceOf[Int]
    if (i <= 0) {
      throw new RuntimeException("The arguments should be greater than zero")
    }
    val si = arguments.get(2).asInstanceOf[Int]
    if (si <= 0) {
      throw new RuntimeException("The arguments should be greater than zero")
    }
    val bi = arguments.get(3).asInstanceOf[Int]
    if (bi <= 0) {
      throw new RuntimeException("The arguments should be greater than zero")
    }
    val float = arguments.get(4).asInstanceOf[Double]
    if (float <= 0) {
      throw new RuntimeException("The arguments should be greater than zero")
    }
    val real = arguments.get(5).asInstanceOf[Double]
    if (real <= 0) {
      throw new RuntimeException("The arguments should be greater than zero")
    }
    val d = arguments.get(6).asInstanceOf[Double]
    if (d <= 0) {
      throw new RuntimeException("The arguments should be greater than zero")
    }
    val b = arguments.get(7).asInstanceOf[Boolean]
    if (!b) {
      throw new RuntimeException("The arguments should be true")
    }
    val ts = arguments.get(8).asInstanceOf[Timestamp]
    if (ts.getTime <= 0) {
      throw new RuntimeException("The arguments should be greater than zero")
    }

    new RowTypeInfo(
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO,
      BasicTypeInfo.BOOLEAN_TYPE_INFO,
      SqlTimeTypeInfo.TIMESTAMP
    )
  }
}

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
object ObjectTableFunction extends TableFunction[Integer] {
  def eval(a: Int, b: Int): Unit = {
    collect(a)
    collect(b)
  }
}

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

class VarArgsFunc0 extends TableFunction[String] {
  @varargs
  def eval(str: String*): Unit = {
    str.foreach(collect)
  }
}
