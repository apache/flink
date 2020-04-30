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

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.{Types, ValidationException}
import org.apache.flink.table.functions.python.{PythonEnv, PythonFunction}
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

  override def getResultType: TypeInformation[Row] = {
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
          val iter = conf.iterator
          while (iter.hasNext) {
            val entry = iter.next()
            val key = entry._1
            val value = entry._2
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

class TableFunc4 extends TableFunction[Row] {
  def eval(b: Byte, s: Short, f: Float): Unit = {
    collect(Row.of("Byte=" + b, "Short=" + s, "Float=" + f))
  }

  override def getResultType: TypeInformation[Row] = {
    new RowTypeInfo(Types.STRING, Types.STRING, Types.STRING)
  }
}

class TableFunc5 extends TableFunction[Row] {
  def eval(row: Row): Unit = {
    collect(row)
  }

  override def getParameterTypes(signature: Array[Class[_]]): Array[TypeInformation[_]] =
    Array(Types.ROW(Types.INT, Types.INT, Types.INT))

  override def getResultType: TypeInformation[Row] =
    Types.ROW(Types.INT, Types.INT, Types.INT)

}

class VarArgsFunc0 extends TableFunction[String] {
  @varargs
  def eval(str: String*): Unit = {
    str.foreach(collect)
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

@SerialVersionUID(1L)
class MockPythonTableFunction extends TableFunction[Row] with PythonFunction {

  def eval(x: Int, y: Int) = ???

  override def getResultType: TypeInformation[Row] =
    new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO)

  override def getSerializedPythonFunction: Array[Byte] = Array[Byte](0)

  override def getPythonEnv: PythonEnv = null
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
