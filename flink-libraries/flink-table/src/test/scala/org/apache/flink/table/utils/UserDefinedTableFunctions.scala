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
import org.apache.flink.types.Row
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.functions.TableFunction


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
