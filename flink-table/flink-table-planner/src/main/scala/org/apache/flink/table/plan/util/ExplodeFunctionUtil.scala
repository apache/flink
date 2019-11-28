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

package org.apache.flink.table.plan.util

import java.util

import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, BasicTypeInfo, PrimitiveArrayTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.{MapTypeInfo, MultisetTypeInfo, ObjectArrayTypeInfo}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

import scala.collection.JavaConverters._

abstract class ExplodeTableFunction[T] extends TableFunction[T] {

  def collectArray(array: Array[T]): Unit = {
    if (null != array) {
      var i = 0
      while (i < array.length) {
        collect(array(i))
        i += 1
      }
    }
  }

  def collect(map: util.Map[T, Integer]): Unit = {
    if (null != map) {
      val it = map.entrySet().iterator()
      while (it.hasNext) {
        val item = it.next()
        val key: T = item.getKey
        val cnt: Int = item.getValue
        var i = 0
        while (i < cnt) {
          collect(key)
          i += 1
        }
      }
    }
  }
}

class MapExplodeTableFunc extends TableFunction[Row] {
  def eval(map: util.Map[Object, Object]): Unit = {
    map.asScala.foreach { case (key,value) =>
      collect(Row.of(key, value))
    }
  }
}

class ObjectExplodeTableFunc extends ExplodeTableFunction[Object] {
  def eval(arr: Array[Object]): Unit = {
    collectArray(arr)
  }

  def eval(map: util.Map[Object, Integer]): Unit = {
    collect(map)
  }
}

class FloatExplodeTableFunc extends ExplodeTableFunction[Float] {
  def eval(arr: Array[Float]): Unit = {
    collectArray(arr)
  }

  def eval(map: util.Map[Float, Integer]): Unit = {
    collect(map)
  }
}

class ShortExplodeTableFunc extends ExplodeTableFunction[Short] {
  def eval(arr: Array[Short]): Unit = {
    collectArray(arr)
  }

  def eval(map: util.Map[Short, Integer]): Unit = {
    collect(map)
  }
}
class IntExplodeTableFunc extends ExplodeTableFunction[Int] {
  def eval(arr: Array[Int]): Unit = {
    collectArray(arr)
  }

  def eval(map: util.Map[Int, Integer]): Unit = {
    collect(map)
  }
}

class LongExplodeTableFunc extends ExplodeTableFunction[Long] {
  def eval(arr: Array[Long]): Unit = {
    collectArray(arr)
  }

  def eval(map: util.Map[Long, Integer]): Unit = {
    collect(map)
  }
}

class DoubleExplodeTableFunc extends ExplodeTableFunction[Double] {
  def eval(arr: Array[Double]): Unit = {
    collectArray(arr)
  }

  def eval(map: util.Map[Double, Integer]): Unit = {
    collect(map)
  }
}

class ByteExplodeTableFunc extends ExplodeTableFunction[Byte] {
  def eval(arr: Array[Byte]): Unit = {
    collectArray(arr)
  }

  def eval(map: util.Map[Byte, Integer]): Unit = {
    collect(map)
  }
}

class BooleanExplodeTableFunc extends ExplodeTableFunction[Boolean] {
  def eval(arr: Array[Boolean]): Unit = {
    collectArray(arr)
  }

  def eval(map: util.Map[Boolean, Integer]): Unit = {
    collect(map)
  }
}

object ExplodeFunctionUtil {
  def explodeTableFuncFromType(ti: TypeInformation[_]): TableFunction[_] = {
    ti match {
      case pat: PrimitiveArrayTypeInfo[_] => createTableFuncByType(pat.getComponentType)

      case _: ObjectArrayTypeInfo[_, _] => new ObjectExplodeTableFunc

      case _: BasicArrayTypeInfo[_, _] => new ObjectExplodeTableFunc

      case mt: MultisetTypeInfo[_] => createTableFuncByType(mt.getElementTypeInfo)

      case mt: MapTypeInfo[_,_] => new MapExplodeTableFunc

      case _ => throw new TableException("Unnesting of '" + ti.toString + "' is not supported.")
    }
  }

  def createTableFuncByType(typeInfo: TypeInformation[_]): TableFunction[_] = {
    typeInfo match {
      case BasicTypeInfo.INT_TYPE_INFO => new IntExplodeTableFunc
      case BasicTypeInfo.LONG_TYPE_INFO => new LongExplodeTableFunc
      case BasicTypeInfo.SHORT_TYPE_INFO => new ShortExplodeTableFunc
      case BasicTypeInfo.FLOAT_TYPE_INFO => new FloatExplodeTableFunc
      case BasicTypeInfo.DOUBLE_TYPE_INFO => new DoubleExplodeTableFunc
      case BasicTypeInfo.BYTE_TYPE_INFO => new ByteExplodeTableFunc
      case BasicTypeInfo.BOOLEAN_TYPE_INFO => new BooleanExplodeTableFunc
      case _ => new ObjectExplodeTableFunc
    }
  }
}
