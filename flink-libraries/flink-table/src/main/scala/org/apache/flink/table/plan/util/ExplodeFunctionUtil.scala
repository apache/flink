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
import org.apache.flink.api.java.typeutils.{MultisetTypeInfo, ObjectArrayTypeInfo}
import org.apache.flink.table.functions.TableFunction

class ObjectExplodeTableFunc extends TableFunction[Object] {
  def eval(arr: Array[Object]): Unit = {
    if (null != arr) {
      var i = 0
      while (i < arr.length) {
        collect(arr(i))
        i += 1
      }
    }
  }

  def eval(map: util.Map[Object, Integer]): Unit = {
    if (null != map) {
      val it = map.entrySet().iterator()
      while(it.hasNext) {
        val item = it.next()
        var i = 0
        while (i < item.getValue) {
          collect(item.getKey)
          i += 1
        }
      }
    }
  }
}

class FloatExplodeTableFunc extends TableFunction[Float] {
  def eval(arr: Array[Float]): Unit = {
    if (null != arr) {
      var i = 0
      while (i < arr.length) {
        collect(arr(i))
        i += 1
      }
    }
  }

  def eval(map: util.Map[Float, Integer]): Unit = {
    if (null != map) {
      val it = map.entrySet().iterator()
      while(it.hasNext) {
        val item = it.next()
        var i = 0
        while (i < item.getValue) {
          collect(item.getKey)
          i += 1
        }
      }
    }
  }
}

class ShortExplodeTableFunc extends TableFunction[Short] {
  def eval(arr: Array[Short]): Unit = {
    if (null != arr) {
      var i = 0
      while (i < arr.length) {
        collect(arr(i))
        i += 1
      }
    }
  }

  def eval(map: util.Map[Short, Integer]): Unit = {
    if (null != map) {
      val it = map.entrySet().iterator()
      while(it.hasNext) {
        val item = it.next()
        var i = 0
        while (i < item.getValue) {
          collect(item.getKey)
          i += 1
        }
      }
    }
  }
}
class IntExplodeTableFunc extends TableFunction[Int] {
  def eval(arr: Array[Int]): Unit = {
    if (null != arr) {
      var i = 0
      while (i < arr.length) {
        collect(arr(i))
        i += 1
      }
    }
  }

  def eval(map: util.Map[Int, Integer]): Unit = {
    if (null != map) {
      val it = map.entrySet().iterator()
      while(it.hasNext) {
        val item = it.next()
        var i = 0
        while (i < item.getValue) {
          collect(item.getKey)
          i += 1
        }
      }
    }
  }
}

class LongExplodeTableFunc extends TableFunction[Long] {
  def eval(arr: Array[Long]): Unit = {
    if (null != arr) {
      var i = 0
      while (i < arr.length) {
        collect(arr(i))
        i += 1
      }
    }
  }

  def eval(map: util.Map[Long, Integer]): Unit = {
    if (null != map) {
      val it = map.entrySet().iterator()
      while(it.hasNext) {
        val item = it.next()
        var i = 0
        while (i < item.getValue) {
          collect(item.getKey)
          i += 1
        }
      }
    }
  }
}

class DoubleExplodeTableFunc extends TableFunction[Double] {
  def eval(arr: Array[Double]): Unit = {
    if (null != arr) {
      var i = 0
      while (i < arr.length) {
        collect(arr(i))
        i += 1
      }
    }
  }

  def eval(map: util.Map[Double, Integer]): Unit = {
    if (null != map) {
      val it = map.entrySet().iterator()
      while(it.hasNext) {
        val item = it.next()
        var i = 0
        while (i < item.getValue) {
          collect(item.getKey)
          i += 1
        }
      }
    }
  }
}

class ByteExplodeTableFunc extends TableFunction[Byte] {
  def eval(arr: Array[Byte]): Unit = {
    if (null != arr) {
      var i = 0
      while (i < arr.length) {
        collect(arr(i))
        i += 1
      }
    }
  }

  def eval(map: util.Map[Byte, Integer]): Unit = {
    if (null != map) {
      val it = map.entrySet().iterator()
      while(it.hasNext) {
        val item = it.next()
        var i = 0
        while (i < item.getValue) {
          collect(item.getKey)
          i += 1
        }
      }
    }
  }
}

class BooleanExplodeTableFunc extends TableFunction[Boolean] {
  def eval(arr: Array[Boolean]): Unit = {
    if (null != arr) {
      var i = 0
      while (i < arr.length) {
        collect(arr(i))
        i += 1
      }
    }
  }

  def eval(map: util.Map[Boolean, Integer]): Unit = {
    if (null != map) {
      val it = map.entrySet().iterator()
      while(it.hasNext) {
        val item = it.next()
        var i = 0
        while (i < item.getValue) {
          collect(item.getKey)
          i += 1
        }
      }
    }
  }
}

object ExplodeFunctionUtil {
  def explodeTableFuncFromType(ti: TypeInformation[_]): TableFunction[_] = {
    ti match {
      case pat: PrimitiveArrayTypeInfo[_] => createTableFuncByType(pat.getComponentType)

      case _: ObjectArrayTypeInfo[_, _] => new ObjectExplodeTableFunc

      case _: BasicArrayTypeInfo[_, _] => new ObjectExplodeTableFunc

      case mt: MultisetTypeInfo[_] => createTableFuncByType(mt.getElementTypeInfo)

      case _ => throw new UnsupportedOperationException(ti.toString + " IS NOT supported")
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
