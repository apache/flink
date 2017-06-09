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

import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, BasicTypeInfo, PrimitiveArrayTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo
import org.apache.flink.table.functions.TableFunction

class ObjectExplodeTableFunc extends TableFunction[Object] {
  def eval(arr: Array[Object]): Unit = {
    arr.foreach(collect)
  }
}

class FloatExplodeTableFunc extends TableFunction[Float] {
  def eval(arr: Array[Float]): Unit = {
    arr.foreach(collect)
  }
}

class ShortExplodeTableFunc extends TableFunction[Short] {
  def eval(arr: Array[Short]): Unit = {
    arr.foreach(collect)
  }
}
class IntExplodeTableFunc extends TableFunction[Int] {
  def eval(arr: Array[Int]): Unit = {
    arr.foreach(collect)
  }
}

class LongExplodeTableFunc extends TableFunction[Long] {
  def eval(arr: Array[Long]): Unit = {
    arr.foreach(collect)
  }
}

class DoubleExplodeTableFunc extends TableFunction[Double] {
  def eval(arr: Array[Double]): Unit = {
    arr.foreach(collect)
  }
}

class ByteExplodeTableFunc extends TableFunction[Byte] {
  def eval(arr: Array[Byte]): Unit = {
    arr.foreach(collect)
  }
}

class BooleanExplodeTableFunc extends TableFunction[Boolean] {
  def eval(arr: Array[Boolean]): Unit = {
    arr.foreach(collect)
  }
}

object ExplodeFunctionUtil {
  def explodeTableFuncFromType(ti: TypeInformation[_]):TableFunction[_] = {
    ti match {
      case pat: PrimitiveArrayTypeInfo[_] => {
        pat.getComponentType match {
          case BasicTypeInfo.INT_TYPE_INFO => new IntExplodeTableFunc
          case BasicTypeInfo.LONG_TYPE_INFO => new LongExplodeTableFunc
          case BasicTypeInfo.SHORT_TYPE_INFO => new ShortExplodeTableFunc
          case BasicTypeInfo.FLOAT_TYPE_INFO => new FloatExplodeTableFunc
          case BasicTypeInfo.DOUBLE_TYPE_INFO => new DoubleExplodeTableFunc
          case BasicTypeInfo.BYTE_TYPE_INFO => new ByteExplodeTableFunc
          case BasicTypeInfo.BOOLEAN_TYPE_INFO => new BooleanExplodeTableFunc
        }
      }
      case _: ObjectArrayTypeInfo[_, _] => new ObjectExplodeTableFunc
      case _: BasicArrayTypeInfo[_, _] => new ObjectExplodeTableFunc
      case _ => throw new UnsupportedOperationException(ti.toString + "IS NOT supported")
    }
  }
}
