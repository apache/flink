package org.apache.flink.table.plan.util

import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, BasicTypeInfo, PrimitiveArrayTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo
import org.apache.flink.table.functions.TableFunction

/**
  * Created by suez on 4/24/17.
  */
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
      case pati: PrimitiveArrayTypeInfo[_] => {
        pati.getComponentType match {
          case BasicTypeInfo.INT_TYPE_INFO => new IntExplodeTableFunc
          case BasicTypeInfo.LONG_TYPE_INFO => new LongExplodeTableFunc
          case BasicTypeInfo.SHORT_TYPE_INFO => new ShortExplodeTableFunc
          case BasicTypeInfo.FLOAT_TYPE_INFO => new FloatExplodeTableFunc
          case BasicTypeInfo.DOUBLE_TYPE_INFO => new DoubleExplodeTableFunc
          case BasicTypeInfo.BYTE_TYPE_INFO => new ByteExplodeTableFunc
          case BasicTypeInfo.BOOLEAN_TYPE_INFO => new BooleanExplodeTableFunc
        }
      }
      case oati: ObjectArrayTypeInfo[_, _] => new ObjectExplodeTableFunc
      case bati: BasicArrayTypeInfo[_, _] => new ObjectExplodeTableFunc
      case _ => throw new UnsupportedOperationException(ti.toString + "IS NOT supported")
    }
  }
}
