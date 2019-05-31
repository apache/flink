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

package org.apache.flink.table.python

import java.nio.charset.StandardCharsets
import java.sql.{Date, Time, Timestamp}
import java.util.function.BiConsumer

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, BasicTypeInfo, PrimitiveArrayTypeInfo, TypeInformation}
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.typeutils.{MapTypeInfo, ObjectArrayTypeInfo, RowTypeInfo}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.api.java.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.flink.types.Row

object PythonTableUtils {

  /**
    * Converts the given [[DataStream]] into a [[Table]].
    *
    * The schema of the [[Table]] is derived from the specified schemaString.
    *
    * @param tableEnv The table environment.
    * @param dataStream The [[DataStream]] to be converted.
    * @param dataType The type information of the table.
    * @return The converted [[Table]].
    */
  def fromDataStream(
      tableEnv: StreamTableEnvironment,
      dataStream: DataStream[Array[Object]],
      dataType: TypeInformation[Row]): Table = {
    val convertedDataStream = dataStream.map(
      new MapFunction[Array[Object], Row] {
        override def map(value: Array[Object]): Row =
          convertTo(dataType).apply(value).asInstanceOf[Row]
      }).returns(dataType.asInstanceOf[TypeInformation[Row]])

    tableEnv.fromDataStream(convertedDataStream)
  }

  /**
    * Converts the given [[DataSet]] into a [[Table]].
    *
    * The schema of the [[Table]] is derived from the specified schemaString.
    *
    * @param tableEnv The table environment.
    * @param dataSet The [[DataSet]] to be converted.
    * @param dataType The type information of the table.
    * @return The converted [[Table]].
    */
  def fromDataSet(
      tableEnv: BatchTableEnvironment,
      dataSet: DataSet[Array[Object]],
      dataType: TypeInformation[Row]): Table = {
    val convertedDataSet = dataSet.map(
      new MapFunction[Array[Object], Row] {
        override def map(value: Array[Object]): Row =
          convertTo(dataType).apply(value).asInstanceOf[Row]
      }).returns(dataType.asInstanceOf[TypeInformation[Row]])

    tableEnv.fromDataSet(convertedDataSet)
  }

  /**
    * Creates a converter that converts `obj` to the type specified by the data type, or returns
    * null if the type of obj is unexpected because Python doesn't enforce the type.
    */
  private def convertTo(dataType: TypeInformation[_]): Any => Any = dataType match {
    case Types.BOOLEAN => (obj: Any) => nullSafeConvert(obj) {
      case b: Boolean => b
    }

    case Types.BYTE => (obj: Any) => nullSafeConvert(obj) {
      case c: Byte => c
      case c: Short => c.toByte
      case c: Int => c.toByte
      case c: Long => c.toByte
    }

    case Types.SHORT => (obj: Any) => nullSafeConvert(obj) {
      case c: Byte => c.toShort
      case c: Short => c
      case c: Int => c.toShort
      case c: Long => c.toShort
    }

    case Types.INT => (obj: Any) => nullSafeConvert(obj) {
      case c: Byte => c.toInt
      case c: Short => c.toInt
      case c: Int => c
      case c: Long => c.toInt
    }

    case Types.LONG => (obj: Any) => nullSafeConvert(obj) {
      case c: Byte => c.toLong
      case c: Short => c.toLong
      case c: Int => c.toLong
      case c: Long => c
    }

    case Types.FLOAT => (obj: Any) => nullSafeConvert(obj) {
      case c: Float => c
      case c: Double => c.toFloat
    }

    case Types.DOUBLE => (obj: Any) => nullSafeConvert(obj) {
      case c: Float => c.toDouble
      case c: Double => c
    }

    case Types.DECIMAL => (obj: Any) => nullSafeConvert(obj) {
      case c: java.math.BigDecimal => c
    }

    case Types.SQL_DATE => (obj: Any) => nullSafeConvert(obj) {
      case c: Int => new Date(c * 86400000)
    }

    case Types.SQL_TIME => (obj: Any) => nullSafeConvert(obj) {
      case c: Long => new Time(c / 1000)
      case c: Int => new Time(c.toLong / 1000)
    }

    case Types.SQL_TIMESTAMP => (obj: Any) => nullSafeConvert(obj) {
      case c: Long => new Timestamp(c / 1000)
      case c: Int => new Timestamp(c.toLong / 1000)
    }

    case Types.STRING => (obj: Any) => nullSafeConvert(obj) {
      case _ => obj.toString
    }

    case PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO =>
      (obj: Any) =>
        nullSafeConvert(obj) {
          case c: String => c.getBytes(StandardCharsets.UTF_8)
          case c if c.getClass.isArray && c.getClass.getComponentType.getName == "byte" => c
        }

    case _: PrimitiveArrayTypeInfo[_] |
         _: BasicArrayTypeInfo[_, _] |
         _: ObjectArrayTypeInfo[_, _] =>
      val elementType = dataType match {
        case p: PrimitiveArrayTypeInfo[_] =>
          p.getComponentType
        case b: BasicArrayTypeInfo[_, _] =>
          b.getComponentInfo
        case o: ObjectArrayTypeInfo[_, _] =>
          o.getComponentInfo
      }
      val elementFromJava = convertTo(elementType)

      (obj: Any) => nullSafeConvert(obj) {
        case c: java.util.List[_] =>
          createArray(elementType,
                      c.size(),
                      i => elementFromJava(c.get(i)))
        case c if c.getClass.isArray =>
          createArray(elementType,
                      c.asInstanceOf[Array[_]].length,
                      i => elementFromJava(c.asInstanceOf[Array[_]](i)))
      }

    case m: MapTypeInfo[_, _] =>
      val keyFromJava = convertTo(m.getKeyTypeInfo)
      val valueFromJava = convertTo(m.getValueTypeInfo)

      (obj: Any) => nullSafeConvert(obj) {
        case javaMap: java.util.Map[_, _] =>
          val map = new java.util.HashMap[Any, Any]
          javaMap.forEach(new BiConsumer[Any, Any] {
            override def accept(k: Any, v: Any): Unit =
              map.put(keyFromJava(k), valueFromJava(v))
          })
          map
      }

    case rowType: RowTypeInfo =>
      val fieldsFromJava = rowType.getFieldTypes.map(f => convertTo(f))

      (obj: Any) => nullSafeConvert(obj) {
        case c if c.getClass.isArray =>
          val r = c.asInstanceOf[Array[_]]
          if (r.length != rowType.getFieldTypes.length) {
            throw new IllegalStateException(
              s"Input row doesn't have expected number of values required by the schema. " +
                s"${rowType.getFieldTypes.length} fields are required while ${r.length} " +
                s"values are provided."
              )
          }

          val row = new Row(r.length)
          var i = 0
          while (i < r.length) {
            row.setField(i, fieldsFromJava(i)(r(i)))
            i += 1
          }
          row
      }

    // UserDefinedType
    case _ => (obj: Any) => obj
  }

  private def nullSafeConvert(input: Any)(f: PartialFunction[Any, Any]): Any = {
    if (input == null) {
      null
    } else {
      f.applyOrElse(input, {
        _: Any => null
      })
    }
  }

  private def createArray(
      elementType: TypeInformation[_],
      length: Int,
      getElement: Int => Any): Array[_] = {
    elementType match {
      case BasicTypeInfo.BOOLEAN_TYPE_INFO =>
        val array = new Array[Boolean](length)
        for (i <- 0 until length) {
          array(i) = getElement(i).asInstanceOf[Boolean]
        }
        array

      case BasicTypeInfo.BYTE_TYPE_INFO =>
        val array = new Array[Byte](length)
        for (i <- 0 until length) {
          array(i) = getElement(i).asInstanceOf[Byte]
        }
        array

      case BasicTypeInfo.SHORT_TYPE_INFO =>
        val array = new Array[Short](length)
        for (i <- 0 until length) {
          array(i) = getElement(i).asInstanceOf[Short]
        }
        array

      case BasicTypeInfo.INT_TYPE_INFO =>
        val array = new Array[Int](length)
        for (i <- 0 until length) {
          array(i) = getElement(i).asInstanceOf[Int]
        }
        array

      case BasicTypeInfo.LONG_TYPE_INFO =>
        val array = new Array[Long](length)
        for (i <- 0 until length) {
          array(i) = getElement(i).asInstanceOf[Long]
        }
        array

      case BasicTypeInfo.FLOAT_TYPE_INFO =>
        val array = new Array[Float](length)
        for (i <- 0 until length) {
          array(i) = getElement(i).asInstanceOf[Float]
        }
        array

      case BasicTypeInfo.DOUBLE_TYPE_INFO =>
        val array = new Array[Double](length)
        for (i <- 0 until length) {
          array(i) = getElement(i).asInstanceOf[Double]
        }
        array

      case BasicTypeInfo.STRING_TYPE_INFO =>
        val array = new Array[java.lang.String](length)
        for (i <- 0 until length) {
          array(i) = getElement(i).asInstanceOf[java.lang.String]
        }
        array

      case _ =>
        val array = new Array[Object](length)
        for (i <- 0 until length) {
          array(i) = getElement(i).asInstanceOf[Object]
        }
        array
    }
  }
}
