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

package org.apache.flink.table.util.python

import java.nio.charset.StandardCharsets
import java.sql.{Date, Time, Timestamp}
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.TimeZone
import java.util.function.BiConsumer

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, BasicTypeInfo, PrimitiveArrayTypeInfo, TypeInformation}
import org.apache.flink.api.java.io.CollectionInputFormat
import org.apache.flink.api.java.typeutils.{MapTypeInfo, ObjectArrayTypeInfo, RowTypeInfo}
import org.apache.flink.core.io.InputSplit
import org.apache.flink.table.api.{TableSchema, Types}
import org.apache.flink.table.sources.InputFormatTableSource
import org.apache.flink.types.Row

import scala.collection.JavaConversions._

object PythonTableUtils {

  /**
    * Wrap the unpickled python data with an InputFormat. It will be passed to
    * PythonInputFormatTableSource later.
    *
    * @param data The unpickled python data.
    * @param dataType The python data type.
    * @param config The execution config used to create serializer.
    * @return An InputFormat containing the python data.
    */
  def getInputFormat(
      data: java.util.List[Array[Object]],
      dataType: TypeInformation[Row],
      config: ExecutionConfig): InputFormat[Row, _] = {
    val converter = convertTo(dataType)
    new CollectionInputFormat(data.map(converter(_).asInstanceOf[Row]),
      dataType.createSerializer(config))
  }

  /**
    * Creates a converter that converts `obj` to the type specified by the data type, or returns
    * null if the type of obj is unexpected because Python doesn't enforce the type.
    */
  private def convertTo(dataType: TypeInformation[_]): Any => Any = dataType match {
    case _ if dataType == Types.BOOLEAN => (obj: Any) => nullSafeConvert(obj) {
      case b: Boolean => b
    }

    case _ if dataType == Types.BYTE => (obj: Any) => nullSafeConvert(obj) {
      case c: Byte => c
      case c: Short => c.toByte
      case c: Int => c.toByte
      case c: Long => c.toByte
    }

    case _ if dataType == Types.SHORT => (obj: Any) => nullSafeConvert(obj) {
      case c: Byte => c.toShort
      case c: Short => c
      case c: Int => c.toShort
      case c: Long => c.toShort
    }

    case _ if dataType == Types.INT => (obj: Any) => nullSafeConvert(obj) {
      case c: Byte => c.toInt
      case c: Short => c.toInt
      case c: Int => c
      case c: Long => c.toInt
    }

    case _ if dataType == Types.LONG => (obj: Any) => nullSafeConvert(obj) {
      case c: Byte => c.toLong
      case c: Short => c.toLong
      case c: Int => c.toLong
      case c: Long => c
    }

    case _ if dataType == Types.FLOAT => (obj: Any) => nullSafeConvert(obj) {
      case c: Float => c
      case c: Double => c.toFloat
    }

    case _ if dataType == Types.DOUBLE => (obj: Any) => nullSafeConvert(obj) {
      case c: Float => c.toDouble
      case c: Double => c
    }

    case _ if dataType == Types.DECIMAL => (obj: Any) => nullSafeConvert(obj) {
      case c: java.math.BigDecimal => c
    }

    case _ if dataType == Types.SQL_DATE => (obj: Any) => nullSafeConvert(obj) {
      case c: Int =>
        val millisLocal = c.toLong * 86400000
        val millisUtc = millisLocal - getOffsetFromLocalMillis(millisLocal)
        new Date(millisUtc)
    }

    case _ if dataType == Types.SQL_TIME => (obj: Any) => nullSafeConvert(obj) {
      case c: Long => new Time(c / 1000)
      case c: Int => new Time(c.toLong / 1000)
    }

    case _ if dataType == Types.SQL_TIMESTAMP => (obj: Any) => nullSafeConvert(obj) {
      case c: Long => new Timestamp(c / 1000)
      case c: Int => new Timestamp(c.toLong / 1000)
    }

    case _ if dataType == Types.INTERVAL_MILLIS() => (obj: Any) => nullSafeConvert(obj) {
      case c: Long => c / 1000
      case c: Int => c.toLong / 1000
    }

    case _ if dataType == Types.STRING => (obj: Any) => nullSafeConvert(obj) {
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
      var boxed = false
      val elementType = dataType match {
        case p: PrimitiveArrayTypeInfo[_] =>
          p.getComponentType
        case b: BasicArrayTypeInfo[_, _] =>
          boxed = true
          b.getComponentInfo
        case o: ObjectArrayTypeInfo[_, _] =>
          boxed = true
          o.getComponentInfo
      }
      val elementFromJava = convertTo(elementType)

      (obj: Any) => nullSafeConvert(obj) {
        case c: java.util.List[_] =>
          createArray(elementType,
                      c.size(),
                      i => elementFromJava(c.get(i)),
                      boxed)
        case c if c.getClass.isArray =>
          createArray(elementType,
                      c.asInstanceOf[Array[_]].length,
                      i => elementFromJava(c.asInstanceOf[Array[_]](i)),
                      boxed)
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
      getElement: Int => Any,
      boxed: Boolean = false): Array[_] = {
    elementType match {
      case BasicTypeInfo.BOOLEAN_TYPE_INFO =>
        if (!boxed) {
          val array = new Array[Boolean](length)
          for (i <- 0 until length) {
            array(i) = getElement(i).asInstanceOf[Boolean]
          }
          array
        } else {
          val array = new Array[java.lang.Boolean](length)
          for (i <- 0 until length) {
            if (getElement(i) != null) {
              array(i) = java.lang.Boolean.valueOf(getElement(i).asInstanceOf[Boolean])
            } else {
              array(i) = null
            }
          }
          array
        }

      case BasicTypeInfo.BYTE_TYPE_INFO =>
        if (!boxed) {
          val array = new Array[Byte](length)
          for (i <- 0 until length) {
            array(i) = getElement(i).asInstanceOf[Byte]
          }
          array
        } else {
          val array = new Array[java.lang.Byte](length)
          for (i <- 0 until length) {
            if (getElement(i) != null) {
              array(i) = java.lang.Byte.valueOf(getElement(i).asInstanceOf[Byte])
            } else {
              array(i) = null
            }
          }
          array
        }

      case BasicTypeInfo.SHORT_TYPE_INFO =>
        if (!boxed) {
          val array = new Array[Short](length)
          for (i <- 0 until length) {
            array(i) = getElement(i).asInstanceOf[Short]
          }
          array
        } else {
          val array = new Array[java.lang.Short](length)
          for (i <- 0 until length) {
            if (getElement(i) != null) {
              array(i) = java.lang.Short.valueOf(getElement(i).asInstanceOf[Short])
            } else {
              array(i) = null
            }
          }
          array
        }

      case BasicTypeInfo.INT_TYPE_INFO =>
        if (!boxed) {
          val array = new Array[Int](length)
          for (i <- 0 until length) {
            array(i) = getElement(i).asInstanceOf[Int]
          }
          array
        } else {
          val array = new Array[java.lang.Integer](length)
          for (i <- 0 until length) {
            if (getElement(i) != null) {
              array(i) = java.lang.Integer.valueOf(getElement(i).asInstanceOf[Int])
            } else {
              array(i) = null
            }
          }
          array
        }

      case BasicTypeInfo.LONG_TYPE_INFO =>
        if (!boxed) {
          val array = new Array[Long](length)
          for (i <- 0 until length) {
            array(i) = getElement(i).asInstanceOf[Long]
          }
          array
        } else {
          val array = new Array[java.lang.Long](length)
          for (i <- 0 until length) {
            if (getElement(i) != null) {
              array(i) = java.lang.Long.valueOf(getElement(i).asInstanceOf[Long])
            } else {
              array(i) = null
            }
          }
          array
        }

      case BasicTypeInfo.FLOAT_TYPE_INFO =>
        if (!boxed) {
          val array = new Array[Float](length)
          for (i <- 0 until length) {
            array(i) = getElement(i).asInstanceOf[Float]
          }
          array
        } else {
          val array = new Array[java.lang.Float](length)
          for (i <- 0 until length) {
            if (getElement(i) != null) {
              array(i) = java.lang.Float.valueOf(getElement(i).asInstanceOf[Float])
            } else {
              array(i) = null
            }
          }
          array
        }

      case BasicTypeInfo.DOUBLE_TYPE_INFO =>
        if (!boxed) {
          val array = new Array[Double](length)
          for (i <- 0 until length) {
            array(i) = getElement(i).asInstanceOf[Double]
          }
          array
        } else {
          val array = new Array[java.lang.Double](length)
          for (i <- 0 until length) {
            if (getElement(i) != null) {
              array(i) = java.lang.Double.valueOf(getElement(i).asInstanceOf[Double])
            } else {
              array(i) = null
            }
          }
          array
        }

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

  def getOffsetFromLocalMillis(millisLocal: Long): Int = {
    val localZone = TimeZone.getDefault
    var result = localZone.getRawOffset
    // the actual offset should be calculated based on milliseconds in UTC
    val offset = localZone.getOffset(millisLocal - result)
    if (offset != result) {
      // DayLight Saving Time
      result = localZone.getOffset(millisLocal - offset)
      if (result != offset) {
        // fallback to do the reverse lookup using java.time.LocalDateTime
        // this should only happen near the start or end of DST
        val localDate = LocalDate.ofEpochDay(millisLocal / 86400000)
        val localTime = LocalTime.ofNanoOfDay(
          Math.floorMod(millisLocal, 86400000) * 1000 * 1000)
        val localDateTime = LocalDateTime.of(localDate, localTime)
        val millisEpoch = localDateTime.atZone(localZone.toZoneId).toInstant.toEpochMilli
        result = (millisLocal - millisEpoch).toInt
      }
    }
    result
  }
}

/**
  * An InputFormatTableSource created by python 'from_element' method.
  *
  * @param inputFormat The input format which contains the python data collection,
  *                    usually created by PythonTableUtils#getInputFormat method
  * @param rowTypeInfo The row type info of the python data.
  *                    It is generated by the python 'from_element' method.
  */
class PythonInputFormatTableSource[Row](
    inputFormat: InputFormat[Row, _ <: InputSplit],
    rowTypeInfo: RowTypeInfo
) extends InputFormatTableSource[Row] {

  override def getInputFormat: InputFormat[Row, _ <: InputSplit] = inputFormat

  override def getTableSchema: TableSchema = TableSchema.fromTypeInfo(rowTypeInfo)

  override def getReturnType: TypeInformation[Row] = rowTypeInfo.asInstanceOf[TypeInformation[Row]]
}
