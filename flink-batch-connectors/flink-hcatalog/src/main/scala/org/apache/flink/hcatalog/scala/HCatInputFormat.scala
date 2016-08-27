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

package org.apache.flink.hcatalog.scala

import org.apache.flink.configuration
import org.apache.flink.hcatalog.HCatInputFormatBase
import org.apache.hadoop.conf.Configuration
import org.apache.hive.hcatalog.data.HCatRecord
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema

/**
 * A InputFormat to read from HCatalog tables.
 * The InputFormat supports projection (selection and order of fields) and partition filters.
 *
 * Data can be returned as [[HCatRecord]] or Scala tuples.
 * Scala tuples support only up to 22 fields.
 *
 */
class HCatInputFormat[T](
  database: String,
  table: String,
  config: Configuration
) extends HCatInputFormatBase[T](database, table, config) {

  def this(database: String, table: String) {
    this(database, table, new Configuration)
  }

  var vals: Array[Any] = Array[Any]()

  override def configure(parameters: configuration.Configuration): Unit = {
    super.configure(parameters)
    vals = new Array[Any](fieldNames.length)
  }

  override protected def getMaxFlinkTupleSize: Int = 22

  override protected def buildFlinkTuple(t: T, record: HCatRecord): T = {

    // Extract all fields from HCatRecord
    var i: Int = 0
    while (i < this.fieldNames.length) {

      val o: AnyRef = record.get(this.fieldNames(i), this.outputSchema)

      // partition columns are returned as String
      //   Check and convert to actual type.
      this.outputSchema.get(i).getType match {
        case HCatFieldSchema.Type.INT =>
          o match {
            case s: String => vals(i) = s.toInt
            case _ => vals(i) = o.asInstanceOf[Int]
          }
        case HCatFieldSchema.Type.TINYINT =>
          o match {
            case s: String => vals(i) = s.toInt.toByte
            case _ => vals(i) = o.asInstanceOf[Byte]
          }
        case HCatFieldSchema.Type.SMALLINT =>
          o match {
            case s: String => vals(i) = s.toInt.toShort
            case _ => vals(i) = o.asInstanceOf[Short]
          }
        case HCatFieldSchema.Type.BIGINT =>
          o match {
            case s: String => vals(i) = s.toLong
            case _ => vals(i) = o.asInstanceOf[Long]
          }
        case HCatFieldSchema.Type.BOOLEAN =>
          o match {
            case s: String => vals(i) = s.toBoolean
            case _ => vals(i) = o.asInstanceOf[Boolean]
          }
        case HCatFieldSchema.Type.FLOAT =>
          o match {
            case s: String => vals(i) = s.toFloat
            case _ => vals(i) = o.asInstanceOf[Float]
          }
        case HCatFieldSchema.Type.DOUBLE =>
          o match {
            case s: String => vals(i) = s.toDouble
            case _ => vals(i) = o.asInstanceOf[Double]
          }
        case HCatFieldSchema.Type.STRING =>
          vals(i) = o
        case HCatFieldSchema.Type.BINARY =>
          if (o.isInstanceOf[String]) {
            throw new RuntimeException("Cannot handle partition keys of type BINARY.")
          }
          else {
            vals(i) = o.asInstanceOf[Array[Byte]]
          }
        case HCatFieldSchema.Type.ARRAY =>
          if (o.isInstanceOf[String]) {
            throw new RuntimeException("Cannot handle partition keys of type ARRAY.")
          }
          else {
            vals(i) = o.asInstanceOf[List[Object]]
          }
        case HCatFieldSchema.Type.MAP =>
          if (o.isInstanceOf[String]) {
            throw new RuntimeException("Cannot handle partition keys of type MAP.")
          }
          else {
            vals(i) = o.asInstanceOf[Map[Object, Object]]
          }
        case HCatFieldSchema.Type.STRUCT =>
          if (o.isInstanceOf[String]) {
            throw new RuntimeException("Cannot handle partition keys of type STRUCT.")
          }
          else {
            vals(i) = o.asInstanceOf[List[Object]]
          }
        case _ =>
          throw new RuntimeException("Invalid type " + this.outputSchema.get(i).getType +
            " encountered.")
      }

      i += 1
    }
    createScalaTuple(vals)
  }

  private def createScalaTuple(vals: Array[Any]): T = {

    this.fieldNames.length match {
      case 1 =>
        Tuple1(vals(0)).asInstanceOf[T]
      case 2 =>
        Tuple2(vals(0), vals(1)).asInstanceOf[T]
      case 3 =>
        Tuple3(vals(0), vals(1), vals(2)).asInstanceOf[T]
      case 4 =>
        Tuple4(vals(0), vals(1), vals(2), vals(3)).asInstanceOf[T]
      case 5 =>
        Tuple5(vals(0), vals(1), vals(2), vals(3), vals(4)).asInstanceOf[T]
      case 6 =>
        Tuple6(vals(0), vals(1), vals(2), vals(3), vals(4), vals(5)).asInstanceOf[T]
      case 7 =>
        Tuple7(vals(0), vals(1), vals(2), vals(3), vals(4), vals(5), vals(6)).asInstanceOf[T]
      case 8 =>
        Tuple8(vals(0), vals(1), vals(2), vals(3), vals(4), vals(5), vals(6), vals(7))
          .asInstanceOf[T]
      case 9 =>
        Tuple9(vals(0), vals(1), vals(2), vals(3), vals(4), vals(5), vals(6), vals(7),
          vals(8)).asInstanceOf[T]
      case 10 =>
        Tuple10(vals(0), vals(1), vals(2), vals(3), vals(4), vals(5), vals(6), vals(7),
          vals(8), vals(9)).asInstanceOf[T]
      case 11 =>
        Tuple11(vals(0), vals(1), vals(2), vals(3), vals(4), vals(5), vals(6), vals(7),
          vals(8), vals(9), vals(10)).asInstanceOf[T]
      case 12 =>
        Tuple12(vals(0), vals(1), vals(2), vals(3), vals(4), vals(5), vals(6), vals(7),
          vals(8), vals(9), vals(10), vals(11)).asInstanceOf[T]
      case 13 =>
        Tuple13(vals(0), vals(1), vals(2), vals(3), vals(4), vals(5), vals(6), vals(7),
          vals(8), vals(9), vals(10), vals(11), vals(12)).asInstanceOf[T]
      case 14 =>
        Tuple14(vals(0), vals(1), vals(2), vals(3), vals(4), vals(5), vals(6), vals(7),
          vals(8), vals(9), vals(10), vals(11), vals(12), vals(13)).asInstanceOf[T]
      case 15 =>
        Tuple15(vals(0), vals(1), vals(2), vals(3), vals(4), vals(5), vals(6), vals(7),
          vals(8), vals(9), vals(10), vals(11), vals(12), vals(13), vals(14)).asInstanceOf[T]
      case 16 =>
        Tuple16(vals(0), vals(1), vals(2), vals(3), vals(4), vals(5), vals(6), vals(7),
          vals(8), vals(9), vals(10), vals(11), vals(12), vals(13), vals(14), vals(15))
          .asInstanceOf[T]
      case 17 =>
        Tuple17(vals(0), vals(1), vals(2), vals(3), vals(4), vals(5), vals(6), vals(7),
          vals(8), vals(9), vals(10), vals(11), vals(12), vals(13), vals(14), vals(15),
          vals(16)).asInstanceOf[T]
      case 18 =>
        Tuple18(vals(0), vals(1), vals(2), vals(3), vals(4), vals(5), vals(6), vals(7),
          vals(8), vals(9), vals(10), vals(11), vals(12), vals(13), vals(14), vals(15),
          vals(16), vals(17)).asInstanceOf[T]
      case 19 =>
        Tuple19(vals(0), vals(1), vals(2), vals(3), vals(4), vals(5), vals(6), vals(7),
          vals(8), vals(9), vals(10), vals(11), vals(12), vals(13), vals(14), vals(15),
          vals(16), vals(17), vals(18)).asInstanceOf[T]
      case 20 =>
        Tuple20(vals(0), vals(1), vals(2), vals(3), vals(4), vals(5), vals(6), vals(7),
          vals(8), vals(9), vals(10), vals(11), vals(12), vals(13), vals(14), vals(15),
          vals(16), vals(17), vals(18), vals(19)).asInstanceOf[T]
      case 21 =>
        Tuple21(vals(0), vals(1), vals(2), vals(3), vals(4), vals(5), vals(6), vals(7),
          vals(8), vals(9), vals(10), vals(11), vals(12), vals(13), vals(14), vals(15),
          vals(16), vals(17), vals(18), vals(19), vals(20)).asInstanceOf[T]
      case 22 =>
        Tuple22(vals(0), vals(1), vals(2), vals(3), vals(4), vals(5), vals(6), vals(7),
          vals(8), vals(9), vals(10), vals(11), vals(12), vals(13), vals(14), vals(15),
          vals(16), vals(17), vals(18), vals(19), vals(20), vals(21)).asInstanceOf[T]
      case _ =>
        throw new RuntimeException("Only up to 22 fields supported for Scala Tuples.")

    }

  }
}
