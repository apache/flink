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

import java.io.IOException
import java.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{WritableTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.hcatalog.HCatOutputFormatBase
import org.apache.hadoop.conf.Configuration
import org.apache.hive.hcatalog.data.{DefaultHCatRecord, HCatRecord}

import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConverters._

/*
 * A OutputFormat to write to HCatalog tables for scala api
 * The outputFormat supports write to table and partitions through partitionValue map.
 *
 * scala tuple types and {@link HCatRecord} are accepted
 * as data source. If user want to write other type of data to HCatalog, a preceding Map function
 * is required to convert the user data type to {@link HCatRecord}
 *
 * The constructor provides type checking and requires import org.apache.flink.api.scala._ in the
 * import statement of the user's program when compiling to allow scala macro to extract type info
 */
/**
 *
 * @param database hive database name, if null, default database is used
 * @param table hive table name
 * @param partitionValues Map of partition values, if null, whole table is used
 * @param config the configuration
 * @tparam T
 */
class HCatOutputFormat[T: TypeInformation](
                                            database: String,
                                            table: String,
                                            partitionValues: Map[String, String],
                                            config: Configuration
                                            ) extends
HCatOutputFormatBase[T](database, table, mapAsJavaMap(partitionValues), config) {
  val ti: TypeInformation[T] = createTypeInformation[T]
  //check the types of columns if the input is tuple(case class) type
  ti match {
    case value: CaseClassTypeInfo[_] => {
      //check the number of columns
      if (value.getArity != this.reqType.getArity) {
        throw new IOException("tuple has different arity from the table's column numbers")
      }
      for (i <- 0 until this.reqType.getArity) {
        val it = value.getTypeAt(i)
        val ot = reqType.asInstanceOf[TupleTypeInfo[_]].getTypeAt(i)
        if (!isCompatibleType(it, ot)) {
          throw new IOException("field "+ i + " has different type from required")
        }
      }
    }
    case value: WritableTypeInfo[HCatRecord] => //HCatRecord is ok
    case _ => throw new IOException("HCatOutputFormat only supports tuple-derived type " +
      "and HCatRecord")
  }

  /**
   *
   * @param database hive database name, if null, default database is used
   * @param table hive table name
   * @param partitionValues Map of partition values, if null, whole table is used
   */
  def this(database: String, table: String, partitionValues: Map[String, String]) {
    this(database, table, partitionValues, new Configuration)
  }

  //convert the type T to HCatRecord, if T is already HCatRecord, do nothing
  override protected def convertTupleToHCat(record: T): HCatRecord = {
    ti match {
      case value: CaseClassTypeInfo[_] => new DefaultHCatRecord(toJavaList(record))
      case value: WritableTypeInfo[HCatRecord] => record.asInstanceOf[HCatRecord]
      case _ => throw new IOException("HCatOutputFormat only supports tuple-derived type " +
        "and HCatRecord")
    }
  }

  //scala tuple max arity 22.
  override protected def getMaxFlinkTupleSize: Int = 22

  //THe HCatalog expects Java types for complex types
  private def toJava(input: AnyRef): Object = {
    input match {
      case value: Map[_, _] => value.asJava
      case value: List[_] => value.asJava
      case _ => input
    }
  }

  //for complex types, we map scala List to java List and scala map to java map.
  //for primitive maps, we require equivalence between their typeinfo
  private def isCompatibleType(scalaT: TypeInformation[_], javaT: TypeInformation[_]): Boolean = {
    if (scalaT.equals(javaT)) true
    else if (scalaT.getTypeClass == classOf[List[_]]) {
      javaT.getTypeClass == classOf[java.util.List[_]]
    }
    else if (scalaT.getTypeClass == classOf[Map[_, _]]) {
      javaT.getTypeClass == classOf[java.util.Map[_, _]]
    }
    else false
  }

  val l = Seq(1,2,3,4,5,6,7,8,9)
  //fill an empty java list with the content of a scala tuple, with conversion for complex types
  private def toJavaList(record: T): util.List[AnyRef] = {
    record match {
      case Tuple1(i1: AnyRef) => Seq(toJava(i1)).asJava
      case Tuple2(i1: AnyRef, i2: AnyRef) => {
        Seq(toJava(i1),toJava(i2)).asJava
      }
      case Tuple3(i1: AnyRef, i2: AnyRef, i3: AnyRef) => {
        Seq(toJava(i1),toJava(i2), toJava(i3)).asJava
      }
      case Tuple4(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef) => {
        Seq(toJava(i1),toJava(i2), toJava(i3), toJava(i4)).asJava
      }
      case Tuple5(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef) => {
        Seq(toJava(i1),toJava(i2), toJava(i3), toJava(i4), toJava(i5)).asJava
      }
      case Tuple6(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef) => {
        Seq(toJava(i1),toJava(i2), toJava(i3), toJava(i4), toJava(i5), toJava(i6)).asJava
      }
      case Tuple7(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef) => {
        Seq(toJava(i1),toJava(i2), toJava(i3), toJava(i4), toJava(i5), toJava(i6), toJava(i7))
          .asJava
      }
      case Tuple8(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef) => {
        Seq(toJava(i1),toJava(i2), toJava(i3), toJava(i4), toJava(i5), toJava(i6), toJava(i7),
        toJava(i8)).asJava
      }
      case Tuple9(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef) => {
        Seq(toJava(i1),toJava(i2), toJava(i3), toJava(i4), toJava(i5), toJava(i6), toJava(i7),
          toJava(i8), toJava(i9)).asJava
      }
      case Tuple10(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef) => {
        Seq(toJava(i1),toJava(i2), toJava(i3), toJava(i4), toJava(i5), toJava(i6), toJava(i7),
          toJava(i8), toJava(i9), toJava(i10)).asJava
      }
      case Tuple11(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef, i11: AnyRef) => {
        Seq(toJava(i1),toJava(i2), toJava(i3), toJava(i4), toJava(i5), toJava(i6), toJava(i7),
          toJava(i8), toJava(i9), toJava(i10), toJava(i11)).asJava
      }
      case Tuple12(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef, i11: AnyRef, i12: AnyRef) => {
        Seq(toJava(i1),toJava(i2), toJava(i3), toJava(i4), toJava(i5), toJava(i6), toJava(i7),
          toJava(i8), toJava(i9), toJava(i10), toJava(i11), toJava(i12)).asJava
      }
      case Tuple13(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef, i11: AnyRef, i12: AnyRef, i13: AnyRef) => {
        Seq(toJava(i1),toJava(i2), toJava(i3), toJava(i4), toJava(i5), toJava(i6), toJava(i7),
          toJava(i8), toJava(i9), toJava(i10), toJava(i11), toJava(i12), toJava(i13)).asJava
      }
      case Tuple14(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef, i11: AnyRef, i12: AnyRef, i13: AnyRef,
      i14: AnyRef) => {
        Seq(toJava(i1),toJava(i2), toJava(i3), toJava(i4), toJava(i5), toJava(i6), toJava(i7),
          toJava(i8), toJava(i9), toJava(i10), toJava(i11), toJava(i12), toJava(i13), toJava(i14))
          .asJava
      }
      case Tuple15(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef, i11: AnyRef, i12: AnyRef, i13: AnyRef,
      i14: AnyRef, i15: AnyRef) => {
        Seq(toJava(i1),toJava(i2), toJava(i3), toJava(i4), toJava(i5), toJava(i6), toJava(i7),
          toJava(i8), toJava(i9), toJava(i10), toJava(i11), toJava(i12), toJava(i13), toJava(i14),
        toJava(i15)).asJava
      }
      case Tuple16(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef, i11: AnyRef, i12: AnyRef, i13: AnyRef,
      i14: AnyRef, i15: AnyRef, i16: AnyRef) => {
        Seq(toJava(i1),toJava(i2), toJava(i3), toJava(i4), toJava(i5), toJava(i6), toJava(i7),
          toJava(i8), toJava(i9), toJava(i10), toJava(i11), toJava(i12), toJava(i13), toJava(i14),
          toJava(i15), toJava(i16)).asJava
      }
      case Tuple17(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef, i11: AnyRef, i12: AnyRef, i13: AnyRef,
      i14: AnyRef, i15: AnyRef, i16: AnyRef, i17: AnyRef) => {
        Seq(toJava(i1),toJava(i2), toJava(i3), toJava(i4), toJava(i5), toJava(i6), toJava(i7),
          toJava(i8), toJava(i9), toJava(i10), toJava(i11), toJava(i12), toJava(i13), toJava(i14),
          toJava(i15), toJava(i16), toJava(i17)).asJava
      }
      case Tuple18(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef, i11: AnyRef, i12: AnyRef, i13: AnyRef,
      i14: AnyRef, i15: AnyRef, i16: AnyRef, i17: AnyRef, i18: AnyRef) => {
        Seq(toJava(i1),toJava(i2), toJava(i3), toJava(i4), toJava(i5), toJava(i6), toJava(i7),
          toJava(i8), toJava(i9), toJava(i10), toJava(i11), toJava(i12), toJava(i13), toJava(i14),
          toJava(i15), toJava(i16), toJava(i17), toJava(i18)).asJava
      }
      case Tuple19(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef, i11: AnyRef, i12: AnyRef, i13: AnyRef,
      i14: AnyRef, i15: AnyRef, i16: AnyRef, i17: AnyRef, i18: AnyRef, i19: AnyRef) => {
        Seq(toJava(i1),toJava(i2), toJava(i3), toJava(i4), toJava(i5), toJava(i6), toJava(i7),
          toJava(i8), toJava(i9), toJava(i10), toJava(i11), toJava(i12), toJava(i13), toJava(i14),
          toJava(i15), toJava(i16), toJava(i17), toJava(i18), toJava(i19)).asJava
      }
      case Tuple20(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef, i11: AnyRef, i12: AnyRef, i13: AnyRef,
      i14: AnyRef, i15: AnyRef, i16: AnyRef, i17: AnyRef, i18: AnyRef, i19: AnyRef, i20: AnyRef)
      => {
        Seq(toJava(i1),toJava(i2), toJava(i3), toJava(i4), toJava(i5), toJava(i6), toJava(i7),
          toJava(i8), toJava(i9), toJava(i10), toJava(i11), toJava(i12), toJava(i13), toJava(i14),
          toJava(i15), toJava(i16), toJava(i17), toJava(i18), toJava(i19), toJava(i20)).asJava
      }
      case Tuple21(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef, i11: AnyRef, i12: AnyRef, i13: AnyRef,
      i14: AnyRef, i15: AnyRef, i16: AnyRef, i17: AnyRef, i18: AnyRef, i19: AnyRef, i20: AnyRef,
      i21: AnyRef) => {
        Seq(toJava(i1),toJava(i2), toJava(i3), toJava(i4), toJava(i5), toJava(i6), toJava(i7),
          toJava(i8), toJava(i9), toJava(i10), toJava(i11), toJava(i12), toJava(i13), toJava(i14),
          toJava(i15), toJava(i16), toJava(i17), toJava(i18), toJava(i19), toJava(i20), toJava(i21))
          .asJava
      }
      case Tuple22(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef, i11: AnyRef, i12: AnyRef, i13: AnyRef,
      i14: AnyRef, i15: AnyRef, i16: AnyRef, i17: AnyRef, i18: AnyRef, i19: AnyRef, i20: AnyRef,
      i21: AnyRef, i22: AnyRef) => {
        Seq(toJava(i1),toJava(i2), toJava(i3), toJava(i4), toJava(i5), toJava(i6), toJava(i7),
          toJava(i8), toJava(i9), toJava(i10), toJava(i11), toJava(i12), toJava(i13), toJava(i14),
          toJava(i15), toJava(i16), toJava(i17), toJava(i18), toJava(i19), toJava(i20), toJava(i21),
        toJava(i22)).asJava
      }
      case _ =>
        throw new IOException("Only scala tuple is allowed")
    }
  }
}
