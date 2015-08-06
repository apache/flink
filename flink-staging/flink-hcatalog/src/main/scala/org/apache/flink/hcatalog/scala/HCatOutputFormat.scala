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
import org.apache.flink.api.java.typeutils.TupleTypeInfo
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

  //get the TypeInformation
  val ti: TypeInformation[T] = createTypeInformation[T]


  //check the types of columns if the input is tuple(case class) type
  if (ti.isInstanceOf[CaseClassTypeInfo[_]]) {
    //check the number of columns
    if (ti.asInstanceOf[CaseClassTypeInfo[_]].getArity != this.reqType.getArity) {
      throw new IOException("tuple has different arity from the table's column numbers")
    }

    for (i <- 0 until this.reqType.getArity) {
      val it = ti.asInstanceOf[CaseClassTypeInfo[_]].getTypeAt(i)
      val ot = reqType.asInstanceOf[TupleTypeInfo[_]].getTypeAt(i)

      if (!isCompatibleType(it, ot)) {
        throw new IOException("field has different type from required")
      }
    }
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
  override protected def TupleToHCatRecord(record: T): HCatRecord = {
    if (ti.isInstanceOf[CaseClassTypeInfo[_]]) {

      val fieldList: util.List[AnyRef] = new util.ArrayList[AnyRef]
      //we've checked the type of the scala tuple matches the table, so fill the list now
      fillList(fieldList, record.asInstanceOf[AnyRef])
      new DefaultHCatRecord(fieldList)
    }
    else if (record.isInstanceOf[HCatRecord]) {
      record.asInstanceOf[HCatRecord]
    }
    else {
      throw new IOException("the record should be either scala Tuple or HCatRecord")
    }
  }

  //scala tuple max arity 22.
  override protected def getMaxFlinkTupleSize: Int = 22

  //THe HCatalog expects Java types for complex types
  private def toJava(i: AnyRef): Object = {
    if (i.isInstanceOf[Map[_, _]]) i.asInstanceOf[Map[_, _]].asJava
    else if (i.isInstanceOf[List[_]]) i.asInstanceOf[List[_]].asJava
    else i
  }

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

  //fill an empty java list with the content of a scala tuple, with conversion for complex types
  private def fillList(list: util.List[AnyRef], o: AnyRef) = {
    o match {
      case Tuple1(i1: AnyRef) => list.add(toJava(i1))
      case Tuple2(i1: AnyRef, i2: AnyRef) => {
        list.add(toJava(i1))
        list.add(toJava(i2))
      }
      case Tuple3(i1: AnyRef, i2: AnyRef, i3: AnyRef) => {
        list.add(toJava(i1))
        list.add(toJava(i2))
        list.add(toJava(i3))
      }
      case Tuple4(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef) => {
        list.add(toJava(i1))
        list.add(toJava(i2))
        list.add(toJava(i3))
        list.add(toJava(i4))
      }
      case Tuple5(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef) => {
        list.add(toJava(i1))
        list.add(toJava(i2))
        list.add(toJava(i3))
        list.add(toJava(i4))
        list.add(toJava(i5))
      }
      case Tuple6(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef) => {
        list.add(toJava(i1))
        list.add(toJava(i2))
        list.add(toJava(i3))
        list.add(toJava(i4))
        list.add(toJava(i5))
        list.add(toJava(i6))
      }
      case Tuple7(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef) => {
        list.add(toJava(i1))
        list.add(toJava(i2))
        list.add(toJava(i3))
        list.add(toJava(i4))
        list.add(toJava(i5))
        list.add(toJava(i6))
        list.add(toJava(i7))
      }
      case Tuple8(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef) => {
        list.add(toJava(i1))
        list.add(toJava(i2))
        list.add(toJava(i3))
        list.add(toJava(i4))
        list.add(toJava(i5))
        list.add(toJava(i6))
        list.add(toJava(i7))
        list.add(toJava(i8))
      }
      case Tuple9(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef) => {
        list.add(toJava(i1))
        list.add(toJava(i2))
        list.add(toJava(i3))
        list.add(toJava(i4))
        list.add(toJava(i5))
        list.add(toJava(i6))
        list.add(toJava(i7))
        list.add(toJava(i8))
        list.add(toJava(i9))
      }
      case Tuple10(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef) => {
        list.add(toJava(i1))
        list.add(toJava(i2))
        list.add(toJava(i3))
        list.add(toJava(i4))
        list.add(toJava(i5))
        list.add(toJava(i6))
        list.add(toJava(i7))
        list.add(toJava(i8))
        list.add(toJava(i9))
      }
      case Tuple11(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef, i11: AnyRef) => {
        list.add(toJava(i1))
        list.add(toJava(i2))
        list.add(toJava(i3))
        list.add(toJava(i4))
        list.add(toJava(i5))
        list.add(toJava(i6))
        list.add(toJava(i7))
        list.add(toJava(i8))
        list.add(toJava(i9))
        list.add(toJava(i11))
      }
      case Tuple12(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef, i11: AnyRef, i12: AnyRef) => {
        list.add(toJava(i1))
        list.add(toJava(i2))
        list.add(toJava(i3))
        list.add(toJava(i4))
        list.add(toJava(i5))
        list.add(toJava(i6))
        list.add(toJava(i7))
        list.add(toJava(i8))
        list.add(toJava(i9))
        list.add(toJava(i11))
        list.add(toJava(i12))
      }
      case Tuple13(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef, i11: AnyRef, i12: AnyRef, i13: AnyRef) => {
        list.add(toJava(i1))
        list.add(toJava(i2))
        list.add(toJava(i3))
        list.add(toJava(i4))
        list.add(toJava(i5))
        list.add(toJava(i6))
        list.add(toJava(i7))
        list.add(toJava(i8))
        list.add(toJava(i9))
        list.add(toJava(i11))
        list.add(toJava(i12))
        list.add(toJava(i13))
      }
      case Tuple14(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef, i11: AnyRef, i12: AnyRef, i13: AnyRef,
      i14: AnyRef) => {
        list.add(toJava(i1))
        list.add(toJava(i2))
        list.add(toJava(i3))
        list.add(toJava(i4))
        list.add(toJava(i5))
        list.add(toJava(i6))
        list.add(toJava(i7))
        list.add(toJava(i8))
        list.add(toJava(i9))
        list.add(toJava(i11))
        list.add(toJava(i12))
        list.add(toJava(i13))
        list.add(toJava(i14))
      }
      case Tuple15(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef, i11: AnyRef, i12: AnyRef, i13: AnyRef,
      i14: AnyRef, i15: AnyRef) => {
        list.add(toJava(i1))
        list.add(toJava(i2))
        list.add(toJava(i3))
        list.add(toJava(i4))
        list.add(toJava(i5))
        list.add(toJava(i6))
        list.add(toJava(i7))
        list.add(toJava(i8))
        list.add(toJava(i9))
        list.add(toJava(i11))
        list.add(toJava(i12))
        list.add(toJava(i13))
        list.add(toJava(i14))
        list.add(toJava(i15))
      };
      case Tuple16(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef, i11: AnyRef, i12: AnyRef, i13: AnyRef,
      i14: AnyRef, i15: AnyRef, i16: AnyRef) => {
        list.add(toJava(i1))
        list.add(toJava(i2))
        list.add(toJava(i3))
        list.add(toJava(i4))
        list.add(toJava(i5))
        list.add(toJava(i6))
        list.add(toJava(i7))
        list.add(toJava(i8))
        list.add(toJava(i9))
        list.add(toJava(i11))
        list.add(toJava(i12))
        list.add(toJava(i13))
        list.add(toJava(i14))
        list.add(toJava(i15))
        list.add(toJava(i16))
      };
      case Tuple17(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef, i11: AnyRef, i12: AnyRef, i13: AnyRef,
      i14: AnyRef, i15: AnyRef, i16: AnyRef, i17: AnyRef) => {
        list.add(toJava(i1))
        list.add(toJava(i2))
        list.add(toJava(i3))
        list.add(toJava(i4))
        list.add(toJava(i5))
        list.add(toJava(i6))
        list.add(toJava(i7))
        list.add(toJava(i8))
        list.add(toJava(i9))
        list.add(toJava(i11))
        list.add(toJava(i12))
        list.add(toJava(i13))
        list.add(toJava(i14))
        list.add(toJava(i15))
        list.add(toJava(i16))
        list.add(toJava(i17))
      };
      case Tuple18(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef, i11: AnyRef, i12: AnyRef, i13: AnyRef,
      i14: AnyRef, i15: AnyRef, i16: AnyRef, i17: AnyRef, i18: AnyRef) => {
        list.add(toJava(i1))
        list.add(toJava(i2))
        list.add(toJava(i3))
        list.add(toJava(i4))
        list.add(toJava(i5))
        list.add(toJava(i6))
        list.add(toJava(i7))
        list.add(toJava(i8))
        list.add(toJava(i9))
        list.add(toJava(i11))
        list.add(toJava(i12))
        list.add(toJava(i13))
        list.add(toJava(i14))
        list.add(toJava(i15))
        list.add(toJava(i16))
        list.add(toJava(i17))
        list.add(toJava(i18))
      };
      case Tuple19(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef, i11: AnyRef, i12: AnyRef, i13: AnyRef,
      i14: AnyRef, i15: AnyRef, i16: AnyRef, i17: AnyRef, i18: AnyRef, i19: AnyRef) => {
        list.add(toJava(i1))
        list.add(toJava(i2))
        list.add(toJava(i3))
        list.add(toJava(i4))
        list.add(toJava(i5))
        list.add(toJava(i6))
        list.add(toJava(i7))
        list.add(toJava(i8))
        list.add(toJava(i9))
        list.add(toJava(i11))
        list.add(toJava(i12))
        list.add(toJava(i13))
        list.add(toJava(i14))
        list.add(toJava(i15))
        list.add(toJava(i16))
        list.add(toJava(i17))
        list.add(toJava(i18))
        list.add(toJava(i19))
      };
      case Tuple20(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef, i11: AnyRef, i12: AnyRef, i13: AnyRef,
      i14: AnyRef, i15: AnyRef, i16: AnyRef, i17: AnyRef, i18: AnyRef, i19: AnyRef, i20: AnyRef)
      => {
        list.add(toJava(i1))
        list.add(toJava(i2))
        list.add(toJava(i3))
        list.add(toJava(i4))
        list.add(toJava(i5))
        list.add(toJava(i6))
        list.add(toJava(i7))
        list.add(toJava(i8))
        list.add(toJava(i9))
        list.add(toJava(i11))
        list.add(toJava(i12))
        list.add(toJava(i13))
        list.add(toJava(i14))
        list.add(toJava(i15))
        list.add(toJava(i16))
        list.add(toJava(i17))
        list.add(toJava(i18))
        list.add(toJava(i19))
        list.add(toJava(i20))
      };
      case Tuple21(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef, i11: AnyRef, i12: AnyRef, i13: AnyRef,
      i14: AnyRef, i15: AnyRef, i16: AnyRef, i17: AnyRef, i18: AnyRef, i19: AnyRef, i20: AnyRef,
      i21: AnyRef) => {
        list.add(toJava(i1))
        list.add(toJava(i2))
        list.add(toJava(i3))
        list.add(toJava(i4))
        list.add(toJava(i5))
        list.add(toJava(i6))
        list.add(toJava(i7))
        list.add(toJava(i8))
        list.add(toJava(i9))
        list.add(toJava(i11))
        list.add(toJava(i12))
        list.add(toJava(i13))
        list.add(toJava(i14))
        list.add(toJava(i15))
        list.add(toJava(i16))
        list.add(toJava(i17))
        list.add(toJava(i18))
        list.add(toJava(i19))
        list.add(toJava(i20))
        list.add(toJava(i21))
      };
      case Tuple22(i1: AnyRef, i2: AnyRef, i3: AnyRef, i4: AnyRef, i5: AnyRef, i6: AnyRef,
      i7: AnyRef, i8: AnyRef, i9: AnyRef, i10: AnyRef, i11: AnyRef, i12: AnyRef, i13: AnyRef,
      i14: AnyRef, i15: AnyRef, i16: AnyRef, i17: AnyRef, i18: AnyRef, i19: AnyRef, i20: AnyRef,
      i21: AnyRef, i22: AnyRef) => {
        list.add(toJava(i1))
        list.add(toJava(i2))
        list.add(toJava(i3))
        list.add(toJava(i4))
        list.add(toJava(i5))
        list.add(toJava(i6))
        list.add(toJava(i7))
        list.add(toJava(i8))
        list.add(toJava(i9))
        list.add(toJava(i11))
        list.add(toJava(i12))
        list.add(toJava(i13))
        list.add(toJava(i14))
        list.add(toJava(i15))
        list.add(toJava(i16))
        list.add(toJava(i17))
        list.add(toJava(i18))
        list.add(toJava(i19))
        list.add(toJava(i20))
        list.add(toJava(i21))
        list.add(toJava(i22))
      };
      case _ =>
        throw new IOException("Only scala tuple is allowed")

    }
  }
}
