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
package org.apache.flink.table.hive.functions

import java.util

import org.apache.flink.table.functions.ScalarFunction
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.typeinfo.{PrimitiveTypeInfo, TypeInfo, TypeInfoFactory}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory

import scala.annotation.varargs

private[hive] class DeferredObjectAdapter
  extends DeferredObject {

  var ob: AnyRef = _

  def set(ob: AnyRef): Unit = {
    this.ob = ob
  }
  override def prepare(i: Int): Unit = {}
  override def get(): AnyRef = {
    this.ob
  }
}

class HiveGenericUDF(className: String) extends ScalarFunction {

  @transient
  private lazy val functionWrapper = HiveFunctionWrapper(className)

  @transient
  private lazy val function = functionWrapper.createFunction[GenericUDF]()

  @transient
  private var argumentInspectors: Array[ObjectInspector] = _

  @transient
  private var returnInspector: ObjectInspector = _

  @transient
  private var deferredObjects: Array[DeferredObject] = _


  @varargs
  def eval(args: AnyRef*): Any = {
    if (null == argumentInspectors) {
      val typeInfos = new util.ArrayList[TypeInfo]()
      args.foreach(arg => {
        typeInfos.add(TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive(arg.getClass))
      })

      argumentInspectors = new Array[ObjectInspector](typeInfos.size())
      args.zipWithIndex.foreach { case (_, i) =>
        argumentInspectors(i) = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
          typeInfos.get(i).asInstanceOf[PrimitiveTypeInfo])
      }
      function.initializeAndFoldConstants(argumentInspectors)

      deferredObjects = new Array[DeferredObject](args.length)

      var i = 0
      val length = args.length
      while (i < length) {
        val idx = i
        deferredObjects(i).asInstanceOf[DeferredObjectAdapter]
          .set(args(i))
        i += 1
      }
      function.evaluate(deferredObjects)
    }
  }
}
