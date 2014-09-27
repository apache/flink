/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api

import _root_.scala.reflect.ClassTag
import language.experimental.macros
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.{CaseClassTypeInfo, TypeUtils}
import org.apache.flink.api.java.{DataSet => JavaDataSet}

/**
 * The Flink Scala API. [[org.apache.flink.api.scala.ExecutionEnvironment]] is the starting-point
 * of any Flink program. It can be used to read from local files, HDFS, or other sources.
 * [[org.apache.flink.api.scala.DataSet]] is the main abstraction of data in Flink. It provides
 * operations that create new DataSets via transformations.
 * [[org.apache.flink.api.scala.GroupedDataSet]] provides operations on grouped data that results
 * from [[org.apache.flink.api.scala.DataSet.groupBy()]].
 *
 * Use [[org.apache.flink.api.scala.ExecutionEnvironment.getExecutionEnvironment]] to obtain
 * an execution environment. This will either create a local environment or a remote environment,
 * depending on the context where your program is executing.
 */
package object scala {
  // We have this here so that we always have generated TypeInformationS when
  // using the Scala API
  implicit def createTypeInformation[T]: TypeInformation[T] = macro TypeUtils.createTypeInfo[T]

  // We need to wrap Java DataSet because we need the scala operations
  private[flink] def wrap[R: ClassTag](set: JavaDataSet[R]) = new DataSet[R](set)

  private[flink] def fieldNames2Indices(
      typeInfo: TypeInformation[_],
      fields: Array[String]): Array[Int] = {
    typeInfo match {
      case ti: CaseClassTypeInfo[_] =>
        ti.getFieldIndices(fields)

      case _ =>
        throw new UnsupportedOperationException("Specifying fields by name is only" +
          "supported on Case Classes (for now).")
    }
  }
}
