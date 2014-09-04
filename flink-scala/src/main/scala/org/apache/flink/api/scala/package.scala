/**
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

package org.apache.flink.api

import _root_.scala.reflect.ClassTag
import language.experimental.macros
import org.apache.flink.types.TypeInformation
import org.apache.flink.api.scala.typeutils.TypeUtils
import org.apache.flink.api.java.{DataSet => JavaDataSet}

package object scala {
  // We have this here so that we always have generated TypeInformationS when
  // using the Scala API
  implicit def createTypeInformation[T]: TypeInformation[T] = macro TypeUtils.createTypeInfo[T]

  // We need to wrap Java DataSet because we need the scala operations
  private[flink] def wrap[R: ClassTag](set: JavaDataSet[R]) = new DataSet[R](set)
}