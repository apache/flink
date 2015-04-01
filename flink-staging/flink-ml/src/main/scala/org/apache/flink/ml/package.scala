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

package org.apache.flink

import org.apache.flink.api.java.operators.DataSink
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.ml.common.LabeledVector

package object ml {

  /** Pimp my [[ExecutionEnvironment]] to directly support `readLibSVM`
    *
    * @param executionEnvironment
    */
  implicit class RichExecutionEnvironment(executionEnvironment: ExecutionEnvironment) {
    def readLibSVM(path: String): DataSet[LabeledVector] = {
      MLUtils.readLibSVM(executionEnvironment, path)
    }
  }

  /** Pimp my [[DataSet]] to directly support `writeAsLibSVM`
    *
    * @param dataSet
    */
  implicit class RichDataSet(dataSet: DataSet[LabeledVector]) {
    def writeAsLibSVM(path: String): DataSink[String] = {
      MLUtils.writeLibSVM(path, dataSet)
    }
  }
}
