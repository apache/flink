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

package org.apache.flink.api.table.input

import org.apache.flink.api.java.tuple.Tuple1
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

class DummyTableSourceWithoutPushdown extends StaticTableSource {

  override def getOutputFieldNames(): Seq[String] = Seq("field")

  override def createStaticDataStream(env: StreamExecutionEnvironment): DataStream[_] = null

  override def createStaticDataSet(env: ExecutionEnvironment): DataSet[_] = {
    env.fromElements(
      new Tuple1[String]("A"),
      new Tuple1[String]("B"),
      new Tuple1[String]("C"),
      new Tuple1[String]("D"))
  }

}
