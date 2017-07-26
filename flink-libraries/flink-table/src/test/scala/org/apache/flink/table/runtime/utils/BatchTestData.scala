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

package org.apache.flink.table.runtime.utils

import java.sql.Timestamp

import org.apache.flink.api.scala._

import scala.collection.mutable
import scala.util.Random

object BatchTestData {

  def getSmall3TupleTimestampDataSet(env: ExecutionEnvironment)
      : DataSet[(Int, Timestamp, String)] = {

    val data = new mutable.MutableList[(Int, Timestamp, String)]
    data.+=((1, Timestamp.valueOf("1984-07-05 23:01:01.105"), "Hi"))
    data.+=((2, Timestamp.valueOf("1972-02-22 07:12:00.333"), "Hello"))
    data.+=((3, Timestamp.valueOf("1938-04-22 06:55:44.55"), "Hello world"))
    env.fromCollection(Random.shuffle(data))
  }

  def getSmall2NestedTupleDataSet(env: ExecutionEnvironment)
      : DataSet[((Int, Int), String, (Int, Int))] = {

    val data = new mutable.MutableList[((Int, Int), String, (Int, Int))]
    data.+=(((1, 1), "one", (2, 2)))
    data.+=(((2, 2), "two", (3, 3)))
    data.+=(((3, 3), "three", (3, 3)))
    env.fromCollection(Random.shuffle(data))
  }
}
