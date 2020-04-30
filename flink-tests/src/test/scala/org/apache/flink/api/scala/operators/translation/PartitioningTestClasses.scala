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

package org.apache.flink.api.scala.operators.translation

import org.apache.flink.api.common.functions.Partitioner



class TestPartitionerInt extends Partitioner[Int] {
  override def partition(key: Int, numPartitions: Int): Int = 0
}

class TestPartitionerLong extends Partitioner[Long] {
  override def partition(key: Long, numPartitions: Int): Int = 0
}

class Pojo2 {

  var a: Int = _
  var b: Int = _
}

class Pojo3 {

  var a: Int = _
  var b: Int = _
  var c: Int = _
}
