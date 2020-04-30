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
package org.apache.flink.api.scala.hadoop.mapred

import org.apache.flink.annotation.Public
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormatBase
import org.apache.hadoop.mapred.{InputFormat, JobConf}

@Public
class HadoopInputFormat[K, V](
    mapredInputFormat: InputFormat[K, V],
    keyClass: Class[K],
    valueClass: Class[V],
    job: JobConf)
  extends HadoopInputFormatBase[K, V, (K, V)](mapredInputFormat, keyClass, valueClass, job) {

  def this(mapredInputFormat: InputFormat[K, V], keyClass: Class[K], valueClass: Class[V]) = {
    this(mapredInputFormat, keyClass, valueClass, new JobConf)
  }

  def nextRecord(reuse: (K, V)): (K, V) = {
    if (!fetched) {
      fetchNext()
    }
    if (!hasNext) {
      null
    } else {
      fetched = false
      (key, value)
    }
  }

}
