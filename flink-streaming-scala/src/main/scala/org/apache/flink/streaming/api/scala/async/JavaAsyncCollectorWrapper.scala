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

package org.apache.flink.streaming.api.scala.async

import org.apache.flink.annotation.Internal
import org.apache.flink.streaming.api.functions.async.collector.{AsyncCollector => JavaAsyncCollector}

import scala.collection.JavaConverters._

/**
  * Internal wrapper class to map a Flink's Java API [[JavaAsyncCollector]] to a Scala
  * [[AsyncCollector]].
  *
  * @param javaAsyncCollector to forward the calls to
  * @tparam OUT type of the output elements
  */
@Internal
class JavaAsyncCollectorWrapper[OUT](val javaAsyncCollector: JavaAsyncCollector[OUT])
  extends AsyncCollector[OUT] {
  override def collect(result: Iterable[OUT]): Unit = {
    javaAsyncCollector.collect(result.asJavaCollection)
  }

  override def collect(throwable: Throwable): Unit = {
    javaAsyncCollector.collect(throwable)
  }
}
