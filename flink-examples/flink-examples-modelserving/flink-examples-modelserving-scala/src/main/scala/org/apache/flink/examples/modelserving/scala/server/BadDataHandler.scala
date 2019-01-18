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

package org.apache.flink.examples.modelserving.scala.server

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

import scala.util.{Failure, Success, Try}

object BadDataHandler {
  def apply[T] = new BadDataHandler[T]
}

/**
  * Implementation of Data handler for bad data.
  */
class BadDataHandler[T] extends FlatMapFunction[Try[T], T] {

  /**
    * Process data.
    *
    * @param t data.
    * @param out Data collector.
    */
  override def flatMap(t: Try[T], out: Collector[T]): Unit = {
    t match {
      case Success(t) => out.collect(t)
      case Failure(e) => println(s"BAD DATA: ${e.getMessage}")
    }
  }
}
