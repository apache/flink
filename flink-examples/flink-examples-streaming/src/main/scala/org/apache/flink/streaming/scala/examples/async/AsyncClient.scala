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

package org.apache.flink.streaming.scala.examples.async

import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.{ExecutionContext, Future}

/** A simple asynchronous client that simulates interacting with an unreliable external service. */
class AsyncClient {

  def query(key: Int)(implicit executor: ExecutionContext): Future[String] = Future {
    val sleep = (ThreadLocalRandom.current.nextFloat * 100).toLong
    try Thread.sleep(sleep) catch {
      case e: InterruptedException =>
        throw new RuntimeException("AsyncClient was interrupted", e)
    }

    if (ThreadLocalRandom.current.nextFloat < 0.001f) {
      throw new RuntimeException("wahahahaha...")
    } else {
      "key" + (key % 10)
    }
  }
}
