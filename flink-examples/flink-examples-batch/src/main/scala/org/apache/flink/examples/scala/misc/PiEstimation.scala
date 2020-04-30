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

package org.apache.flink.examples.scala.misc

import org.apache.flink.api.scala._

object PiEstimation {

  def main(args: Array[String]) {

    val numSamples: Long = if (args.length > 0) args(0).toLong else 1000000

    val env = ExecutionEnvironment.getExecutionEnvironment

    // count how many of the samples would randomly fall into
    // the upper right quadrant of the unit circle
    val count =
      env.generateSequence(1, numSamples)
        .map  { sample =>
          val x = Math.random()
          val y = Math.random()
          if (x * x + y * y < 1) 1L else 0L
        }
        .reduce(_ + _)

    // ratio of samples in upper right quadrant vs total samples gives surface of upper
    // right quadrant, times 4 gives surface of whole unit circle, i.e. PI
    val pi = count
      .map ( _ * 4.0 / numSamples)

    println("We estimate Pi to be:")

    pi.print()
  }

}
