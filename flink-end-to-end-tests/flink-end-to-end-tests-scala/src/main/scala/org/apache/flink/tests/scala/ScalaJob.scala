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
package org.apache.flink.tests.scala

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import scala.runtime.BoxesRunTime

/**
 * A Scala job that can only run with Scala 2.11.
 *
 * <p>This job also acts as a stand-on for Java jobs using some Scala library.
 */
object ScalaJob {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    // we want to go through serialization to check for kryo issues
    env.disableOperatorChaining()

    env.fromElements(new NonPojo()).map(new MapFunction[NonPojo, NonPojo] {
      override def map(value: NonPojo): NonPojo = {
        // use some method that was removed in 2.12+
        BoxesRunTime.hashFromNumber(value.getSomeInt)
        value
      }
    })

    env.execute();
  }
}
