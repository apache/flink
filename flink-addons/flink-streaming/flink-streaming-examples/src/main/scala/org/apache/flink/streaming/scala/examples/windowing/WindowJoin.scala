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

package org.apache.flink.streaming.scala.examples.windowing

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import scala.util.Random

object WindowJoin {

  case class Name(id: Long, name: String)
  case class Age(id: Long, age: Int)
  case class Person(name: String, age: Long)

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //Create streams for names and ages by mapping the inputs to the corresponding objects
    val names = env.addSource(nameStream _).map(x => Name(x._1, x._2))
    val ages = env.addSource(ageStream _).map(x => Age(x._1, x._2))

    //Join the two input streams by id on the last second and create new Person objects
    //containing both name and age
    val joined =
      names.join(ages).onWindow(1000)
                      .where("id").equalTo("id") { (n, a) => Person(n.name, a.age) }

    joined print

    env.execute("WindowJoin")
  }

  //Stream source for generating (id, name) pairs
  def nameStream(out: Collector[(Long, String)]) = {
    val names = Array("tom", "jerry", "alice", "bob", "john", "grace")

    for (i <- 1 to 10000) {
      if (i % 100 == 0) Thread.sleep(1000) else {
        out.collect((i, names(Random.nextInt(names.length))))
      }
    }
  }

  //Stream source for generating (id, age) pairs
  def ageStream(out: Collector[(Long, Int)]) = {
    for (i <- 1 to 10000) {
      if (i % 100 == 0) Thread.sleep(1000) else {
        out.collect((i, Random.nextInt(90)))
      }
    }
  }

}
