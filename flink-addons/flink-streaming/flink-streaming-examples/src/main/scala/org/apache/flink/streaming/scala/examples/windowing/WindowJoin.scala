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

import scala.Stream._
import scala.util.Random
import java.util.concurrent.TimeUnit

object WindowJoin {

  case class Name(id: Long, name: String)
  case class Age(id: Long, age: Int)
  case class Person(name: String, age: Long)

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //Create streams for names and ages by mapping the inputs to the corresponding objects
    val names = env.fromCollection(nameStream).map(x => Name(x._1, x._2))
    val ages = env.fromCollection(ageStream).map(x => Age(x._1, x._2))

    //Join the two input streams by id on the last second every 2 seconds and create new 
    //Person objects containing both name and age
    val joined =
      names.join(ages).onWindow(1, TimeUnit.SECONDS).every(2, TimeUnit.SECONDS)
                      .where("id").equalTo("id") { (n, a) => Person(n.name, a.age) }

    joined print

    env.execute("WindowJoin")
  }

  def nameStream() : Stream[(Long,String)] = {
    def nameMapper(names: Array[String])(x: Int) : (Long, String) =
    {
      if(x%100==0) Thread.sleep(1000)
      (x, names(Random.nextInt(names.length)))
    }
    range(1,10000).map(nameMapper(Array("tom", "jerry", "alice", "bob", "john", "grace")))
  }

  def ageStream() : Stream[(Long,Int)] = {
    def ageMapper(x: Int) : (Long, Int) =
    {
      if(x%100==0) Thread.sleep(1000)
      (x, Random.nextInt(90))
    }
    range(1,10000).map(ageMapper)
  }

}
