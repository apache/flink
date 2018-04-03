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

package org.apache.flink.tests.streaming

import java.io.{ByteArrayOutputStream, File}
import java.nio.ByteBuffer

import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import collection.JavaConversions._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.util.Random

object AvroClassLoadingTest {
  def main(args: Array[String]) {

    def serialize(user: User): Array[Byte] = {
      val out = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().binaryEncoder(out, null)
      val writer = new SpecificDatumWriter[User](User.getClassSchema)

      writer.write(user, encoder)
      encoder.flush()
      out.close()

      out.toByteArray
    }

    def deserialize(bytes: Array[Byte]): User = {
      val reader = new SpecificDatumReader[User](User.getClassSchema)
      val decoder = DecoderFactory.get.binaryDecoder(bytes, null)
      reader.read(null, decoder)
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.seconds(5)))

    val usersStream = env.addSource(new SourceFunction[Array[Byte]]() {
      var running = true

      override def run(ctx: SourceFunction.SourceContext[Array[Byte]]): Unit = {
        val rand = new Random()
        val entries = Array(
          Tuple14(
            null,
            true,
            24,
            1,
            0.1f,
            0.001,
            randomByteArray(),
            "Jon",
            "Snow",
            possible_option.male,
            seqAsJavaList(Seq[CharSequence]("Facebook", "Twitter")),
            mapAsJavaMap(Map[CharSequence, java.lang.Long]("John" -> 100l, "John1" -> 101l)),
            "lala",
            randomByteArray()
          ),
          Tuple14(
            null,
            false,
            -10394,
            -1,
            0.1f,
            0.001,
            randomByteArray(),
            "God",
            "Almighty",
            possible_option.other,
            seqAsJavaList(Seq[CharSequence]("Instagram", "Snapchat")),
            mapAsJavaMap(Map[CharSequence, java.lang.Long]("BestScore" -> 100l, "WorstScore" -> -100l)),
            "something",
            randomByteArray()
          ),
          Tuple14(
            null,
            true,
            0,
            0,
            0.0f,
            0.0,
            randomByteArray(),
            "Flink",
            "Squirrel",
            possible_option.female,
            seqAsJavaList(Seq[CharSequence]()),
            mapAsJavaMap(Map[CharSequence, java.lang.Long]()),
            null,
            randomByteArray()
          )
        )
        while (running) {
          val entry = entries(rand.nextInt(entries.length))
          val user = new User(
            entry._1,
            entry._2,
            entry._3,
            entry._4,
            entry._5,
            entry._6,
            ByteBuffer.wrap(entry._7),
            entry._8,
            entry._9,
            entry._10,
            entry._11,
            entry._12,
            entry._13,
            new hash(entry._14)
          )
          ctx.collect(serialize(user))
          Thread.sleep(1000)
        }
      }

      override def cancel(): Unit = running = false
    })

    usersStream
      .map { bytes =>
        if (new File("/tmp/die").exists()) {
          throw new RuntimeException("/tmp/die exists!")
        }

        bytes
      }
      .map { bytes =>
        val user = deserialize(bytes)
        s"Hello, ${user.getFirstname} ${user.getLastname}!"
      }
      .print()

    env.execute()
  }

  def randomByteArray(): Array[Byte] = {
    val array = new Array[Byte](100)
    new Random().nextBytes(array)
    return array
  }
}

