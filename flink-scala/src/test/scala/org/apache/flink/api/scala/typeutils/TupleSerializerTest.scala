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

package org.apache.flink.api.scala.typeutils

import org.apache.flink.api.common.typeutils._
import org.apache.flink.api.common.typeutils.base._

import scala.util.Random

case class CaseClass(a: Int, b: String, c: (Long, Byte))

class TupleSerializerTest
  extends SerializerTestBase[(Int, Long, String, Option[Int], (Int, CaseClass))] {

  override protected def createSerializer():
  TypeSerializer[(Int, Long, String, Option[Int], (Int, CaseClass))] =
    new CaseClassSerializer[(Int, Long, String, Option[Int], (Int, CaseClass))](
      classOf[(Int, Long, String, Option[Int], (Int, CaseClass))],
      Array(
        new IntSerializer(),
        new LongSerializer(),
        new StringSerializer(),
        new OptionSerializer[Int](new IntSerializer().asInstanceOf[TypeSerializer[Int]]),
        new CaseClassSerializer[(Int, CaseClass)](
          classOf[(Int, CaseClass)],
          Array(
            new IntSerializer(),
            new CaseClassSerializer[CaseClass](
              classOf[CaseClass],
              Array(
                new IntSerializer(),
                new StringSerializer(),
                new CaseClassSerializer[(Long, Byte)](
                  classOf[(Long, Byte)],
                  Array(
                    new LongSerializer(),
                    new ByteSerializer()
                  )
                ) {
                  override def createInstance(fields: Array[AnyRef]): (Long, Byte) = (
                    fields(0).asInstanceOf[Long],
                    fields(1).asInstanceOf[Byte]
                  )
                }
              )
            ) {
              override def createInstance(fields: Array[AnyRef]): CaseClass = CaseClass(
                fields(0).asInstanceOf[Int],
                fields(1).asInstanceOf[String],
                fields(2).asInstanceOf[(Long, Byte)]
              )
            }
          )
        ) {
          override def createInstance(fields: Array[AnyRef]): (Int, CaseClass) = (
            fields(0).asInstanceOf[Int],
            fields(1).asInstanceOf[CaseClass]
          )
        }
      )
    ) {
      override def createInstance(fields: Array[AnyRef]):
      (Int, Long, String, Option[Int], (Int, CaseClass)) = (
        fields(0).asInstanceOf[Int],
        fields(1).asInstanceOf[Long],
        fields(2).asInstanceOf[String],
        fields(3).asInstanceOf[Option[Int]],
        fields(4).asInstanceOf[(Int, CaseClass)]
      )
    }

  override protected def getLength: Int = -1

  override protected def getTypeClass:
  Class[(Int, Long, String, Option[Int], (Int, CaseClass))] =
    classOf[(Int, Long, String, Option[Int], (Int, CaseClass))]

  override protected def getTestData:
  Array[(Int, Long, String, Option[Int], (Int, CaseClass))] = {

    val rnd = new Random(874597969123412341L)
    Array[(Int, Long, String, Option[Int], (Int, CaseClass))](
      (rnd.nextInt(), rnd.nextLong(), rnd.nextDouble().toString,
        Option(rnd.nextInt()), (rnd.nextInt(), CaseClass(
        rnd.nextInt(), rnd.nextDouble().toString,
        (rnd.nextLong(), rnd.nextInt().toByte)
      ))),
      (rnd.nextInt(), rnd.nextLong(), rnd.nextDouble().toString,
        Option(rnd.nextInt()), (rnd.nextInt(), CaseClass(
        rnd.nextInt(), null, null
      ))),
      (rnd.nextInt(), rnd.nextLong(), rnd.nextDouble().toString,
        Option(rnd.nextInt()), (rnd.nextInt(), null)),
      (rnd.nextInt(), rnd.nextLong(), rnd.nextDouble().toString, null, null),
      (rnd.nextInt(), rnd.nextLong(), null, null, null),
      null
    )
  }
}
