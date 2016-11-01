package org.apache.flink.api.scala.typeutils

import org.apache.flink.api.common.typeutils.base.IntSerializer
import org.apache.flink.api.common.typeutils.{SerializerTestBase, TypeSerializer}

import scala.util.Random

class OptionSerializerTest extends SerializerTestBase[Option[Integer]] {

  override protected def createSerializer(): TypeSerializer[Option[Integer]] =
    new OptionSerializer[Integer](new IntSerializer())

  override protected def getLength: Int = -1

  override protected def getTypeClass: Class[Option[Integer]] = classOf[Option[Integer]]

  override protected def getTestData: Array[Option[Integer]] = {
    val rnd = new Random(874597969123412341L)
    val rndInt = rnd.nextInt

    Array[Option[Integer]](
      Option(Integer.valueOf(0)),
      Option(Integer.valueOf(rndInt)),
      Option(Integer.valueOf(-rndInt)),
      Option.empty,
      null
    )
  }
}
