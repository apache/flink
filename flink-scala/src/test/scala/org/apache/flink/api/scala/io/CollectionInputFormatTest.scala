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
package org.apache.flink.api.scala.io

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.java.io.CollectionInputFormat
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.core.io.GenericInputSplit
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import org.apache.flink.api.scala._
import scala.collection.JavaConverters._

class ElementType(val id: Int) {
  def this() {
    this(-1)
  }

  override def equals(obj: Any): Boolean = {
    if (obj != null && obj.isInstanceOf[ElementType]) {
      val et = obj.asInstanceOf[ElementType]
      et.id == this.id
    }
    else {
      false
    }
  }
}

class CollectionInputFormatTest {

  @Test
  def testSerializability(): Unit = {

    val inputCollection = Seq(new ElementType(1), new ElementType(2), new ElementType(3))
    val info = createTypeInformation[ElementType]

    val inputFormat: CollectionInputFormat[ElementType] = {
      new CollectionInputFormat[ElementType](
        inputCollection.asJava,
        info.createSerializer(new ExecutionConfig))
    }

    val buffer = new ByteArrayOutputStream
    val out = new ObjectOutputStream(buffer)

    out.writeObject(inputFormat)

    val in = new ObjectInputStream(new ByteArrayInputStream(buffer.toByteArray))
    val serializationResult: AnyRef = in.readObject

    assertNotNull(serializationResult)
    assertTrue(serializationResult.isInstanceOf[CollectionInputFormat[_]])

    val result = serializationResult.asInstanceOf[CollectionInputFormat[ElementType]]
    val inputSplit = new GenericInputSplit(0, 1)
    inputFormat.open(inputSplit)
    result.open(inputSplit)

    while (!inputFormat.reachedEnd && !result.reachedEnd) {
      val expectedElement = inputFormat.nextRecord(null)
      val actualElement = result.nextRecord(null)
      assertEquals(expectedElement, actualElement)
    }
  }

  @Test
  def testSerializabilityStrings(): Unit = {
    val data = Seq("To bey or not to be,--that is the question:--",
      "Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune",
      "Or to take arms against a sea of troubles,", "And by opposing end them?--To die," +
        "--to sleep,--", "No more; and by a sleep to say we end", "The heartache, " +
        "and the thousand natural shocks", "That flesh is heir to,--'tis a consummation",
      "Devoutly to be wish'd. To die,--to sleep;--", "To sleep! perchance to dream:--ay, " +
        "there's the rub;", "For in that sleep of death what dreams may come,",
      "When we have shuffled off this mortal coil,", "Must give us pause: there's the respect",
      "That makes calamity of so long life;", "For who would bear the whips and scorns of time,",
      "The oppressor's wrong, the proud man's contumely,", "The pangs of despis'd love, " +
        "the law's delay,", "The insolence of office, and the spurns",
      "That patient merit of the unworthy takes,", "When he himself might his quietus make",
      "With a bare bodkin? who would these fardels bear,", "To grunt and sweat under a weary " +
        "life,", "But that the dread of something after death,--", "The undiscover'd country, " +
        "from whose bourn", "No traveller returns,--puzzles the will,",
      "And makes us rather bear those ills we have", "Than fly to others that we know not of?",
      "Thus conscience does make cowards of us all;", "And thus the native hue of resolution",
      "Is sicklied o'er with the pale cast of thought;", "And enterprises of great pith and " +
        "moment,", "With this regard, their currents turn awry,", "And lose the name of action" +
        ".--Soft you now!", "The fair Ophelia!--Nymph, in thy orisons",
      "Be all my sins remember'd.")

    val inputFormat = new CollectionInputFormat[String](
      data.asJava,
      BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig))
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)

    oos.writeObject(inputFormat)
    oos.close()

    val bais = new ByteArrayInputStream(baos.toByteArray)
    val ois = new ObjectInputStream(bais)
    val result: AnyRef = ois.readObject

    assertTrue(result.isInstanceOf[CollectionInputFormat[_]])
    var i: Int = 0
    val in = result.asInstanceOf[CollectionInputFormat[String]]
    in.open(new GenericInputSplit(0, 1))

    while (!in.reachedEnd) {
      assertEquals(data(i), in.nextRecord(""))
      i += 1
    }
    assertEquals(data.length, i)
  }
}

