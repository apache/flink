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

package org.apache.flink.runtime.jobmanager

import org.apache.flink.runtime.io.network.api.{RecordReader, RecordWriter}
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable
import org.apache.flink.runtime.types.IntegerRecord

object Tasks {
  class BlockingNoOpInvokable extends AbstractInvokable {
    override def registerInputOutput(): Unit = {}

    override def invoke(): Unit = {
      val o = new Object()
      o.synchronized{
        o.wait()
      }
    }
  }

  class NoOpInvokable extends AbstractInvokable{
    override def registerInputOutput(): Unit = {}

    override def invoke(): Unit = {}
  }

  class WaitingNoOpInvokable extends AbstractInvokable{
    val waitingTime = 100L

    override def registerInputOutput(): Unit = {}

    override def invoke(): Unit = {
      Thread.sleep(waitingTime)
    }
  }

  class Sender extends AbstractInvokable{
    var writer: RecordWriter[IntegerRecord] = _
    override def registerInputOutput(): Unit = {
      writer = new RecordWriter[IntegerRecord](this)
    }

    override def invoke(): Unit = {
      try{
        writer.initializeSerializers()
        writer.emit(new IntegerRecord(42))
        writer.emit(new IntegerRecord(1337))
        writer.flush()
      }finally{
        writer.clearBuffers()
      }
    }
  }

  class Receiver extends AbstractInvokable {
    var reader: RecordReader[IntegerRecord] = _

    override def registerInputOutput(): Unit = {
      reader = new RecordReader[IntegerRecord](this, classOf[IntegerRecord])
    }

    override def invoke(): Unit = {
      val i1 = reader.next()
      val i2 = reader.next()
      val i3 = reader.next()

      if(i1.getValue != 42 || i2.getValue != 1337 || i3 != null){
        throw new Exception("Wrong data received.")
      }
    }
  }

  class FailingOnceReceiver extends Receiver {
    import FailingOnceReceiver.failed

    override def invoke(): Unit = {
      if(!failed && getEnvironment.getIndexInSubtaskGroup == 0){
        failed = true
        throw new Exception("Test exception.")
      }else{
        super.invoke()
      }
    }
  }

  object FailingOnceReceiver{
    var failed = false
  }

  class BlockingOnceReceiver extends Receiver {

  }

  object BlockingOnceReceiver{
    var blocking = true
  }

  class AgnosticReceiver extends AbstractInvokable {
    var reader: RecordReader[IntegerRecord] = _

    override def registerInputOutput(): Unit = {
      reader = new RecordReader[IntegerRecord](this, classOf[IntegerRecord])
    }

    override def invoke(): Unit = {
      while(reader.next() != null){}
    }
  }

  class AgnosticBinaryReceiver extends AbstractInvokable {
    var reader1: RecordReader[IntegerRecord] = _
    var reader2: RecordReader[IntegerRecord] = _

    override def registerInputOutput(): Unit = {
      reader1 = new RecordReader[IntegerRecord](this, classOf[IntegerRecord])
      reader2 = new RecordReader[IntegerRecord](this, classOf[IntegerRecord])
    }

    override def invoke(): Unit = {
      while(reader1.next() != null){}
      while(reader2.next() != null){}
    }
  }

  class ExceptionSender extends AbstractInvokable{
    var writer: RecordWriter[IntegerRecord] = _

    override def registerInputOutput(): Unit = {
      writer = new RecordWriter[IntegerRecord](this)
    }

    override def invoke(): Unit = {
      writer.initializeSerializers()

      throw new Exception("Test exception")
    }
  }

  class SometimesExceptionSender extends AbstractInvokable {
    var writer: RecordWriter[IntegerRecord] = _

    override def registerInputOutput(): Unit = {
      writer = new RecordWriter[IntegerRecord](this)
    }

    override def invoke(): Unit = {
      writer.initializeSerializers()

      if(Math.random() < 0.05){
        throw new Exception("Test exception")
      }else{
        val o = new Object()
        o.synchronized(o.wait())
      }
    }
  }

  class ExceptionReceiver extends AbstractInvokable {
    override def registerInputOutput(): Unit = {
      new RecordReader[IntegerRecord](this, classOf[IntegerRecord])
    }

    override def invoke(): Unit = {
      throw new Exception("Test exception")
    }
  }

  class InstantiationErrorSender extends AbstractInvokable{
    throw new RuntimeException("Test exception in constructor")

    override def registerInputOutput(): Unit = {
    }

    override def invoke(): Unit = {
    }
  }

  class SometimesInstantiationErrorSender extends AbstractInvokable{
    if(Math.random < 0.05){
      throw new RuntimeException("Test exception in constructor")
    }

    override def registerInputOutput(): Unit = {
      new RecordWriter[IntegerRecord](this)
    }

    override def invoke(): Unit = {
      val o = new Object()
      o.synchronized(o.wait())
    }
  }

  class BlockingReceiver extends AbstractInvokable {
    override def registerInputOutput(): Unit = {
      new RecordReader[IntegerRecord](this, classOf[IntegerRecord])
    }

    override def invoke(): Unit = {
      val o = new Object
      o.synchronized(
        o.wait()
      )
    }
  }
}
