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

import org.apache.flink.runtime.io.network.api.reader.RecordReader
import org.apache.flink.runtime.io.network.api.writer.RecordWriter
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable
import org.apache.flink.types.IntValue


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
    var writer: RecordWriter[IntValue] = _
    override def registerInputOutput(): Unit = {
      writer = new RecordWriter[IntValue](getEnvironment.getWriter(0))
    }

    override def invoke(): Unit = {
      try{
        writer.emit(new IntValue(42))
        writer.emit(new IntValue(1337))
        writer.flush()
      }finally{
        writer.clearBuffers()
      }
    }
  }

  class Forwarder extends AbstractInvokable {
    var reader: RecordReader[IntValue] = _
    var writer: RecordWriter[IntValue] = _
    override def registerInputOutput(): Unit = {
      reader = new RecordReader[IntValue](getEnvironment.getInputGate(0),
        classOf[IntValue])
      writer = new RecordWriter[IntValue](getEnvironment.getWriter(0))
    }

    override def invoke(): Unit = {
      try {
        while (true) {
          val record = reader.next()

          if (record == null) {
            return
          }

          writer.emit(record)
        }

        writer.flush()
      } finally {
        writer.clearBuffers()
      }
    }
  }

  class Receiver extends AbstractInvokable {
    var reader: RecordReader[IntValue] = _

    override def registerInputOutput(): Unit = {
      val env = getEnvironment

      reader = new RecordReader[IntValue](env.getInputGate(0), classOf[IntValue])
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
      if(!failed && getEnvironment.getTaskInfo.getIndexOfThisSubtask == 0){
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
    import BlockingOnceReceiver.blocking

    override def invoke(): Unit = {
      if(blocking) {
        val o = new Object
        o.synchronized{
          o.wait()
        }
      } else {
        super.invoke()
      }
    }

  }

  object BlockingOnceReceiver{
    var blocking = true
  }

  class AgnosticReceiver extends AbstractInvokable {
    var reader: RecordReader[IntValue] = _

    override def registerInputOutput(): Unit = {
      val env = getEnvironment

      reader = new RecordReader[IntValue](env.getInputGate(0), classOf[IntValue])
    }

    override def invoke(): Unit = {
      while(reader.next() != null){}
    }
  }

  class AgnosticBinaryReceiver extends AbstractInvokable {
    var reader1: RecordReader[IntValue] = _
    var reader2: RecordReader[IntValue] = _

    override def registerInputOutput(): Unit = {
      val env = getEnvironment

      reader1 = new RecordReader[IntValue](env.getInputGate(0), classOf[IntValue])
      reader2 = new RecordReader[IntValue](env.getInputGate(1), classOf[IntValue])
    }

    override def invoke(): Unit = {
      while(reader1.next() != null){}
      while(reader2.next() != null){}
    }
  }

  class AgnosticTertiaryReceiver extends AbstractInvokable {
    var reader1: RecordReader[IntValue] = _
    var reader2: RecordReader[IntValue] = _
    var reader3: RecordReader[IntValue] = _

    override def registerInputOutput(): Unit = {
      val env = getEnvironment

      reader1 = new RecordReader[IntValue](env.getInputGate(0), classOf[IntValue])
      reader2 = new RecordReader[IntValue](env.getInputGate(1), classOf[IntValue])
      reader3 = new RecordReader[IntValue](env.getInputGate(2), classOf[IntValue])
    }

    override def invoke(): Unit = {
      while(reader1.next() != null){}
      while(reader2.next() != null){}
      while(reader3.next() != null){}
    }
  }

  class ExceptionSender extends AbstractInvokable{
    var writer: RecordWriter[IntValue] = _

    override def registerInputOutput(): Unit = {
      writer = new RecordWriter[IntValue](getEnvironment.getWriter(0))
    }

    override def invoke(): Unit = {
      throw new Exception("Test exception")
    }
  }

  class SometimesExceptionSender extends AbstractInvokable {
    var writer: RecordWriter[IntValue] = _

    override def registerInputOutput(): Unit = {
      writer = new RecordWriter[IntValue](getEnvironment.getWriter(0))
    }

    override def invoke(): Unit = {
      // this only works if the TaskManager runs in the same JVM as the test case
      if(SometimesExceptionSender.failingSenders.contains(this.getIndexInSubtaskGroup)){
        throw new Exception("Test exception")
      }else{
        val o = new Object()
        o.synchronized(o.wait())
      }
    }
  }

  object SometimesExceptionSender {
    var failingSenders = Set[Int](0)
  }

  class ExceptionReceiver extends AbstractInvokable {
    override def registerInputOutput(): Unit = {
      new RecordReader[IntValue](getEnvironment.getInputGate(0), classOf[IntValue])
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

    // this only works if the TaskManager runs in the same JVM as the test case
    if(SometimesInstantiationErrorSender.failingSenders.contains(this.getIndexInSubtaskGroup)){
      throw new RuntimeException("Test exception in constructor")
    }

    override def registerInputOutput(): Unit = {
      new RecordWriter[IntValue](getEnvironment.getWriter(0))
    }

    override def invoke(): Unit = {
      val o = new Object()
      o.synchronized(o.wait())
    }
  }

  object SometimesInstantiationErrorSender {
    var failingSenders = Set[Int](0)
  }

  class BlockingReceiver extends AbstractInvokable {
    override def registerInputOutput(): Unit = {
      new RecordReader[IntValue](getEnvironment.getInputGate(0), classOf[IntValue])
    }

    override def invoke(): Unit = {
      val o = new Object
      o.synchronized(
        o.wait()
      )
    }
  }
}
