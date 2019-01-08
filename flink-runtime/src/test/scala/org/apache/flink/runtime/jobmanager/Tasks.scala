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

import org.apache.flink.runtime.execution.Environment
import org.apache.flink.runtime.io.network.api.reader.RecordReader
import org.apache.flink.runtime.io.network.api.writer.RecordWriter
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable
import org.apache.flink.types.IntValue


object Tasks {

  class Sender(environment: Environment)
    extends AbstractInvokable(environment) {

    override def invoke(): Unit = {
      val writer = new RecordWriter[IntValue](getEnvironment.getWriter(0))

      try{
        writer.emit(new IntValue(42))
        writer.emit(new IntValue(1337))
        writer.flushAll()
      }finally{
        writer.clearBuffers()
      }
    }
  }

  class Forwarder(environment: Environment)
    extends AbstractInvokable(environment) {

    override def invoke(): Unit = {
      val reader = new RecordReader[IntValue](
        getEnvironment.getInputGate(0),
        classOf[IntValue],
        getEnvironment.getTaskManagerInfo.getTmpDirectories)
      
      val writer = new RecordWriter[IntValue](getEnvironment.getWriter(0))

      try {
        while (true) {
          val record = reader.next()

          if (record == null) {
            return
          }

          writer.emit(record)
        }

        writer.flushAll()
      } finally {
        writer.clearBuffers()
      }
    }
  }

  class Receiver(environment: Environment)
    extends AbstractInvokable(environment) {

    override def invoke(): Unit = {
      val reader = new RecordReader[IntValue](
        getEnvironment.getInputGate(0),
        classOf[IntValue],
        getEnvironment.getTaskManagerInfo.getTmpDirectories)

      val i1 = reader.next()
      val i2 = reader.next()
      val i3 = reader.next()

      if(i1.getValue != 42 || i2.getValue != 1337 || i3 != null){
        throw new Exception("Wrong data received.")
      }
    }
  }

  class FailingOnceReceiver(environment: Environment)
    extends Receiver(environment) {
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

  class BlockingOnceReceiver(environment: Environment)
    extends Receiver(environment) {
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

  class AgnosticReceiver(environment: Environment)
    extends AbstractInvokable(environment) {

    override def invoke(): Unit = {
      val reader= new RecordReader[IntValue](
        getEnvironment.getInputGate(0),
        classOf[IntValue],
        getEnvironment.getTaskManagerInfo.getTmpDirectories)

      while(reader.next() != null){}
    }
  }

  class AgnosticBinaryReceiver(environment: Environment)
    extends AbstractInvokable(environment) {

    override def invoke(): Unit = {
      val reader1 = new RecordReader[IntValue](
        getEnvironment.getInputGate(0),
        classOf[IntValue],
        getEnvironment.getTaskManagerInfo.getTmpDirectories)
      
      val reader2 = new RecordReader[IntValue](
        getEnvironment.getInputGate(1),
        classOf[IntValue],
        getEnvironment.getTaskManagerInfo.getTmpDirectories)

      while(reader1.next() != null){}
      while(reader2.next() != null){}
    }
  }

  class AgnosticTertiaryReceiver(environment: Environment)
    extends AbstractInvokable(environment) {

    override def invoke(): Unit = {
      val env = getEnvironment

      val reader1 = new RecordReader[IntValue](
        env.getInputGate(0),
        classOf[IntValue],
        getEnvironment.getTaskManagerInfo.getTmpDirectories)
      
      val reader2 = new RecordReader[IntValue](
        env.getInputGate(1),
        classOf[IntValue],
        getEnvironment.getTaskManagerInfo.getTmpDirectories)
      
      val reader3 = new RecordReader[IntValue](
        env.getInputGate(2),
        classOf[IntValue],
        getEnvironment.getTaskManagerInfo.getTmpDirectories)

      while(reader1.next() != null){}
      while(reader2.next() != null){}
      while(reader3.next() != null){}
    }
  }

  class ExceptionSender(environment: Environment)
    extends AbstractInvokable(environment) {

    override def invoke(): Unit = {
      throw new Exception("Test exception")
    }
  }

  class SometimesExceptionSender(environment: Environment)
    extends AbstractInvokable(environment) {

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

  class ExceptionReceiver(environment: Environment)
    extends AbstractInvokable(environment) {

    override def invoke(): Unit = {
      throw new Exception("Test exception")
    }
  }

  class InstantiationErrorSender(environment: Environment)
    extends AbstractInvokable(environment) {
    throw new RuntimeException("Test exception in constructor")

    override def invoke(): Unit = {
    }
  }

  class SometimesInstantiationErrorSender(environment: Environment)
    extends AbstractInvokable(environment) {

    // this only works if the TaskManager runs in the same JVM as the test case
    if(SometimesInstantiationErrorSender.failingSenders.contains(this.getIndexInSubtaskGroup)){
      throw new RuntimeException("Test exception in constructor")
    }

    override def invoke(): Unit = {
      val o = new Object()
      o.synchronized(o.wait())
    }
  }

  object SometimesInstantiationErrorSender {
    var failingSenders = Set[Int](0)
  }

  class BlockingReceiver(environment: Environment)
    extends AbstractInvokable(environment) {
    override def invoke(): Unit = {
      val o = new Object
      o.synchronized(
        o.wait()
      )
    }
  }
}
