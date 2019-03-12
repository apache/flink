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
import org.apache.flink.runtime.io.network.api.writer.{RecordWriter, RecordWriterBuilder}
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable
import org.apache.flink.types.IntValue

object Tasks {

  class Forwarder(environment: Environment)
    extends AbstractInvokable(environment) {

    override def invoke(): Unit = {
      val reader = new RecordReader[IntValue](
        getEnvironment.getInputGate(0),
        classOf[IntValue],
        getEnvironment.getTaskManagerInfo.getTmpDirectories)
      
      val writer = new RecordWriterBuilder().build(
        getEnvironment.getWriter(0)).asInstanceOf[RecordWriter[IntValue]]

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
