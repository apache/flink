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

package org.apache.flink.streaming.scala.examples.wordcount.util

import org.apache.flink.api.common.{ExecutionConfig, RuntimeExecutionMode}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.examples.wordcount.util.{CLI => JCLI}

import java.time.Duration
import java.util
import java.util.Optional

/**
 * A simple CLI parser for the [[org.apache.flink.streaming.scala.examples.wordcount.WordCount]]
 * example application.
 */
object CLI {
  def fromArgs(args: Array[String]) = new CLI(JCLI.fromArgs(args))
}

class CLI private (val inner: JCLI) extends ExecutionConfig.GlobalJobParameters {

  def input: Option[Array[Path]] = asScala(inner.getInputs)

  def discoveryInterval: Option[Duration] = asScala(inner.getDiscoveryInterval)

  def output: Option[Path] = asScala(inner.getOutput)

  def executionMode: RuntimeExecutionMode = inner.getExecutionMode

  def getInt(key: String): Option[Int] = {
    val result = inner.getInt(key)
    if (result.isPresent) {
      Option(result.getAsInt)
    } else {
      None
    }
  }

  override def equals(obj: Any): Boolean =
    obj.isInstanceOf[CLI] && inner.equals(obj.asInstanceOf[CLI].inner)

  override def hashCode(): Int = inner.hashCode()

  override def toMap: util.Map[String, String] = inner.toMap

  private def asScala[T](optional: Optional[T]): Option[T] =
    Option(optional.orElse(null.asInstanceOf[T]))
}
