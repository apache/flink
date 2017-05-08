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

package org.apache.flink.api.scala

import java.io.BufferedReader

import _root_.scala.tools.nsc.interpreter._
import _root_.scala.io.AnsiColor.{MAGENTA, RESET}

class ILoopCompat(
                   in0: Option[BufferedReader],
                   out0: JPrintWriter)
  extends ILoop(in0, out0) {

  override def prompt = {
    val promptStr = "Scala-Flink> "
    s"$MAGENTA$promptStr$RESET"
  }

  protected def addThunk(f: => Unit): Unit = f
}
