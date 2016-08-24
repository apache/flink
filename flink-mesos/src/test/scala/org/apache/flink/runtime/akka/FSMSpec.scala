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

package org.apache.flink.runtime.akka

import java.util

import org.mockito.ArgumentMatcher
import org.scalatest.WordSpecLike

import scala.collection.JavaConverters._

/**
  * Extends wordspec with FSM functionality.
  */
abstract class FSMSpec extends FSMSpecLike {
}

/**
  * Implementation trait for class <code>FSMSpec</code>, which extends wordspec
  * with FSM functionality.
  *
  * For example: "MyFSM" when inState {
  *   "Connected" should handle {
  *     "Disconnect" which {
  *       "transitions to Disconnected" in (pending)
  *     }
  *   }
  * }
  *
  */
abstract trait FSMSpecLike extends WordSpecLike {
  /**
    * After word to describe the states that an FSM may be in.
    */
  def inState = afterWord("in state")

  /**
    * After word to describe the events that an FSM may handle in a given state.
    * @return
    */
  def handle = afterWord("handle")
}

