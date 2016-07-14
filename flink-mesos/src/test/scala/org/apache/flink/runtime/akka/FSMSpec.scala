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
  * Implementation trait for class <code>FSMSpec</code>, which extends wordspec with FSM functionality.
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

