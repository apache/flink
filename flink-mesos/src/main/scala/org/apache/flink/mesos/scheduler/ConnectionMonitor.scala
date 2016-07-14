package org.apache.flink.mesos.scheduler

import akka.actor.{Actor, FSM, Props}
import grizzled.slf4j.Logger
import org.apache.flink.configuration.Configuration
import org.apache.flink.mesos.scheduler.ConnectionMonitor._
import org.apache.flink.mesos.scheduler.messages._

import scala.concurrent.duration._

/**
  * Actively monitors the Mesos connection.
  */
class ConnectionMonitor() extends Actor with FSM[FsmState, Unit] {

  val LOG = Logger(getClass)

  startWith(StoppedState, None)

  when(StoppedState) {
    case Event(msg: Start, _) =>
      LOG.info(s"Connecting to Mesos...")
      goto(ConnectingState)
  }

  when(ConnectingState, stateTimeout = CONNECT_RETRY_RATE) {
    case Event(msg: Stop, _) =>
      goto(StoppedState)

    case Event(msg: Registered, _) =>
      LOG.info(s"Connected to Mesos as framework ID ${msg.frameworkId.getValue}.")
      LOG.debug(s"   Master Info: ${msg.masterInfo}")
      goto(ConnectedState)

    case Event(msg: ReRegistered, _) =>
      LOG.info("Reconnected to a new Mesos master.")
      LOG.debug(s"   Master Info: ${msg.masterInfo}")
      goto(ConnectedState)

    case Event(StateTimeout, _) =>
      LOG.warn("Unable to connect to Mesos; still trying...")
      stay()
  }

  when(ConnectedState) {
    case Event(msg: Stop, _) =>
      goto(StoppedState)

    case Event(msg: Disconnected, _) =>
      LOG.warn("Disconnected from the Mesos master.  Reconnecting...")
      goto(ConnectingState)
  }

  onTransition {
    case previousState -> nextState =>
      LOG.debug(s"State change ($previousState -> $nextState) with data ${nextStateData}")
  }

  initialize()
}

object ConnectionMonitor {

  val CONNECT_RETRY_RATE = (5 seconds)

  // ------------------------------------------------------------------------
  // State
  // ------------------------------------------------------------------------

  /**
    * An FSM state of the connection monitor.
    */
  sealed trait FsmState
  case object StoppedState extends FsmState
  case object ConnectingState extends FsmState
  case object ConnectedState extends FsmState

  // ------------------------------------------------------------------------
  //  Messages
  // ------------------------------------------------------------------------

  /**
    * Starts the connection monitor.
    */
  case class Start()

  /**
    * Stops the connection monitor.
    */
  case class Stop()

  // ------------------------------------------------------------------------
  //  Utils
  // ------------------------------------------------------------------------

  /**
    * Creates the properties for the ConnectionMonitor actor.
    *
    * @param actorClass the connection monitor actor class
    * @param flinkConfig the Flink configuration.
    * @tparam T the type of the connection monitor actor class
    * @return the Props to create the connection monitor
    */
  def createActorProps[T <: ConnectionMonitor](actorClass: Class[T],
     flinkConfig: Configuration): Props = {
    Props.create(actorClass)
  }
}
