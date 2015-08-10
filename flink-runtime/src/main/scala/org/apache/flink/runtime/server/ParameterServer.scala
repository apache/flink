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

package org.apache.flink.runtime.server

import java.util.UUID

import _root_.akka.actor._
import _root_.akka.pattern.ask
import grizzled.slf4j.Logger
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.instance.{AkkaActorGateway, ActorGateway, InstanceID}
import org.apache.flink.runtime.messages.InvalidServerAccessException
import org.apache.flink.runtime.messages.JobManagerMessages.{ResponseLeaderSessionID, RequestLeaderSessionID}
import org.apache.flink.runtime.messages.ServerMessages._
import org.apache.flink.runtime.{LeaderSessionMessages, FlinkActor, LogMessages}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps


class ParameterServer extends FlinkActor with LeaderSessionMessages with LogMessages{

  override val log = Logger(getClass)

  private var jobManager: Option[ActorGateway] = None
  private var taskManager: Option[ActorGateway] = None
  private var storeManager: Option[ActorGateway] = None
  private var taskManagerID: Option[InstanceID] = None
  private var heartbeatScheduler: Option[Cancellable] = None
  private var statusQueueCheckScheduler: Option[Cancellable] = None

  // sent by the Job Manager. All messages are forward to the actor gateway according to this map
  private var keyGatewayMapping: mutable.HashMap[String, ActorGateway] = _

  // used to provide fault tolerance. We send all we have to this server too.
  private var redundantPartner: ActorGateway = _

  override val leaderSessionID: Option[UUID] = Some(UUID.randomUUID())

  /**
   * Queue of all messages who we haven't sent back the result of.
   * We index the message by the client id and store the request, client's actor ref, time we last
   * shot off the request to an appropriate server, the attempt number and status of the message
   * whether the server has accepted to process it or not.
   */
  private var waitingClients
    : mutable.HashMap[UUID, (Boolean, ClientRequests, ActorRef, Long, Int)] = _

  /**
   * Maintain where we need to send the reply of a request from another server.
   */
  private var waitingServers: mutable.HashMap[UUID, ActorRef] = _

  /**
   * Run when the parameter server is started. Simply logs an informational message.
   */
  override def preStart(): Unit = {
    log.info(s"Starting ParameterServer at ${self.path.toSerializationFormat}.")
  }

  /**
   * Run post stopping
   */
  override def postStop(): Unit = {

    waitingClients.clear()
    waitingServers.clear()

    if(heartbeatScheduler.nonEmpty) {
      heartbeatScheduler.get.cancel()
      heartbeatScheduler = None
    }
    if(statusQueueCheckScheduler.nonEmpty) {
      statusQueueCheckScheduler.get.cancel()
      statusQueueCheckScheduler = None
    }

    log.info(s"Stopping ParameterServer ${self.path.toSerializationFormat}.")

    log.debug(s"ParameterServer ${self.path} is completely stopped.")
  }

  override def handleMessage: Receive = {
    case message: ClientRequests =>
      // sent from our clients. Forward to appropriate server
      handleRequests(message, sender(), 1)

    case message: ServerRequest =>
      // sent by another server. Check if we can handle that and then handle it.
      handleIncoming(message, sender())

    case message: ServerResponse =>
      // handle reply and forward to client.
      handleResponse(message)

    case message: StoreReply =>
      handleStoreResponse(message)

    case message: ServerAcknowledgement =>
      // mark a message received and agrred to be served to. Or fail.
      handleAcknowledgement(message)

    case TriggerHeartbeat =>
      sendHeartbeat()

    case ServerClearQueueReminder =>
      handleWaitingQueue()

    case ServerRegistrationAcknowledge(keyGatewayMap, copyPartner) =>
      // sent by Job Manager with data about where to forward keys
      keyGatewayMapping = keyGatewayMap
      redundantPartner = copyPartner

    case message: ServerRetry =>
      handleRequests(message.message, message.sender, message.retryNumber)

    case message: KickOffParameterServer =>
      // sent by our task manager
      handleKickoff(message)

    case ServerRegistrationRefuse(error) =>
      // nothing much to do. We can keep doing our work as long as we can. If the Task Manager also
      // lost connection, we'll anyway have to reset our state and register again, if ever.

    case RequestLeaderSessionID =>
      sender() ! ResponseLeaderSessionID(leaderSessionID)
  }

  /**
   * Handle unmatched messages with an exception.
   */
  override def unhandled(message: Any): Unit = {
    // let everyone know we're crashing
    taskManager.get.tell(ServerError(taskManagerID.get, new Exception("Parameter Server failed")))
    jobManager.get.tell(ServerError(taskManagerID.get, new Exception("Parameter Server failed")))
    storeManager.get.tell(ServerError(taskManagerID.get, new Exception("Parameter Server failed")))

    // now crash
    throw new RuntimeException("Received unknown message " + message)
  }

  private def sendHeartbeat(): Unit = {
    // first check connection to the store
    checkConnectionToStore()
    // now send a heartbeat
    jobManager.get.tell(
      ServerHeartbeat(taskManagerID.get, new AkkaActorGateway(self, leaderSessionID)))
  }

  private def checkConnectionToStore(): Unit = {
    try {
      val futureLeaderSessionID =
        (storeManager.get.actor() ? RequestLeaderSessionID)(ParameterServer.STORE_TIMEOUT)
          .mapTo[ResponseLeaderSessionID]
      val leaderSessionID = Await.result(futureLeaderSessionID, ParameterServer.STORE_TIMEOUT)
        .leaderSessionID
      assert(leaderSessionID == storeManager.get.leaderSessionID())
    } catch{
      case e: AnyRef => unhandled(Status.Failure(e))
    }
  }

  private def handleKickoff(message: KickOffParameterServer): Unit = {
    // first check if the store is reachable
    storeManager = Some(message.storeManager)
    checkConnectionToStore()

    // now initialize everything
    jobManager = Some(message.jobManager)
    taskManagerID = Some(message.taskManagerID)
    taskManager = Some(message.taskManager)

    keyGatewayMapping = new mutable.HashMap[String, ActorGateway]()
    waitingClients = new mutable.HashMap[UUID, (Boolean, ClientRequests, ActorRef, Long, Int)]()
    waitingServers = new mutable.HashMap[UUID, ActorRef]()

    // schedule a heartbeat to the job manager
    heartbeatScheduler = Some(context.system.scheduler.schedule(
      ParameterServer.INITIAL_DELAY,
      ParameterServer.HEARTBEAT_INTERVAL,
      self,
      TriggerHeartbeat
    )(context.dispatcher))

    // schedule a reminder to clear off the client requests status reports
    statusQueueCheckScheduler = Some(context.system.scheduler.schedule(
      ParameterServer.RETRY_DELAY,
      ParameterServer.RETRY_DELAY,
      self,
      ServerClearQueueReminder
    )(context.dispatcher))
  }

  private def handleRequests(message: ClientRequests, sender: ActorRef, retry: Int): Unit = {
    // we capture all requests from our clients and forward them to appropriate servers
    if(keyGatewayMapping.get(message.key).isEmpty && !message.isInstanceOf[RegisterClient]){
      sender ! ClientFailure(new Exception("The key has not been registered at the server yet."))
    } else if(keyGatewayMapping.get(message.key).isEmpty && message.isInstanceOf[RegisterClient]){
      // ask the Job Manager for where this key belongs and re-send this message
      jobManager.get.tell(RequestKeyGateway(message.key))
      // we'll retry this
      self ! ServerRetry(message, retry, sender)
    } else{
      // can be successfully sent. Thus, let's send it. First, we need a unique id.
      var messageID = UUID.randomUUID()
      var counter = 0
      while(waitingClients.contains(messageID) && counter < 5){
        messageID = UUID.randomUUID()
        counter = counter + 1
      }
      if(waitingClients.contains(messageID)){
        sender ! ClientFailure(new Exception("A freaky error occurred on the Server."))
      } else{
        // now we can send the message.
        waitingClients.put(messageID, (false, message, sender, System.currentTimeMillis(), retry))
        keyGatewayMapping.get(message.key).get.tell(ServerRequest(messageID, message))
      }
    }
  }

  private def handleIncoming(message: ServerRequest, sender: ActorRef): Unit = {
    // we capture all requests from all servers
    // first, store where the response to this message must be sent.
    waitingServers.put(message.messageID, sender)
    storeManager.get.tell(StoreMessage(message.messageID, message.message))
  }

  private def handleStoreResponse(message: StoreReply): Unit = {
    // forward the response to an appropriate server
    waitingServers.remove(message.messageID).get ! ServerResponse(message.messageID, message.reply)
  }

  private def handleAcknowledgement(message: ServerAcknowledgement): Unit = {
    // all acknowledgement of client requests from respective servers
    val props = waitingClients.get(message.messageID)
    if(props.nonEmpty){
      if(message.state){
        // update the state to be true. We'll wait for a response now.
        waitingClients.put(
          message.messageID, (true, props.get._2, props.get._3, props.get._4, props.get._5))
      } else{
        // server denied the request explicitly. We fail.
        waitingClients.remove(message.messageID)
        // if the failure was due to invalid key access, maybe the Job Manager changed where this
        // key should have gone. Send again. Our key gateway mapping might have been updated by now.
        if(message.error.isInstanceOf[InvalidServerAccessException]){
          self ! ServerRetry(props.get._2, props.get._5, props.get._3)
        } else {
          props.get._3 ! ClientFailure(message.error)
        }
      }
    }
    // otherwise we most likely got the response first and already removed it.
  }

  private def handleResponse(message: ServerResponse): Unit = {
    // handle the responses for every client message we sent.
    // remove from waiting list and send the result back :)
    waitingClients.remove(message.messageID).get._3 ! message.result
  }

  private def handleWaitingQueue(): Unit = {
    // check if there's any message which has timed out. If so, retry if allowed.
    val waiters = waitingClients.keySet
    val currentTime = System.currentTimeMillis()

    waiters.iterator.foreach(
      waiter => {
        val messageProps = waitingClients.get(waiter).get
        // if the status is still false, meaning un-acknowledged, see if we're timed out.
        if(!messageProps._1){
          // timed out
          if(messageProps._4 - currentTime > ParameterServer.RETRY_DELAY.length){
            waitingClients.remove(waiter)
            // if already over the limit in retrying, fail
            if(messageProps._5 == ParameterServer.MAX_RETRIES){
              messageProps._3.tell(
                ClientFailure(new Exception("Connection to the server timed out.")),
                ActorRef.noSender)
            } else{
              // otherwise, retry.
              self ! ServerRetry(messageProps._2, messageProps._5 + 1, messageProps._3)
            }
          }
        }
      }
    )
  }
}

/**
 * Parameter Server companion object. Contains the entry point (main method) to run the
 * Parameter Server in a standalone fashion. Also contains various utility methods to start
 * the ParameterServer and to look up the Parameter Server actor reference.
 */
object ParameterServer {

  val INITIAL_DELAY: FiniteDuration = 0 milliseconds
  val HEARTBEAT_INTERVAL: FiniteDuration = 5000 milliseconds
  val RETRY_DELAY: FiniteDuration = 5000 milliseconds
  val STORE_TIMEOUT: FiniteDuration = 30000 milliseconds
  val MAX_RETRIES: Int = 3

  /**
   * Starts the ParameterServer based on the given configuration, in the given actor system.
   *
   * @param configuration The configuration for the ParameterServer
   * @param actorSystem Teh actor system running the ParameterServer
   * @return A reference to ParameterServer
   */
  def startParameterServerActor(
      configuration: Configuration,
      actorSystem: ActorSystem)
    : ActorRef = {

    val parameterServerProps = Props(classOf[ParameterServer])

    actorSystem.actorOf(parameterServerProps)
  }

  /**
   * Starts the ParameterStore based on the given configuration, in the given actor system.
   *
   * @param configuration The configuration for the ParameterServerStore
   * @param actorSystem Teh actor system running the ParameterServerStore
   * @return A reference to ParameterServerStore
   */
  def startParameterStoreActor(
      configuration: Configuration,
      actorSystem: ActorSystem)
  : ActorRef = {

    val parameterServerStoreProps = Props(classOf[ParameterStore])

    actorSystem.actorOf(parameterServerStoreProps)
  }
}
