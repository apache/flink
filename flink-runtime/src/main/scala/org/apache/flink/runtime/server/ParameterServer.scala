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
import grizzled.slf4j.Logger
import org.apache.flink.api.common.server.{Update, UpdateStrategy, Parameter}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.instance.{AkkaActorGateway, ActorGateway, InstanceID}
import org.apache.flink.runtime.messages.Messages.Disconnect
import org.apache.flink.runtime.messages.ServerMessages.InvalidServerAccessException
import org.apache.flink.runtime.messages.JobManagerMessages.{ResponseLeaderSessionID, RequestLeaderSessionID}
import org.apache.flink.runtime.messages.ServerMessages._
import org.apache.flink.runtime.{LeaderSessionMessages, FlinkActor, LogMessages}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.collection.JavaConverters._


class ParameterServer extends FlinkActor with LeaderSessionMessages with LogMessages{

  override val log = Logger(getClass)

  private var jobManager: Option[ActorGateway] = None
  private var taskManager: Option[ActorGateway] = None
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

  private var store: mutable.HashMap[String, KeyManager] = _

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
      keyGatewayMapping.clear()
      keyGatewayMap.foreach(mapping => keyGatewayMapping.put(mapping.key, mapping.server))
      redundantPartner = copyPartner

    case message: ServerRetry =>
      handleRequests(message.message, message.sender, message.retryNumber)

    case message: KickOffParameterServer =>
      // sent by our task manager
      handleKickoff(message)

    case ServerRegistrationRefuse(error) =>
      // nothing much to do. We can keep doing our work as long as we can. If the Task Manager also
      // lost connection, we'll anyway have to reset our state and register again, if ever.

    case Disconnect =>
      // lost connection to Job Manager. Kill self. Task Manager will have to restart us.
      handleJobManagerDisconnect()

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

    // now crash
    throw new RuntimeException("Received unknown message " + message)
  }

  private def handleJobManagerDisconnect(): Unit = {
    println(System.currentTimeMillis() + "Received job manager disconnect")
    heartbeatScheduler.get.cancel()
    statusQueueCheckScheduler.get.cancel()

    keyGatewayMapping.clear()

    jobManager = None
    taskManager = None
    taskManagerID = None

    store.valuesIterator.foreach(manager => manager.shutdown)
    store.clear()
    waitingClients.clear()
    waitingServers.clear()
  }

  private def sendHeartbeat(): Unit = {
    // now send a heartbeat
    println(System.currentTimeMillis() + "Sending heart beat")
    jobManager.get.tell(
      ServerHeartbeat(taskManagerID.get, new AkkaActorGateway(self, leaderSessionID)))
  }

  private def handleKickoff(message: KickOffParameterServer): Unit = {
    // initialize everything
    jobManager = Some(message.jobManager)
    taskManagerID = Some(message.taskManagerID)
    taskManager = Some(message.taskManager)

    keyGatewayMapping = new mutable.HashMap[String, ActorGateway]()
    waitingClients = new mutable.HashMap[UUID, (Boolean, ClientRequests, ActorRef, Long, Int)]()
    waitingServers = new mutable.HashMap[UUID, ActorRef]()
    store = new mutable.HashMap[String, KeyManager]()

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
      jobManager.get.tell(RequestKeyGateway(message.key, taskManagerID.get))
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
    // first see if we can work on this key.
    if(!store.contains(message.message.key) && !message.message.isInstanceOf[RegisterClient]){
      sender ! ServerResponse(message.messageID, ClientFailure(new InvalidServerAccessException))
    } else{
      waitingServers.put(message.messageID, sender)
      sender ! ServerAcknowledgement(message.messageID, true)
      // now process the message
      handleClientMessage(message)
    }
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

  private def handleClientMessage(message: ServerRequest): Unit = {
    val key = message.message.key
    val manager = store.get(key)
    message.message match{
      case UpdateParameter(_, update) =>
        manager.get.update(update, message.messageID)

      case PullParameter(_) =>
        manager.get.getParameter(message.messageID)

      case RegisterClient(id, _, value, strategy, slack) =>
        manager match{
          case Some(managerExists) =>
            if(slack == managerExists.slack && strategy == managerExists.strategy){
              managerExists.registerClient(id, message.messageID)
            } else{
              self ! StoreReply(
                message.messageID,
                ClientFailure(new Exception("Slack and strategy must be the same for all clients"))
              )
            }
          case None =>
            store.put(key, new KeyManager(
              key, value, strategy, slack, new AkkaActorGateway(self, leaderSessionID))
            )
            store.get(key).get.registerClient(id, message.messageID)
        }
    }
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
}

class KeyManager(
    val key: String,
    val value: Parameter,
    val strategy: UpdateStrategy,
    val slack: Int,
    server: ActorGateway){

  private val registeredClients = new mutable.ArrayBuffer[Int]()
  private val waitingUpdates = new mutable.HashMap[Update, UUID]()

  // the minimum clock is only incremented when we have received all messages from minimumClock + 1
  private var minimumClock: Int = 0


  def registerClient(id: Int, messageID: UUID): Unit = {
    this.synchronized {
      registeredClients.synchronized {
        if (!registeredClients.contains(id)) {
          registeredClients += id
        }
        server.tell(StoreReply(messageID, RegistrationSuccess()))
      }
    }
  }

  def getParameter(messageID: UUID): Unit = {
    this.synchronized {
      value.setClock(minimumClock + 1)
      server.tell(StoreReply(messageID, PullSuccess(value)))
    }
  }

  def update(update: Update, messageID: UUID): Unit ={
    this.synchronized{
      strategy match{
        case UpdateStrategy.BATCH =>
          if(update.getClock != minimumClock + 1){
            server.tell(StoreReply(
              messageID, ClientFailure(new Exception("Clock synchronization issue.")))
            )
          } else{
            waitingUpdates.put(update, messageID)
            if(waitingUpdates.size == registeredClients.length){
              var messageToSend: ClientResponses = UpdateSuccess()
              try {
                value.reduce(waitingUpdates.keySet.toList.asJavaCollection)
                minimumClock += 1
              } catch{
                case e: AnyRef =>
                  messageToSend = ClientFailure(e)
              }
              waitingUpdates.valuesIterator.foreach(
                messager => server.tell(StoreReply(messager, messageToSend))
              )
              waitingUpdates.clear()
            }
          }
        case UpdateStrategy.ASYNC =>
          try{
            value.update(update)
            server.tell(StoreReply(messageID, UpdateSuccess()))
          } catch{
            case e: AnyRef =>
              server.tell(StoreReply(messageID, ClientFailure(e)))
          }
        case UpdateStrategy.SSP =>
        // TODO
      }
    }
  }

  def shutdown: Unit = {
    this.synchronized{
      registeredClients.clear()
      waitingUpdates.clear()
    }
  }
}
