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

import java.io.IOException
import java.net._
import java.time
import java.util.concurrent.{Callable, CompletableFuture}

import akka.actor._
import akka.pattern.{ask => akkaAsk}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration._
import org.apache.flink.runtime.clusterframework.BootstrapTools.{FixedThreadPoolExecutorConfiguration, ForkJoinExecutorConfiguration}
import org.apache.flink.runtime.concurrent.FutureUtils
import org.apache.flink.runtime.net.SSLUtils
import org.apache.flink.util.NetUtils
import org.apache.flink.util.TimeUtils
import org.apache.flink.util.function.FunctionUtils
import org.jboss.netty.channel.ChannelException
import org.jboss.netty.logging.{InternalLoggerFactory, Slf4JLoggerFactory}
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * This class contains utility functions for akka. It contains methods to start an actor system with
 * a given akka configuration. Furthermore, the akka configuration used for starting the different
 * actor systems resides in this class.
 */
object AkkaUtils {
  val LOG: Logger = LoggerFactory.getLogger(AkkaUtils.getClass)

  val FLINK_ACTOR_SYSTEM_NAME = "flink"

  def getFlinkActorSystemName = {
    FLINK_ACTOR_SYSTEM_NAME
  }

  /**
   * Creates a local actor system without remoting.
   *
   * @param configuration instance containing the user provided configuration values
   * @return The created actor system
   */
  def createLocalActorSystem(configuration: Configuration): ActorSystem = {
    val akkaConfig = getAkkaConfig(configuration, None)
    createActorSystem(akkaConfig)
  }

  /**
    * Creates an actor system bound to the given hostname and port.
    *
    * @param configuration instance containing the user provided configuration values
    * @param hostname of the network interface to bind to
    * @param port of to bind to
    * @return created actor system
    */
  def createActorSystem(
      configuration: Configuration,
      hostname: String,
      port: Int)
    : ActorSystem = {

    createActorSystem(configuration, Some((hostname, port)))
  }

  /**
   * Creates an actor system. If a listening address is specified, then the actor system will listen
   * on that address for messages from a remote actor system. If not, then a local actor system
   * will be instantiated.
   *
   * @param configuration instance containing the user provided configuration values
   * @param listeningAddress an optional tuple containing a bindAddress and a port to bind to.
    *                        If the parameter is None, then a local actor system will be created.
   * @return created actor system
   */
  def createActorSystem(
      configuration: Configuration,
      listeningAddress: Option[(String, Int)])
    : ActorSystem = {
    val akkaConfig = getAkkaConfig(configuration, listeningAddress)
    createActorSystem(akkaConfig)
  }

  /**
   * Creates an actor system with the given akka config.
   *
   * @param akkaConfig configuration for the actor system
   * @return created actor system
   */
  def createActorSystem(akkaConfig: Config): ActorSystem = {
    createActorSystem(FLINK_ACTOR_SYSTEM_NAME, akkaConfig)
  }

  /**
    * Creates an actor system with the given akka config.
    *
    * @param akkaConfig configuration for the actor system
    * @return created actor system
    */
  def createActorSystem(actorSystemName: String, akkaConfig: Config): ActorSystem = {
    // Initialize slf4j as logger of Akka's Netty instead of java.util.logging (FLINK-1650)
    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory)
    RobustActorSystem.create(actorSystemName, akkaConfig)
  }

  /**
   * Creates an actor system with the default config and listening on a random port of the
   * localhost.
   *
   * @return default actor system listening on a random port of the localhost
   */
  def createDefaultActorSystem(): ActorSystem = {
    createActorSystem(getDefaultAkkaConfig)
  }

  /**
    * Returns a remote Akka config for the given configuration values.
    *
    * @param configuration containing the user provided configuration values
    * @param hostname to bind against. If null, then the loopback interface is used
    * @param port to bind against
    * @param executorConfig containing the user specified config of executor
    * @return A remote Akka config
    */
  def getAkkaConfig(configuration: Configuration,
                    hostname: String,
                    port: Int,
                    executorConfig: Config): Config = {
    getAkkaConfig(configuration, Some((hostname, port)), None, executorConfig)
  }

  /**
    * Returns a remote Akka config for the given configuration values.
    *
    * @param configuration containing the user provided configuration values
    * @param hostname to bind against. If null, then the loopback interface is used
    * @param port to bind against
    * @return A remote Akka config
    */
  def getAkkaConfig(configuration: Configuration,
                    hostname: String,
                    port: Int): Config = {
    getAkkaConfig(configuration, Some((hostname, port)))
  }

  /**
    * Return a local Akka config for the given configuration values.
    *
    * @param configuration containing the user provided configuration values
    * @return A local Akka config
    */
  def getAkkaConfig(configuration: Configuration): Config = {
    getAkkaConfig(configuration, None)
  }

  /**
   * Creates an akka config with the provided configuration values. If the listening address is
   * specified, then the actor system will listen on the respective address.
   *
   * @param configuration instance containing the user provided configuration values
   * @param externalAddress optional tuple of bindAddress and port to be reachable at.
   *                        If None is given, then an Akka config for local actor system
   *                        will be returned
   * @return Akka config
   */
  @throws(classOf[UnknownHostException])
  def getAkkaConfig(configuration: Configuration,
                    externalAddress: Option[(String, Int)]): Config = {
    getAkkaConfig(
      configuration,
      externalAddress,
      None,
      getForkJoinExecutorConfig(ForkJoinExecutorConfiguration.fromConfiguration(configuration)))
  }

  /**
    * Creates an akka config with the provided configuration values. If the listening address is
    * specified, then the actor system will listen on the respective address.
    *
    * @param configuration instance containing the user provided configuration values
    * @param externalAddress optional tuple of external address and port to be reachable at.
    *                        If None is given, then an Akka config for local actor system
    *                        will be returned
    * @param bindAddress optional tuple of bind address and port to be used locally.
    *                    If None is given, wildcard IP address and the external port wil be used.
    *                    Take effects only if externalAddress is not None.
    * @param executorConfig config defining the used executor by the default dispatcher
    * @return Akka config
    */
  @throws(classOf[UnknownHostException])
  def getAkkaConfig(configuration: Configuration,
                    externalAddress: Option[(String, Int)],
                    bindAddress: Option[(String, Int)],
                    executorConfig: Config): Config = {
    val defaultConfig = getBasicAkkaConfig(configuration).withFallback(executorConfig)

    externalAddress match {

      case Some((externalHostname, externalPort)) =>

        bindAddress match {

          case Some((bindHostname, bindPort)) =>

            val remoteConfig = getRemoteAkkaConfig(
              configuration, bindHostname, bindPort, externalHostname, externalPort)

            remoteConfig.withFallback(defaultConfig)

          case None =>
            val remoteConfig = getRemoteAkkaConfig(configuration,
              // the wildcard IP lets us bind to all network interfaces
              NetUtils.getWildcardIPAddress, externalPort, externalHostname, externalPort)

            remoteConfig.withFallback(defaultConfig)
        }

      case None =>
        defaultConfig
    }
  }

  /**
   * Creates the default akka configuration which listens on a random port on the local machine.
   * All configuration values are set to default values.
   *
   * @return Flink's Akka default config
   */
  def getDefaultAkkaConfig: Config = {
    getAkkaConfig(new Configuration(), Some(("", 0)))
  }

  /**
   * Gets the basic Akka config which is shared by remote and local actor systems.
   *
   * @param configuration instance which contains the user specified values for the configuration
   * @return Flink's basic Akka config
   */
  private def getBasicAkkaConfig(configuration: Configuration): Config = {
    val akkaThroughput = configuration.getInteger(AkkaOptions.DISPATCHER_THROUGHPUT)
    val lifecycleEvents = configuration.getBoolean(AkkaOptions.LOG_LIFECYCLE_EVENTS)

    val jvmExitOnFatalError = if (
      configuration.getBoolean(AkkaOptions.JVM_EXIT_ON_FATAL_ERROR)){
      "on"
    } else {
      "off"
    }

    val logLifecycleEvents = if (lifecycleEvents) "on" else "off"

    val logLevel = getLogLevel

    val supervisorStrategy = classOf[EscalatingSupervisorStrategy]
      .getCanonicalName

    val config =
      s"""
        |akka {
        | daemonic = off
        |
        | loggers = ["akka.event.slf4j.Slf4jLogger"]
        | logging-filter = "akka.event.slf4j.Slf4jLoggingFilter"
        | log-config-on-start = off
        | logger-startup-timeout = 30s
        |
        | jvm-exit-on-fatal-error = $jvmExitOnFatalError
        |
        | serialize-messages = off
        |
        | loglevel = $logLevel
        | stdout-loglevel = OFF
        |
        | log-dead-letters = $logLifecycleEvents
        | log-dead-letters-during-shutdown = $logLifecycleEvents
        |
        | actor {
        |   guardian-supervisor-strategy = $supervisorStrategy
        |
        |   warn-about-java-serializer-usage = off
        |
        |   default-dispatcher {
        |     throughput = $akkaThroughput
        |   }
        |
        |   supervisor-dispatcher {
        |     type = Dispatcher
        |     executor = "thread-pool-executor"
        |     thread-pool-executor {
        |       core-pool-size-min = 1
        |       core-pool-size-max = 1
        |     }
        |   }
        | }
        |}
      """.stripMargin

    ConfigFactory.parseString(config)
  }

  def getThreadPoolExecutorConfig(configuration: FixedThreadPoolExecutorConfiguration): Config = {
    val threadPriority = configuration.getThreadPriority
    val minNumThreads = configuration.getMinNumThreads
    val maxNumThreads = configuration.getMaxNumThreads

    val configString = s"""
       |akka {
       |  actor {
       |    default-dispatcher {
       |      type = akka.dispatch.PriorityThreadsDispatcher
       |      executor = "thread-pool-executor"
       |      thread-priority = $threadPriority
       |      thread-pool-executor {
       |        core-pool-size-min = $minNumThreads
       |        core-pool-size-max = $maxNumThreads
       |      }
       |    }
       |  }
       |}
        """.
      stripMargin

    ConfigFactory.parseString(configString)
  }

  def getForkJoinExecutorConfig(configuration: ForkJoinExecutorConfiguration): Config = {
    val forkJoinExecutorParallelismFactor = configuration.getParallelismFactor

    val forkJoinExecutorParallelismMin = configuration.getMinParallelism

    val forkJoinExecutorParallelismMax = configuration.getMaxParallelism

    val configString = s"""
       |akka {
       |  actor {
       |    default-dispatcher {
       |      executor = "fork-join-executor"
       |      fork-join-executor {
       |        parallelism-factor = $forkJoinExecutorParallelismFactor
       |        parallelism-min = $forkJoinExecutorParallelismMin
       |        parallelism-max = $forkJoinExecutorParallelismMax
       |      }
       |    }
       |  }
       |}""".stripMargin

    ConfigFactory.parseString(configString)
  }

  def testDispatcherConfig: Config = {
    val config =
      s"""
         |akka {
         |  actor {
         |    default-dispatcher {
         |      fork-join-executor {
         |        parallelism-factor = 1.0
         |        parallelism-min = 2
         |        parallelism-max = 4
         |      }
         |    }
         |  }
         |}
      """.stripMargin

    ConfigFactory.parseString(config)
  }

  private def validateHeartbeat(pauseParamName: String,
                                pauseValue: time.Duration,
                                intervalParamName: String,
                                intervalValue: time.Duration): Unit = {
    if (pauseValue.compareTo(intervalValue) <= 0) {
      throw new IllegalConfigurationException(
        "%s [%s] must greater than %s [%s]",
        pauseParamName,
        pauseValue,
        intervalParamName,
        intervalValue)
    }
  }

  /**
   * Creates a Akka config for a remote actor system listening on port on the network interface
   * identified by bindAddress.
   *
   * @param configuration instance containing the user provided configuration values
   * @param bindAddress of the network interface to bind on
   * @param port to bind to or if 0 then Akka picks a free port automatically
   * @param externalHostname The host name to expect for Akka messages
   * @param externalPort The port to expect for Akka messages
   * @return Flink's Akka configuration for remote actor systems
   */
  private def getRemoteAkkaConfig(
      configuration: Configuration,
      bindAddress: String,
      port: Int,
      externalHostname: String,
      externalPort: Int): Config = {

    val normalizedExternalHostname = NetUtils.unresolvedHostToNormalizedString(externalHostname)

    val akkaAskTimeout = getTimeout(configuration)

    val startupTimeout = TimeUtils.getStringInMillis(
      TimeUtils.parseDuration(
        configuration.getString(
          AkkaOptions.STARTUP_TIMEOUT,
          TimeUtils.getStringInMillis(akkaAskTimeout.multipliedBy(10L)))))

    val transportHeartbeatIntervalDuration = TimeUtils.parseDuration(
      configuration.getString(AkkaOptions.TRANSPORT_HEARTBEAT_INTERVAL))

    val transportHeartbeatPauseDuration = TimeUtils.parseDuration(
      configuration.getString(AkkaOptions.TRANSPORT_HEARTBEAT_PAUSE))

    validateHeartbeat(
      AkkaOptions.TRANSPORT_HEARTBEAT_PAUSE.key(),
      transportHeartbeatPauseDuration,
      AkkaOptions.TRANSPORT_HEARTBEAT_INTERVAL.key(),
      transportHeartbeatIntervalDuration)

    val transportHeartbeatInterval = TimeUtils.getStringInMillis(transportHeartbeatIntervalDuration)

    val transportHeartbeatPause = TimeUtils.getStringInMillis(transportHeartbeatPauseDuration)

    val transportThreshold = configuration.getDouble(AkkaOptions.TRANSPORT_THRESHOLD)

    val akkaTCPTimeout = TimeUtils.getStringInMillis(
      TimeUtils.parseDuration(configuration.getString(AkkaOptions.TCP_TIMEOUT)))

    val akkaFramesize = configuration.getString(AkkaOptions.FRAMESIZE)

    val lifecycleEvents = configuration.getBoolean(AkkaOptions.LOG_LIFECYCLE_EVENTS)

    val logLifecycleEvents = if (lifecycleEvents) "on" else "off"

    val akkaEnableSSLConfig = configuration.getBoolean(AkkaOptions.SSL_ENABLED) &&
          SSLUtils.isInternalSSLEnabled(configuration)

    val retryGateClosedFor = configuration.getLong(AkkaOptions.RETRY_GATE_CLOSED_FOR)

    val akkaEnableSSL = if (akkaEnableSSLConfig) "on" else "off"

    val akkaSSLKeyStore = configuration.getString(
                              SecurityOptions.SSL_INTERNAL_KEYSTORE,
                              configuration.getString(SecurityOptions.SSL_KEYSTORE))

    val akkaSSLKeyStorePassword = configuration.getString(
                              SecurityOptions.SSL_INTERNAL_KEYSTORE_PASSWORD,
                              configuration.getString(SecurityOptions.SSL_KEYSTORE_PASSWORD))

    val akkaSSLKeyPassword = configuration.getString(
                              SecurityOptions.SSL_INTERNAL_KEY_PASSWORD,
                              configuration.getString(SecurityOptions.SSL_KEY_PASSWORD))

    val akkaSSLTrustStore = configuration.getString(
                              SecurityOptions.SSL_INTERNAL_TRUSTSTORE,
                              configuration.getString(SecurityOptions.SSL_TRUSTSTORE))

    val akkaSSLTrustStorePassword = configuration.getString(
                              SecurityOptions.SSL_INTERNAL_TRUSTSTORE_PASSWORD,
                              configuration.getString(SecurityOptions.SSL_TRUSTSTORE_PASSWORD))

    val akkaSSLCertFingerprintString = configuration.getString(
                              SecurityOptions.SSL_INTERNAL_CERT_FINGERPRINT)

    val akkaSSLCertFingerprints = if ( akkaSSLCertFingerprintString != null ) {
      akkaSSLCertFingerprintString.split(",").toList.mkString("[\"", "\",\"", "\"]")
    } else  "[]"

    val akkaSSLProtocol = configuration.getString(SecurityOptions.SSL_PROTOCOL)

    val akkaSSLAlgorithmsString = configuration.getString(SecurityOptions.SSL_ALGORITHMS)
    val akkaSSLAlgorithms = akkaSSLAlgorithmsString.split(",").toList.mkString("[", ",", "]")

    val clientSocketWorkerPoolPoolSizeMin =
      configuration.getInteger(AkkaOptions.CLIENT_SOCKET_WORKER_POOL_SIZE_MIN)

    val clientSocketWorkerPoolPoolSizeMax =
      configuration.getInteger(AkkaOptions.CLIENT_SOCKET_WORKER_POOL_SIZE_MAX)

    val clientSocketWorkerPoolPoolSizeFactor =
      configuration.getDouble(AkkaOptions.CLIENT_SOCKET_WORKER_POOL_SIZE_FACTOR)

    val serverSocketWorkerPoolPoolSizeMin =
      configuration.getInteger(AkkaOptions.SERVER_SOCKET_WORKER_POOL_SIZE_MIN)

    val serverSocketWorkerPoolPoolSizeMax =
      configuration.getInteger(AkkaOptions.SERVER_SOCKET_WORKER_POOL_SIZE_MAX)

    val serverSocketWorkerPoolPoolSizeFactor =
      configuration.getDouble(AkkaOptions.SERVER_SOCKET_WORKER_POOL_SIZE_FACTOR)


    val configString =
      s"""
         |akka {
         |  actor {
         |    provider = "akka.remote.RemoteActorRefProvider"
         |  }
         |
         |  remote {
         |    startup-timeout = $startupTimeout
         |
         |    transport-failure-detector{
         |      acceptable-heartbeat-pause = $transportHeartbeatPause
         |      heartbeat-interval = $transportHeartbeatInterval
         |      threshold = $transportThreshold
         |    }
         |
         |    netty {
         |      tcp {
         |        transport-class = "akka.remote.transport.netty.NettyTransport"
         |        port = $externalPort
         |        bind-port = $port
         |        connection-timeout = $akkaTCPTimeout
         |        maximum-frame-size = $akkaFramesize
         |        tcp-nodelay = on
         |
         |        client-socket-worker-pool {
         |          pool-size-min = $clientSocketWorkerPoolPoolSizeMin
         |          pool-size-max = $clientSocketWorkerPoolPoolSizeMax
         |          pool-size-factor = $clientSocketWorkerPoolPoolSizeFactor
         |        }
         |
         |        server-socket-worker-pool {
         |          pool-size-min = $serverSocketWorkerPoolPoolSizeMin
         |          pool-size-max = $serverSocketWorkerPoolPoolSizeMax
         |          pool-size-factor = $serverSocketWorkerPoolPoolSizeFactor
         |        }
         |      }
         |    }
         |
         |    log-remote-lifecycle-events = $logLifecycleEvents
         |
         |    retry-gate-closed-for = ${retryGateClosedFor + " ms"}
         |  }
         |}
       """.stripMargin

    val effectiveHostname =
      if (normalizedExternalHostname != null && normalizedExternalHostname.nonEmpty) {
        normalizedExternalHostname
      } else {
        // if bindAddress is null or empty, then leave bindAddress unspecified. Akka will pick
        // InetAddress.getLocalHost.getHostAddress
        ""
      }

    val hostnameConfigString =
      s"""
         |akka {
         |  remote {
         |    netty {
         |      tcp {
         |        hostname = "$effectiveHostname"
         |        bind-hostname = "$bindAddress"
         |      }
         |    }
         |  }
         |}
       """.stripMargin

    val sslConfigString = if (akkaEnableSSLConfig) {
      s"""
         |akka {
         |  remote {
         |
         |    enabled-transports = ["akka.remote.netty.ssl"]
         |
         |    netty {
         |
         |      ssl = $${akka.remote.netty.tcp}
         |
         |      ssl {
         |
         |        enable-ssl = $akkaEnableSSL
         |        ssl-engine-provider = org.apache.flink.runtime.akka.CustomSSLEngineProvider
         |        security {
         |          key-store = "$akkaSSLKeyStore"
         |          key-store-password = "$akkaSSLKeyStorePassword"
         |          key-password = "$akkaSSLKeyPassword"
         |          trust-store = "$akkaSSLTrustStore"
         |          trust-store-password = "$akkaSSLTrustStorePassword"
         |          protocol = $akkaSSLProtocol
         |          enabled-algorithms = $akkaSSLAlgorithms
         |          random-number-generator = ""
         |          require-mutual-authentication = on
         |          cert-fingerprints = $akkaSSLCertFingerprints
         |        }
         |      }
         |    }
         |  }
         |}
       """.stripMargin
    }else{
      ""
    }

    ConfigFactory.parseString(configString + hostnameConfigString + sslConfigString).resolve()
  }

  def getLogLevel: String = {
    if (LOG.isTraceEnabled) {
      "TRACE"
    } else {
      if (LOG.isDebugEnabled) {
        "DEBUG"
      } else {
        if (LOG.isInfoEnabled) {
          "INFO"
        } else {
          if (LOG.isWarnEnabled) {
            "WARNING"
          } else {
            if (LOG.isErrorEnabled) {
              "ERROR"
            } else {
              "OFF"
            }
          }
        }
      }
    }
  }

  /** Returns a [[Future]] to the [[ActorRef]] of the child of a given actor. The child is specified
    * by providing its actor name.
    *
    * @param parent [[ActorRef]] to the parent of the child to be retrieved
    * @param child Name of the child actor
    * @param system [[ActorSystem]] to be used
    * @param timeout Maximum timeout for the future
    * @return [[Future]] to the [[ActorRef]] of the child actor
    */
  def getChild(
      parent: ActorRef,
      child: String,
      system: ActorSystem,
      timeout: FiniteDuration)
    : Future[ActorRef] = {
    system.actorSelection(parent.path / child).resolveOne()(timeout)
  }

  /** Returns a [[Future]] to the [[ActorRef]] of an actor. The actor is specified by its path.
    *
    * @param path Path to the actor to be retrieved
    * @param system [[ActorSystem]] to be used
    * @param timeout Maximum timeout for the future
    * @return [[Future]] to the [[ActorRef]] of the actor
    */
  def getActorRefFuture(
      path: String,
      system: ActorSystem,
      timeout: FiniteDuration)
    : Future[ActorRef] = {
    system.actorSelection(path).resolveOne()(timeout)
  }

  /** Returns an [[ActorRef]] for the actor specified by the path parameter.
    *
    * @param path Path to the actor to be retrieved
    * @param system [[ActorSystem]] to be used
    * @param timeout Maximum timeout for the future
    * @throws java.io.IOException
    * @return [[ActorRef]] of the requested [[Actor]]
    */
  @throws(classOf[IOException])
  def getActorRef(
      path: String,
      system: ActorSystem,
      timeout: FiniteDuration)
    : ActorRef = {
    try {
      val future = AkkaUtils.getActorRefFuture(path, system, timeout)
      Await.result(future, timeout)
    }
    catch {
      case e @ (_ : ActorNotFound | _ : TimeoutException) =>
        throw new IOException(
          s"Actor at $path not reachable. " +
            "Please make sure that the actor is running and its port is reachable.", e)

      case e: IOException =>
        throw new IOException(s"Could not connect to the actor at $path", e)
    }
  }


  /**
   * Utility function to construct a future which tries multiple times to execute itself if it
   * fails. If the maximum number of tries are exceeded, then the future fails.
   *
   * @param body function describing the future action
   * @param tries number of maximum tries before the future fails
   * @param executionContext which shall execute the future
   * @tparam T return type of the future
   * @return future which tries to recover by re-executing itself a given number of times
   */
  def retry[T](body: => T, tries: Int)(implicit executionContext: ExecutionContext): Future[T] = {
    Future{ body }.recoverWith{
      case t:Throwable =>
        if(tries > 0){
          retry(body, tries - 1)
        }else{
          Future.failed(t)
        }
    }
  }

  /**
   * Utility function to construct a future which tries multiple times to execute itself if it
   * fails. If the maximum number of tries are exceeded, then the future fails.
   *
   * @param callable future action
   * @param tries maximum number of tries before the future fails
   * @param executionContext which shall execute the future
   * @tparam T return type of the future
   * @return future which tries to recover by re-executing itself a given number of times
   */
  def retry[T](callable: Callable[T], tries: Int)(implicit executionContext: ExecutionContext):
  Future[T] = {
    retry(callable.call(), tries)
  }

  /**
   * Utility function to construct a future which tries multiple times to execute itself if it
   * fails. If the maximum number of tries are exceeded, then the future fails.
   *
   * @param target actor which receives the message
   * @param message to be sent to the target actor
   * @param tries maximum number of tries before the future fails
   * @param executionContext which shall execute the future
   * @param timeout of the future
   * @return future which tries to recover by re-executing itself a given number of times
   */
  def retry(target: ActorRef, message: Any, tries: Int)(implicit executionContext:
  ExecutionContext, timeout: FiniteDuration): Future[Any] = {
    (target ? message)(timeout) recoverWith{
      case t: Throwable =>
        if(tries > 0){
          retry(target, message, tries-1)
        }else{
          Future.failed(t)
        }
    }
  }

  def getTimeout(config: Configuration): time.Duration = {
    TimeUtils.parseDuration(config.getString(AkkaOptions.ASK_TIMEOUT))
  }

  def getTimeoutAsTime(config: Configuration): Time = {
    try {
      val duration = getTimeout(config)

      Time.milliseconds(duration.toMillis)
    } catch {
      case _: NumberFormatException =>
        throw new IllegalConfigurationException(AkkaUtils.formatDurationParsingErrorMessage)
    }
  }

  def getDefaultTimeout: Time = {
    val duration = TimeUtils.parseDuration(AkkaOptions.ASK_TIMEOUT.defaultValue())

    Time.milliseconds(duration.toMillis)
  }

  def getLookupTimeout(config: Configuration): time.Duration = {
    TimeUtils.parseDuration(config.getString(AkkaOptions.LOOKUP_TIMEOUT))
  }

  /** Returns the address of the given [[ActorSystem]]. The [[Address]] object contains
    * the port and the host under which the actor system is reachable
    *
    * @param system [[ActorSystem]] for which the [[Address]] shall be retrieved
    * @return [[Address]] of the given [[ActorSystem]]
    */
  def getAddress(system: ActorSystem): Address = {
    RemoteAddressExtension(system).address
  }

  /** Returns the given [[ActorRef]]'s path string representation with host and port of the
    * [[ActorSystem]] in which the actor is running.
    *
    * @param system [[ActorSystem]] in which the given [[ActorRef]] is running
    * @param actor [[ActorRef]] of the [[Actor]] for which the URL has to be generated
    * @return String containing the [[ActorSystem]] independent URL of the [[Actor]]
    */
  def getAkkaURL(system: ActorSystem, actor: ActorRef): String = {
    val address = getAddress(system)
    actor.path.toStringWithAddress(address)
  }

  /** Returns the AkkaURL for a given [[ActorSystem]] and a path describing a running [[Actor]] in
    * the actor system.
    *
    * @param system [[ActorSystem]] in which the given [[Actor]] is running
    * @param path Path describing an [[Actor]] for which the URL has to be generated
    * @return String containing the [[ActorSystem]] independent URL of an [[Actor]] specified by
    *         path.
    */
  def getAkkaURL(system: ActorSystem, path: String): String = {
    val address = getAddress(system)
    address.toString + path
  }

  /** Extracts the hostname and the port of the remote actor system from the given Akka URL. The
    * result is an [[InetSocketAddress]] instance containing the extracted hostname and port. If
    * the Akka URL does not contain the hostname and port information, e.g. a local Akka URL is
    * provided, then an [[Exception]] is thrown.
    *
    * @param akkaURL The URL to extract the host and port from.
    * @throws java.lang.Exception Thrown, if the given string does not represent a proper url
    * @return The InetSocketAddress with the extracted host and port.
    */
  @throws(classOf[Exception])
  def getInetSocketAddressFromAkkaURL(akkaURL: String): InetSocketAddress = {
    // AkkaURLs have the form schema://systemName@host:port/.... if it's a remote Akka URL
    try {
      val address = getAddressFromAkkaURL(akkaURL)

      (address.host, address.port) match {
        case (Some(hostname), Some(portValue)) => new InetSocketAddress(hostname, portValue)
        case _ => throw new MalformedURLException()
      }
    }
    catch {
      case _ : MalformedURLException =>
        throw new Exception(s"Could not retrieve InetSocketAddress from Akka URL $akkaURL")
    }
  }

  /**
    * Extracts the [[Address]] from the given akka URL.
    *
    * @param akkaURL to extract the [[Address]] from
    * @throws java.net.MalformedURLException if the [[Address]] could not be parsed from
    *                                        the given akka URL
    * @return Extracted [[Address]] from the given akka URL
    */
  @throws(classOf[MalformedURLException])
  def getAddressFromAkkaURL(akkaURL: String): Address = {
    AddressFromURIString(akkaURL)
  }

  def formatDurationParsingErrorMessage: String = {
    "Duration format must be \"val unit\", where 'val' is a number and 'unit' is " +
      "(d|day)|(h|hour)|(min|minute)|s|sec|second)|(ms|milli|millisecond)|" +
      "(µs|micro|microsecond)|(ns|nano|nanosecond)"
  }

  /**
    * Returns the local akka url for the given actor name.
    *
    * @param actorName Actor name identifying the actor
    * @return Local Akka URL for the given actor
    */
  def getLocalAkkaURL(actorName: String): String = {
    "akka://flink/user/" + actorName
  }

  /**
    * Retries a function if it fails because of a [[java.net.BindException]].
    *
    * @param fn The function to retry
    * @param stopCond Flag to signal termination
    * @param maxSleepBetweenRetries Max random sleep time between retries
    * @tparam T Return type of the function to retry
    * @return Return value of the function to retry
    */
  @tailrec
  def retryOnBindException[T](
      fn: => T,
      stopCond: => Boolean,
      maxSleepBetweenRetries : Long = 0 )
    : scala.util.Try[T] = {

    def sleepBeforeRetry() : Unit = {
      if (maxSleepBetweenRetries > 0) {
        val sleepTime = (Math.random() * maxSleepBetweenRetries).asInstanceOf[Long]
        LOG.info(s"Retrying after bind exception. Sleeping for $sleepTime ms.")
        Thread.sleep(sleepTime)
      }
    }

    scala.util.Try {
      fn
    } match {
      case scala.util.Failure(x: BindException) =>
        if (stopCond) {
          scala.util.Failure(x)
        } else {
          sleepBeforeRetry()
          retryOnBindException(fn, stopCond)
        }
      case scala.util.Failure(x: Exception) => x.getCause match {
        case _: ChannelException =>
          if (stopCond) {
            scala.util.Failure(new RuntimeException(
              "Unable to do further retries starting the actor system"))
          } else {
            sleepBeforeRetry()
            retryOnBindException(fn, stopCond)
          }
        case _ => scala.util.Failure(x)
      }
      case f => f
    }
  }

  /**
    * Terminates the given [[ActorSystem]] and returns its termination future.
    *
    * @param actorSystem to terminate
    * @return Termination future
    */
  def terminateActorSystem(actorSystem: ActorSystem): CompletableFuture[Void] = {
    FutureUtils.toJava(actorSystem.terminate).thenAccept(FunctionUtils.ignoreFn())
  }
}

