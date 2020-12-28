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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.description.Description;

import static org.apache.flink.configuration.description.LinkElement.link;

/** Akka configuration options. */
@PublicEvolving
public class AkkaOptions {

    /** Flag whether to capture call stacks for RPC ask calls. */
    public static final ConfigOption<Boolean> CAPTURE_ASK_CALLSTACK =
            ConfigOptions.key("akka.ask.callstack")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "If true, call stack for asynchronous asks are captured. That way, when an ask fails "
                                    + "(for example times out), you get a proper exception, describing to the original method call and "
                                    + "call site. Note that in case of having millions of concurrent RPC calls, this may add to the "
                                    + "memory footprint.");

    /** Timeout for akka ask calls. */
    public static final ConfigOption<String> ASK_TIMEOUT =
            ConfigOptions.key("akka.ask.timeout")
                    .stringType()
                    .defaultValue("10 s")
                    .withDescription(
                            "Timeout used for all futures and blocking Akka calls. If Flink fails due to timeouts then you"
                                    + " should try to increase this value. Timeouts can be caused by slow machines or a congested network. The"
                                    + " timeout value requires a time-unit specifier (ms/s/min/h/d).");
    /** The Akka tcp connection timeout. */
    public static final ConfigOption<String> TCP_TIMEOUT =
            ConfigOptions.key("akka.tcp.timeout")
                    .stringType()
                    .defaultValue("20 s")
                    .withDescription(
                            "Timeout for all outbound connections. If you should experience problems with connecting to a"
                                    + " TaskManager due to a slow network, you should increase this value.");

    /** Timeout for the startup of the actor system. */
    public static final ConfigOption<String> STARTUP_TIMEOUT =
            ConfigOptions.key("akka.startup-timeout")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Timeout after which the startup of a remote component is considered being failed.");

    /** Heartbeat interval of the transport failure detector. */
    public static final ConfigOption<String> TRANSPORT_HEARTBEAT_INTERVAL =
            ConfigOptions.key("akka.transport.heartbeat.interval")
                    .stringType()
                    .defaultValue("1000 s")
                    .withDescription(
                            "Heartbeat interval for Akka’s transport failure detector. Since Flink uses TCP, the detector"
                                    + " is not necessary. Therefore, the detector is disabled by setting the interval to a very high value. In"
                                    + " case you should need the transport failure detector, set the interval to some reasonable value. The"
                                    + " interval value requires a time-unit specifier (ms/s/min/h/d).");

    /** Allowed heartbeat pause for the transport failure detector. */
    public static final ConfigOption<String> TRANSPORT_HEARTBEAT_PAUSE =
            ConfigOptions.key("akka.transport.heartbeat.pause")
                    .stringType()
                    .defaultValue("6000 s")
                    .withDescription(
                            "Acceptable heartbeat pause for Akka’s transport failure detector. Since Flink uses TCP, the"
                                    + " detector is not necessary. Therefore, the detector is disabled by setting the pause to a very high value."
                                    + " In case you should need the transport failure detector, set the pause to some reasonable value."
                                    + " The pause value requires a time-unit specifier (ms/s/min/h/d).");

    /** Detection threshold of transport failure detector. */
    public static final ConfigOption<Double> TRANSPORT_THRESHOLD =
            ConfigOptions.key("akka.transport.threshold")
                    .doubleType()
                    .defaultValue(300.0)
                    .withDescription(
                            "Threshold for the transport failure detector. Since Flink uses TCP, the detector is not"
                                    + " necessary and, thus, the threshold is set to a high value.");

    /** Override SSL support for the Akka transport. */
    public static final ConfigOption<Boolean> SSL_ENABLED =
            ConfigOptions.key("akka.ssl.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Turns on SSL for Akka’s remote communication. This is applicable only when the global ssl flag"
                                    + " security.ssl.enabled is set to true.");

    /** Maximum framesize of akka messages. */
    public static final ConfigOption<String> FRAMESIZE =
            ConfigOptions.key("akka.framesize")
                    .stringType()
                    .defaultValue("10485760b")
                    .withDescription(
                            "Maximum size of messages which are sent between the JobManager and the TaskManagers. If Flink"
                                    + " fails because messages exceed this limit, then you should increase it. The message size requires a"
                                    + " size-unit specifier.");

    /** Maximum number of messages until another actor is executed by the same thread. */
    public static final ConfigOption<Integer> DISPATCHER_THROUGHPUT =
            ConfigOptions.key("akka.throughput")
                    .intType()
                    .defaultValue(15)
                    .withDescription(
                            "Number of messages that are processed in a batch before returning the thread to the pool. Low"
                                    + " values denote a fair scheduling whereas high values can increase the performance at the cost of unfairness.");

    /** Log lifecycle events. */
    public static final ConfigOption<Boolean> LOG_LIFECYCLE_EVENTS =
            ConfigOptions.key("akka.log.lifecycle.events")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Turns on the Akka’s remote logging of events. Set this value to 'true' in case of debugging.");

    /** Timeout for all blocking calls that look up remote actors. */
    public static final ConfigOption<String> LOOKUP_TIMEOUT =
            ConfigOptions.key("akka.lookup.timeout")
                    .stringType()
                    .defaultValue("10 s")
                    .withDescription(
                            "Timeout used for the lookup of the JobManager. The timeout value has to contain a time-unit"
                                    + " specifier (ms/s/min/h/d).");

    /**
     * Timeout for all blocking calls on the client side.
     *
     * @deprecated Use the {@code ClientOptions.CLIENT_TIMEOUT} instead.
     */
    @Deprecated
    public static final ConfigOption<String> CLIENT_TIMEOUT =
            ConfigOptions.key("akka.client.timeout")
                    .stringType()
                    .defaultValue("60 s")
                    .withDescription(
                            "DEPRECATED: Use the \"client.timeout\" instead."
                                    + " Timeout for all blocking calls on the client side.");

    /** Exit JVM on fatal Akka errors. */
    public static final ConfigOption<Boolean> JVM_EXIT_ON_FATAL_ERROR =
            ConfigOptions.key("akka.jvm-exit-on-fatal-error")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Exit JVM on fatal Akka errors.");

    /** Milliseconds a gate should be closed for after a remote connection was disconnected. */
    public static final ConfigOption<Long> RETRY_GATE_CLOSED_FOR =
            ConfigOptions.key("akka.retry-gate-closed-for")
                    .longType()
                    .defaultValue(50L)
                    .withDescription(
                            "Milliseconds a gate should be closed for after a remote connection was disconnected.");

    // ==================================================
    // Configurations for fork-join-executor.
    // ==================================================

    public static final ConfigOption<Double> FORK_JOIN_EXECUTOR_PARALLELISM_FACTOR =
            ConfigOptions.key("akka.fork-join-executor.parallelism-factor")
                    .doubleType()
                    .defaultValue(2.0)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The parallelism factor is used to determine thread pool size using the"
                                                    + " following formula: ceil(available processors * factor). Resulting size"
                                                    + " is then bounded by the parallelism-min and parallelism-max values.")
                                    .build());

    public static final ConfigOption<Integer> FORK_JOIN_EXECUTOR_PARALLELISM_MIN =
            ConfigOptions.key("akka.fork-join-executor.parallelism-min")
                    .intType()
                    .defaultValue(8)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Min number of threads to cap factor-based parallelism number to.")
                                    .build());

    public static final ConfigOption<Integer> FORK_JOIN_EXECUTOR_PARALLELISM_MAX =
            ConfigOptions.key("akka.fork-join-executor.parallelism-max")
                    .intType()
                    .defaultValue(64)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Max number of threads to cap factor-based parallelism number to.")
                                    .build());

    // ==================================================
    // Configurations for client-socket-work-pool.
    // ==================================================

    public static final ConfigOption<Integer> CLIENT_SOCKET_WORKER_POOL_SIZE_MIN =
            ConfigOptions.key("akka.client-socket-worker-pool.pool-size-min")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            Description.builder()
                                    .text("Min number of threads to cap factor-based number to.")
                                    .build());

    public static final ConfigOption<Integer> CLIENT_SOCKET_WORKER_POOL_SIZE_MAX =
            ConfigOptions.key("akka.client-socket-worker-pool.pool-size-max")
                    .intType()
                    .defaultValue(2)
                    .withDescription(
                            Description.builder()
                                    .text("Max number of threads to cap factor-based number to.")
                                    .build());

    public static final ConfigOption<Double> CLIENT_SOCKET_WORKER_POOL_SIZE_FACTOR =
            ConfigOptions.key("akka.client-socket-worker-pool.pool-size-factor")
                    .doubleType()
                    .defaultValue(1.0)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The pool size factor is used to determine thread pool size"
                                                    + " using the following formula: ceil(available processors * factor)."
                                                    + " Resulting size is then bounded by the pool-size-min and"
                                                    + " pool-size-max values.")
                                    .build());

    // ==================================================
    // Configurations for server-socket-work-pool.
    // ==================================================

    public static final ConfigOption<Integer> SERVER_SOCKET_WORKER_POOL_SIZE_MIN =
            ConfigOptions.key("akka.server-socket-worker-pool.pool-size-min")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            Description.builder()
                                    .text("Min number of threads to cap factor-based number to.")
                                    .build());

    public static final ConfigOption<Integer> SERVER_SOCKET_WORKER_POOL_SIZE_MAX =
            ConfigOptions.key("akka.server-socket-worker-pool.pool-size-max")
                    .intType()
                    .defaultValue(2)
                    .withDescription(
                            Description.builder()
                                    .text("Max number of threads to cap factor-based number to.")
                                    .build());

    public static final ConfigOption<Double> SERVER_SOCKET_WORKER_POOL_SIZE_FACTOR =
            ConfigOptions.key("akka.server-socket-worker-pool.pool-size-factor")
                    .doubleType()
                    .defaultValue(1.0)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The pool size factor is used to determine thread pool size"
                                                    + " using the following formula: ceil(available processors * factor)."
                                                    + " Resulting size is then bounded by the pool-size-min and"
                                                    + " pool-size-max values.")
                                    .build());

    // ==================================================
    // Deprecated options
    // ==================================================

    /**
     * The Akka death watch heartbeat interval.
     *
     * @deprecated Don't use this option anymore. It has no effect on Flink.
     */
    @Deprecated
    public static final ConfigOption<String> WATCH_HEARTBEAT_INTERVAL =
            ConfigOptions.key("akka.watch.heartbeat.interval")
                    .defaultValue(ASK_TIMEOUT.defaultValue())
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Heartbeat interval for Akka’s DeathWatch mechanism to detect dead TaskManagers. If"
                                                    + " TaskManagers are wrongly marked dead because of lost or delayed heartbeat messages, then you"
                                                    + " should decrease this value or increase akka.watch.heartbeat.pause. A thorough description of"
                                                    + " Akka’s DeathWatch can be found %s",
                                            link(
                                                    "http://doc.akka.io/docs/akka/snapshot/scala/remoting.html#failure-detector",
                                                    "here"))
                                    .build());

    /**
     * The maximum acceptable Akka death watch heartbeat pause.
     *
     * @deprecated Don't use this option anymore. It has no effect on Flink.
     */
    @Deprecated
    public static final ConfigOption<String> WATCH_HEARTBEAT_PAUSE =
            ConfigOptions.key("akka.watch.heartbeat.pause")
                    .defaultValue("60 s")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Acceptable heartbeat pause for Akka’s DeathWatch mechanism. A low value does not allow an"
                                                    + " irregular heartbeat. If TaskManagers are wrongly marked dead because of lost or delayed"
                                                    + " heartbeat messages, then you should increase this value or decrease akka.watch.heartbeat.interval."
                                                    + " Higher value increases the time to detect a dead TaskManager. A thorough description of Akka’s"
                                                    + " DeathWatch can be found %s",
                                            link(
                                                    "http://doc.akka.io/docs/akka/snapshot/scala/remoting.html#failure-detector",
                                                    "here"))
                                    .build());

    /**
     * Detection threshold for the phi accrual watch failure detector.
     *
     * @deprecated Don't use this option anymore. It has no effect on Flink.
     */
    @Deprecated
    public static final ConfigOption<Integer> WATCH_THRESHOLD =
            ConfigOptions.key("akka.watch.threshold")
                    .defaultValue(12)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Threshold for the DeathWatch failure detector. A low value is prone to false positives whereas"
                                                    + " a high value increases the time to detect a dead TaskManager. A thorough description of Akka’s"
                                                    + " DeathWatch can be found %s",
                                            link(
                                                    "http://doc.akka.io/docs/akka/snapshot/scala/remoting.html#failure-detector",
                                                    "here"))
                                    .build());
}
