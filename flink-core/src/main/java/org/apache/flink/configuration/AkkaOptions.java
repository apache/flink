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
import org.apache.flink.util.TimeUtils;

import java.time.Duration;

import static org.apache.flink.configuration.description.LinkElement.link;

/**
 * RPC configuration options.
 *
 * @deprecated Use {@link RpcOptions} instead.
 */
@PublicEvolving
@Deprecated // since 1.19.0
public class AkkaOptions {

    public static boolean isForceRpcInvocationSerializationEnabled(Configuration config) {
        return RpcOptions.isForceRpcInvocationSerializationEnabled(config);
    }

    /** Flag whether to capture call stacks for RPC ask calls. */
    public static final ConfigOption<Boolean> CAPTURE_ASK_CALLSTACK =
            RpcOptions.CAPTURE_ASK_CALLSTACK;

    /** Timeout for Pekko ask calls. */
    public static final ConfigOption<Duration> ASK_TIMEOUT_DURATION =
            RpcOptions.ASK_TIMEOUT_DURATION;

    /** @deprecated Use {@link RpcOptions#ASK_TIMEOUT_DURATION} */
    @Deprecated
    public static final ConfigOption<String> ASK_TIMEOUT =
            ConfigOptions.key(RpcOptions.ASK_TIMEOUT_DURATION.key())
                    .stringType()
                    .defaultValue(
                            TimeUtils.formatWithHighestUnit(
                                    RpcOptions.ASK_TIMEOUT_DURATION.defaultValue()))
                    .withDescription(RpcOptions.ASK_TIMEOUT_DURATION.description());

    /** The Pekko tcp connection timeout. */
    public static final ConfigOption<String> TCP_TIMEOUT = RpcOptions.TCP_TIMEOUT;

    /** Timeout for the startup of the actor system. */
    public static final ConfigOption<String> STARTUP_TIMEOUT = RpcOptions.STARTUP_TIMEOUT;

    /** Override SSL support for the Pekko transport. */
    public static final ConfigOption<Boolean> SSL_ENABLED = RpcOptions.SSL_ENABLED;

    /** Maximum framesize of Pekko messages. */
    public static final ConfigOption<String> FRAMESIZE = RpcOptions.FRAMESIZE;

    /** Maximum number of messages until another actor is executed by the same thread. */
    public static final ConfigOption<Integer> DISPATCHER_THROUGHPUT =
            RpcOptions.DISPATCHER_THROUGHPUT;

    /** Log lifecycle events. */
    public static final ConfigOption<Boolean> LOG_LIFECYCLE_EVENTS =
            RpcOptions.LOG_LIFECYCLE_EVENTS;

    /** Timeout for all blocking calls that look up remote actors. */
    public static final ConfigOption<Duration> LOOKUP_TIMEOUT_DURATION =
            RpcOptions.LOOKUP_TIMEOUT_DURATION;

    /** @deprecated use {@link RpcOptions#LOOKUP_TIMEOUT_DURATION} */
    @Deprecated
    public static final ConfigOption<String> LOOKUP_TIMEOUT =
            ConfigOptions.key(RpcOptions.LOOKUP_TIMEOUT_DURATION.key())
                    .stringType()
                    .defaultValue(
                            TimeUtils.formatWithHighestUnit(
                                    RpcOptions.LOOKUP_TIMEOUT_DURATION.defaultValue()))
                    .withDescription(RpcOptions.LOOKUP_TIMEOUT_DURATION.description());

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

    /** Exit JVM on fatal Pekko errors. */
    public static final ConfigOption<Boolean> JVM_EXIT_ON_FATAL_ERROR =
            RpcOptions.JVM_EXIT_ON_FATAL_ERROR;

    /** Milliseconds a gate should be closed for after a remote connection was disconnected. */
    public static final ConfigOption<Long> RETRY_GATE_CLOSED_FOR = RpcOptions.RETRY_GATE_CLOSED_FOR;

    // ==================================================
    // Configurations for fork-join-executor.
    // ==================================================

    public static final ConfigOption<Double> FORK_JOIN_EXECUTOR_PARALLELISM_FACTOR =
            RpcOptions.FORK_JOIN_EXECUTOR_PARALLELISM_FACTOR;

    public static final ConfigOption<Integer> FORK_JOIN_EXECUTOR_PARALLELISM_MIN =
            RpcOptions.FORK_JOIN_EXECUTOR_PARALLELISM_MIN;

    public static final ConfigOption<Integer> FORK_JOIN_EXECUTOR_PARALLELISM_MAX =
            RpcOptions.FORK_JOIN_EXECUTOR_PARALLELISM_MAX;

    public static final ConfigOption<Double> REMOTE_FORK_JOIN_EXECUTOR_PARALLELISM_FACTOR =
            RpcOptions.REMOTE_FORK_JOIN_EXECUTOR_PARALLELISM_FACTOR;

    public static final ConfigOption<Integer> REMOTE_FORK_JOIN_EXECUTOR_PARALLELISM_MIN =
            RpcOptions.REMOTE_FORK_JOIN_EXECUTOR_PARALLELISM_MIN;

    public static final ConfigOption<Integer> REMOTE_FORK_JOIN_EXECUTOR_PARALLELISM_MAX =
            RpcOptions.REMOTE_FORK_JOIN_EXECUTOR_PARALLELISM_MAX;

    // ==================================================
    // Configurations for client-socket-work-pool.
    // ==================================================

    public static final ConfigOption<Integer> CLIENT_SOCKET_WORKER_POOL_SIZE_MIN =
            RpcOptions.CLIENT_SOCKET_WORKER_POOL_SIZE_MIN;

    public static final ConfigOption<Integer> CLIENT_SOCKET_WORKER_POOL_SIZE_MAX =
            RpcOptions.CLIENT_SOCKET_WORKER_POOL_SIZE_MAX;

    public static final ConfigOption<Double> CLIENT_SOCKET_WORKER_POOL_SIZE_FACTOR =
            RpcOptions.CLIENT_SOCKET_WORKER_POOL_SIZE_FACTOR;

    // ==================================================
    // Configurations for server-socket-work-pool.
    // ==================================================

    public static final ConfigOption<Integer> SERVER_SOCKET_WORKER_POOL_SIZE_MIN =
            RpcOptions.SERVER_SOCKET_WORKER_POOL_SIZE_MIN;

    public static final ConfigOption<Integer> SERVER_SOCKET_WORKER_POOL_SIZE_MAX =
            RpcOptions.SERVER_SOCKET_WORKER_POOL_SIZE_MAX;

    public static final ConfigOption<Double> SERVER_SOCKET_WORKER_POOL_SIZE_FACTOR =
            RpcOptions.SERVER_SOCKET_WORKER_POOL_SIZE_FACTOR;

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
                    .stringType()
                    .defaultValue(ASK_TIMEOUT.defaultValue())
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Heartbeat interval for Akka’s DeathWatch mechanism to detect dead TaskManagers. If"
                                                    + " TaskManagers are wrongly marked dead because of lost or delayed heartbeat messages, then you"
                                                    + " should decrease this value or increase akka.watch.heartbeat.pause. A thorough description of"
                                                    + " Akka’s DeathWatch can be found %s",
                                            link(
                                                    "https://pekko.apache.org/docs/pekko/current/remoting-artery.html#failure-detector",
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
                    .stringType()
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
                                                    "https://pekko.apache.org/docs/pekko/current/remoting-artery.html#failure-detector",
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
                    .intType()
                    .defaultValue(12)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Threshold for the DeathWatch failure detector. A low value is prone to false positives whereas"
                                                    + " a high value increases the time to detect a dead TaskManager. A thorough description of Akka’s"
                                                    + " DeathWatch can be found %s",
                                            link(
                                                    "https://pekko.apache.org/docs/pekko/current/remoting-artery.html#failure-detector",
                                                    "here"))
                                    .build());
}
