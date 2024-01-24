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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;

import java.time.Duration;

/**
 * RPC configuration options.
 *
 * @deprecated Use {@link RpcOptions} instead.
 */
@PublicEvolving
@Deprecated // since 1.19.0
public class AkkaOptions {

    @Internal
    @Documentation.ExcludeFromDocumentation("Internal use only")
    public static final ConfigOption<Boolean> FORCE_RPC_INVOCATION_SERIALIZATION =
            RpcOptions.FORCE_RPC_INVOCATION_SERIALIZATION;

    public static boolean isForceRpcInvocationSerializationEnabled(Configuration config) {
        return RpcOptions.isForceRpcInvocationSerializationEnabled(config);
    }

    /** Flag whether to capture call stacks for RPC ask calls. */
    public static final ConfigOption<Boolean> CAPTURE_ASK_CALLSTACK =
            RpcOptions.CAPTURE_ASK_CALLSTACK;

    /** Timeout for Pekko ask calls. */
    public static final ConfigOption<Duration> ASK_TIMEOUT_DURATION =
            RpcOptions.ASK_TIMEOUT_DURATION;

    /** @deprecated Use {@link #ASK_TIMEOUT_DURATION} */
    @Deprecated public static final ConfigOption<String> ASK_TIMEOUT = RpcOptions.ASK_TIMEOUT;

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

    /** @deprecated use {@link #LOOKUP_TIMEOUT_DURATION} */
    @Deprecated public static final ConfigOption<String> LOOKUP_TIMEOUT = RpcOptions.LOOKUP_TIMEOUT;

    /**
     * Timeout for all blocking calls on the client side.
     *
     * @deprecated Use the {@code ClientOptions.CLIENT_TIMEOUT} instead.
     */
    @Deprecated public static final ConfigOption<String> CLIENT_TIMEOUT = RpcOptions.CLIENT_TIMEOUT;

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
            RpcOptions.WATCH_HEARTBEAT_INTERVAL;

    /**
     * The maximum acceptable Akka death watch heartbeat pause.
     *
     * @deprecated Don't use this option anymore. It has no effect on Flink.
     */
    @Deprecated
    public static final ConfigOption<String> WATCH_HEARTBEAT_PAUSE =
            RpcOptions.WATCH_HEARTBEAT_PAUSE;

    /**
     * Detection threshold for the phi accrual watch failure detector.
     *
     * @deprecated Don't use this option anymore. It has no effect on Flink.
     */
    @Deprecated
    public static final ConfigOption<Integer> WATCH_THRESHOLD = RpcOptions.WATCH_THRESHOLD;
}
