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
import org.apache.flink.configuration.description.Description;

import java.time.Duration;

import static org.apache.flink.configuration.description.TextElement.code;

/** RPC configuration options. */
@PublicEvolving
public class RpcOptions {

    @Internal
    @Documentation.ExcludeFromDocumentation("Internal use only")
    public static final ConfigOption<Boolean> FORCE_RPC_INVOCATION_SERIALIZATION =
            ConfigOptions.key("pekko.rpc.force-invocation-serialization")
                    .booleanType()
                    .defaultValue(false)
                    .withDeprecatedKeys("akka.rpc.force-invocation-serialization")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Forces the serialization of all RPC invocations (that are not explicitly annotated with %s)."
                                                    + "This option can be used to find serialization issues in the argument/response types without relying requiring HA setups."
                                                    + "This option should not be enabled in production.",
                                            code("org.apache.flink.runtime.rpc.Local"))
                                    .build());

    public static boolean isForceRpcInvocationSerializationEnabled(Configuration config) {
        return config.getOptional(FORCE_RPC_INVOCATION_SERIALIZATION)
                .orElse(
                        FORCE_RPC_INVOCATION_SERIALIZATION.defaultValue()
                                || System.getProperties()
                                        .containsKey(FORCE_RPC_INVOCATION_SERIALIZATION.key()));
    }

    /** Flag whether to capture call stacks for RPC ask calls. */
    public static final ConfigOption<Boolean> CAPTURE_ASK_CALLSTACK =
            ConfigOptions.key("pekko.ask.callstack")
                    .booleanType()
                    .defaultValue(true)
                    .withDeprecatedKeys("akka.ask.callstack")
                    .withDescription(
                            "If true, call stack for asynchronous asks are captured. That way, when an ask fails "
                                    + "(for example times out), you get a proper exception, describing to the original method call and "
                                    + "call site. Note that in case of having millions of concurrent RPC calls, this may add to the "
                                    + "memory footprint.");

    /** Timeout for Pekko ask calls. */
    public static final ConfigOption<Duration> ASK_TIMEOUT_DURATION =
            ConfigOptions.key("pekko.ask.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDeprecatedKeys("akka.ask.timeout")
                    .withDescription(
                            "Timeout used for all futures and blocking Pekko calls. If Flink fails due to timeouts then you"
                                    + " should try to increase this value. Timeouts can be caused by slow machines or a congested network. The"
                                    + " timeout value requires a time-unit specifier (ms/s/min/h/d).");

    /** The Pekko tcp connection timeout. */
    public static final ConfigOption<String> TCP_TIMEOUT =
            ConfigOptions.key("pekko.tcp.timeout")
                    .stringType()
                    .defaultValue("20 s")
                    .withDeprecatedKeys("akka.tcp.timeout")
                    .withDescription(
                            "Timeout for all outbound connections. If you should experience problems with connecting to a"
                                    + " TaskManager due to a slow network, you should increase this value.");

    /** Timeout for the startup of the actor system. */
    public static final ConfigOption<String> STARTUP_TIMEOUT =
            ConfigOptions.key("pekko.startup-timeout")
                    .stringType()
                    .noDefaultValue()
                    .withDeprecatedKeys("akka.startup-timeout")
                    .withDescription(
                            "Timeout after which the startup of a remote component is considered being failed.");

    /** Override SSL support for the Pekko transport. */
    public static final ConfigOption<Boolean> SSL_ENABLED =
            ConfigOptions.key("pekko.ssl.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDeprecatedKeys("akka.ssl.enabled")
                    .withDescription(
                            "Turns on SSL for Pekko’s remote communication. This is applicable only when the global ssl flag"
                                    + " security.ssl.enabled is set to true.");

    /** Maximum framesize of Pekko messages. */
    public static final ConfigOption<String> FRAMESIZE =
            ConfigOptions.key("pekko.framesize")
                    .stringType()
                    .defaultValue("10485760b")
                    .withDeprecatedKeys("akka.framesize")
                    .withDescription(
                            "Maximum size of messages which are sent between the JobManager and the TaskManagers. If Flink"
                                    + " fails because messages exceed this limit, then you should increase it. The message size requires a"
                                    + " size-unit specifier.");

    /** Maximum number of messages until another actor is executed by the same thread. */
    public static final ConfigOption<Integer> DISPATCHER_THROUGHPUT =
            ConfigOptions.key("pekko.throughput")
                    .intType()
                    .defaultValue(15)
                    .withDeprecatedKeys("akka.throughput")
                    .withDescription(
                            "Number of messages that are processed in a batch before returning the thread to the pool. Low"
                                    + " values denote a fair scheduling whereas high values can increase the performance at the cost of unfairness.");

    /** Log lifecycle events. */
    public static final ConfigOption<Boolean> LOG_LIFECYCLE_EVENTS =
            ConfigOptions.key("pekko.log.lifecycle.events")
                    .booleanType()
                    .defaultValue(false)
                    .withDeprecatedKeys("akka.log.lifecycle.events")
                    .withDescription(
                            "Turns on the Pekko’s remote logging of events. Set this value to 'true' in case of debugging.");

    /** Timeout for all blocking calls that look up remote actors. */
    public static final ConfigOption<Duration> LOOKUP_TIMEOUT_DURATION =
            ConfigOptions.key("pekko.lookup.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDeprecatedKeys("akka.lookup.timeout")
                    .withDescription(
                            "Timeout used for the lookup of the JobManager. The timeout value has to contain a time-unit"
                                    + " specifier (ms/s/min/h/d).");

    /** Exit JVM on fatal Pekko errors. */
    public static final ConfigOption<Boolean> JVM_EXIT_ON_FATAL_ERROR =
            ConfigOptions.key("pekko.jvm-exit-on-fatal-error")
                    .booleanType()
                    .defaultValue(true)
                    .withDeprecatedKeys("akka.jvm-exit-on-fatal-error")
                    .withDescription("Exit JVM on fatal Pekko errors.");

    /** Milliseconds a gate should be closed for after a remote connection was disconnected. */
    public static final ConfigOption<Long> RETRY_GATE_CLOSED_FOR =
            ConfigOptions.key("pekko.retry-gate-closed-for")
                    .longType()
                    .defaultValue(50L)
                    .withDeprecatedKeys("akka.retry-gate-closed-for")
                    .withDescription(
                            "Milliseconds a gate should be closed for after a remote connection was disconnected.");

    // ==================================================
    // Configurations for fork-join-executor.
    // ==================================================

    public static final ConfigOption<Double> FORK_JOIN_EXECUTOR_PARALLELISM_FACTOR =
            ConfigOptions.key("pekko.fork-join-executor.parallelism-factor")
                    .doubleType()
                    .defaultValue(2.0)
                    .withDeprecatedKeys("akka.fork-join-executor.parallelism-factor")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The parallelism factor is used to determine thread pool size using the"
                                                    + " following formula: ceil(available processors * factor). Resulting size"
                                                    + " is then bounded by the parallelism-min and parallelism-max values.")
                                    .build());

    public static final ConfigOption<Integer> FORK_JOIN_EXECUTOR_PARALLELISM_MIN =
            ConfigOptions.key("pekko.fork-join-executor.parallelism-min")
                    .intType()
                    .defaultValue(8)
                    .withDeprecatedKeys("akka.fork-join-executor.parallelism-min")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Min number of threads to cap factor-based parallelism number to.")
                                    .build());

    public static final ConfigOption<Integer> FORK_JOIN_EXECUTOR_PARALLELISM_MAX =
            ConfigOptions.key("pekko.fork-join-executor.parallelism-max")
                    .intType()
                    .defaultValue(64)
                    .withDeprecatedKeys("akka.fork-join-executor.parallelism-max")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Max number of threads to cap factor-based parallelism number to.")
                                    .build());

    public static final ConfigOption<Double> REMOTE_FORK_JOIN_EXECUTOR_PARALLELISM_FACTOR =
            ConfigOptions.key("pekko.remote-fork-join-executor.parallelism-factor")
                    .doubleType()
                    .defaultValue(2.0)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The parallelism factor is used to determine thread pool size using the"
                                                    + " following formula: ceil(available processors * factor). Resulting size"
                                                    + " is then bounded by the parallelism-min and parallelism-max values.")
                                    .build());

    public static final ConfigOption<Integer> REMOTE_FORK_JOIN_EXECUTOR_PARALLELISM_MIN =
            ConfigOptions.key("pekko.remote-fork-join-executor.parallelism-min")
                    .intType()
                    .defaultValue(8)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Min number of threads to cap factor-based parallelism number to.")
                                    .build());

    public static final ConfigOption<Integer> REMOTE_FORK_JOIN_EXECUTOR_PARALLELISM_MAX =
            ConfigOptions.key("pekko.remote-fork-join-executor.parallelism-max")
                    .intType()
                    .defaultValue(16)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Max number of threads to cap factor-based parallelism number to.")
                                    .build());

    // ==================================================
    // Configurations for client-socket-work-pool.
    // ==================================================

    public static final ConfigOption<Integer> CLIENT_SOCKET_WORKER_POOL_SIZE_MIN =
            ConfigOptions.key("pekko.client-socket-worker-pool.pool-size-min")
                    .intType()
                    .defaultValue(1)
                    .withDeprecatedKeys("akka.client-socket-worker-pool.pool-size-min")
                    .withDescription(
                            Description.builder()
                                    .text("Min number of threads to cap factor-based number to.")
                                    .build());

    public static final ConfigOption<Integer> CLIENT_SOCKET_WORKER_POOL_SIZE_MAX =
            ConfigOptions.key("pekko.client-socket-worker-pool.pool-size-max")
                    .intType()
                    .defaultValue(2)
                    .withDeprecatedKeys("akka.client-socket-worker-pool.pool-size-max")
                    .withDescription(
                            Description.builder()
                                    .text("Max number of threads to cap factor-based number to.")
                                    .build());

    public static final ConfigOption<Double> CLIENT_SOCKET_WORKER_POOL_SIZE_FACTOR =
            ConfigOptions.key("pekko.client-socket-worker-pool.pool-size-factor")
                    .doubleType()
                    .defaultValue(1.0)
                    .withDeprecatedKeys("akka.client-socket-worker-pool.pool-size-factor")
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
            ConfigOptions.key("pekko.server-socket-worker-pool.pool-size-min")
                    .intType()
                    .defaultValue(1)
                    .withDeprecatedKeys("akka.server-socket-worker-pool.pool-size-min")
                    .withDescription(
                            Description.builder()
                                    .text("Min number of threads to cap factor-based number to.")
                                    .build());

    public static final ConfigOption<Integer> SERVER_SOCKET_WORKER_POOL_SIZE_MAX =
            ConfigOptions.key("pekko.server-socket-worker-pool.pool-size-max")
                    .intType()
                    .defaultValue(2)
                    .withDeprecatedKeys("akka.server-socket-worker-pool.pool-size-max")
                    .withDescription(
                            Description.builder()
                                    .text("Max number of threads to cap factor-based number to.")
                                    .build());

    public static final ConfigOption<Double> SERVER_SOCKET_WORKER_POOL_SIZE_FACTOR =
            ConfigOptions.key("pekko.server-socket-worker-pool.pool-size-factor")
                    .doubleType()
                    .defaultValue(1.0)
                    .withDeprecatedKeys("akka.server-socket-worker-pool.pool-size-factor")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The pool size factor is used to determine thread pool size"
                                                    + " using the following formula: ceil(available processors * factor)."
                                                    + " Resulting size is then bounded by the pool-size-min and"
                                                    + " pool-size-max values.")
                                    .build());
}
